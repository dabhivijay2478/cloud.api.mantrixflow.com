/**
 * PostgreSQL Pipeline Service
 * Orchestrates end-to-end data pipeline execution
 * 
 * ============================================================================
 * ARCHITECTURE: Job-Based Pipeline Execution with Authoritative State
 * ============================================================================
 * 
 * This service implements a COMPLETE ARCHITECTURAL REFACTOR that fixes
 * systemic design problems in the pipeline execution system.
 * 
 * CRITICAL ARCHITECTURAL PRINCIPLES:
 * 
 * 1. SEPARATION OF SETUP AND EXECUTION
 *    - Setup Phase: Resolves destination table ONCE and LOCKS it in run record
 *    - Execution Phase: Reads from run record (AUTHORITATIVE) and ONLY writes data
 *    - Table creation NEVER happens during execution
 * 
 * 2. RUN RECORD AS AUTHORITATIVE SOURCE OF TRUTH
 *    - postgres_pipeline_runs stores:
 *      * resolved_destination_schema/table (locked during setup)
 *      * resolved_column_mappings (SINGLE source of truth)
 *      * job_state (drives migration behavior)
 *      * last_sync_cursor (authoritative cursor for incremental sync)
 *    - Migration execution MUST read from run record
 *    - Cannot override or recreate tables
 * 
 * 3. FIELD MAPPINGS AS SINGLE SOURCE OF TRUTH
 *    - Field mappings determine:
 *      * Which columns are migrated
 *      * Schema creation (if applicable)
 *      * Insert/update payload structure
 *    - Only mapped fields are included in migration
 * 
 * 4. PRIMARY KEY & UPSERT RULES
 *    - Only UUID is allowed as primary key
 *    - Same UUID → UPDATE existing row
 *    - New UUID → INSERT new row
 *    - Works identically for existing and newly created tables
 * 
 * EXECUTION FLOW:
 * 
 * SETUP PHASE (ONE TIME - before migration):
 *   1. Create run record
 *   2. Extract field mappings from transformers
 *   3. resolveAndPrepareDestinationTable():
 *      * Resolves whether table is EXISTING or NEW (DETERMINISTIC)
 *      * Creates table if needed (ONLY in setup phase)
 *      * Returns resolved table information
 *   4. LOCK resolved table in run record:
 *      * resolved_destination_schema
 *      * resolved_destination_table
 *      * resolved_column_mappings
 *      * destination_table_was_created
 *      * job_state = 'running'
 * 
 * MIGRATION PHASE (data movement):
 *   1. Read resolved table from run record (AUTHORITATIVE)
 *   2. Read from source (using cursor from run record if incremental)
 *   3. Transform data (apply field mappings)
 *   4. Write to destination (ONLY writes data - no table creation):
 *      * Uses resolved table from run record
 *      * Filters to only mapped columns
 *      * Uses upsert for UUID primary keys
 *   5. Update run record:
 *      * job_state = 'completed'
 *      * last_sync_cursor (for incremental sync)
 *      * execution statistics
 * 
 * RULES (ENFORCED):
 * - If destinationTableExists = true → MUST use existing table, NEVER create
 * - If destinationTableExists = false → Create new table ONLY if auto-generated name
 * - Migration execution CANNOT create tables (reads from run record)
 * - Field mappings determine which columns are migrated
 * - Only mapped fields are included in migration payload
 * 
 * ============================================================================
 */

import {
    Injectable,
    Logger,
    NotFoundException,
    BadRequestException,
} from '@nestjs/common';
import { randomUUID } from 'crypto';
import { PostgresConnectionPoolService } from '../data-sources/postgres/services/postgres-connection-pool.service';
import { PostgresQueryExecutorService } from '../data-sources/postgres/services/postgres-query-executor.service';
import { PostgresDestinationService } from './emitters/postgres-destination.service';
import { PostgresSchemaMapperService } from './transformers/postgres-schema-mapper.service';
import { PostgresPipelineRepository, PipelineWithSchemas } from './repositories/postgres-pipeline.repository';
import { PipelineSourceSchemaRepository } from './repositories/pipeline-source-schema.repository';
import { PipelineDestinationSchemaRepository } from './repositories/pipeline-destination-schema.repository';
import { PostgresConnectionRepository } from '../data-sources/postgres/repositories/postgres-connection.repository';
import {
    PipelineRunResult,
    PipelineError,
    ValidationResult,
    DryRunResult,
    ColumnMapping,
    Transformation,
} from '../data-sources/postgres/postgres.types';
import type { PostgresPipeline, PipelineSourceSchema, PipelineDestinationSchema } from '@db/schema';

@Injectable()
export class PostgresPipelineService {
    private readonly logger = new Logger(PostgresPipelineService.name);

    constructor(
        private readonly pipelineRepository: PostgresPipelineRepository,
        private readonly sourceSchemaRepository: PipelineSourceSchemaRepository,
        private readonly destinationSchemaRepository: PipelineDestinationSchemaRepository,
        private readonly connectionRepository: PostgresConnectionRepository,
        private readonly connectionPool: PostgresConnectionPoolService,
        private readonly queryExecutor: PostgresQueryExecutorService,
        private readonly destinationService: PostgresDestinationService,
        private readonly schemaMapper: PostgresSchemaMapperService,
    ) { }

    /**
     * Create pipeline with source and destination schemas
     */
    async createPipeline(data: {
        orgId: string;
        userId: string;
        name: string;
        description?: string;
        sourceType: string;
        sourceConnectionId?: string;
        sourceConfig?: any;
        sourceSchema?: string;
        sourceTable?: string;
        sourceQuery?: string;
        destinationConnectionId: string;
        destinationSchema?: string;
        destinationTable: string;
        columnMappings?: any[];
        transformations?: any[];
        writeMode?: string;
        upsertKey?: string[];
        syncMode?: string;
        incrementalColumn?: string;
        syncFrequency?: string;
        collectors?: Array<{
            id: string;
            sourceId: string;
            selectedTables: string[];
            transformers?: Array<{
                id: string;
                name: string;
                collectorId?: string;
                emitterId?: string;
                fieldMappings?: Array<{ source: string; destination: string }>; // JSON array format
            }>;
        }>;
        emitters?: Array<{
            id: string;
            transformId: string;
            destinationId: string; // References existing connection (like collectors use sourceId)
            destinationName: string;
            destinationType: string;
            connectionConfig?: Record<string, string>; // Optional, ignored - connection is referenced by destinationId
        }>;
    }): Promise<PostgresPipeline> {
        // Validate that destination connection exists and is accessible
        try {
            const destinationConnection = await this.connectionRepository.findById(
                data.destinationConnectionId,
                data.orgId,
            );
            if (!destinationConnection) {
                throw new BadRequestException(
                    `Destination connection ${data.destinationConnectionId} not found or not accessible. Please ensure the connection exists and you have access to it.`,
                );
            }
            
            // Check connection status
            if (destinationConnection.status === 'error') {
                this.logger.warn(
                    `Destination connection ${destinationConnection.name} has error status. Last error: ${destinationConnection.lastError}`,
                );
                throw new BadRequestException(
                    `Destination connection "${destinationConnection.name}" has an error status. Please test and fix the connection in the Data Sources page before creating a pipeline. Last error: ${destinationConnection.lastError || 'Unknown error'}`,
                );
            }
            
            this.logger.log(
                `Validated destination connection: ${destinationConnection.name} (${data.destinationConnectionId})`,
            );
        } catch (error) {
            if (error instanceof BadRequestException) {
                throw error;
            }
            this.logger.error(
                `Failed to validate destination connection: ${error instanceof Error ? error.message : String(error)}`,
            );
            throw new BadRequestException(
                `Failed to validate destination connection: ${error instanceof Error ? error.message : 'Unknown error'}. Please ensure the connection exists and is accessible.`,
            );
        }

        // Validate source connection if provided
        if (data.sourceConnectionId) {
            try {
                const sourceConnection = await this.connectionRepository.findById(
                    data.sourceConnectionId,
                    data.orgId,
                );
                if (!sourceConnection) {
                    throw new BadRequestException(
                        `Source connection ${data.sourceConnectionId} not found or not accessible`,
                    );
                }
                this.logger.log(
                    `Validated source connection: ${sourceConnection.name} (${data.sourceConnectionId})`,
                );
            } catch (error) {
                if (error instanceof BadRequestException) {
                    throw error;
                }
                this.logger.error(
                    `Failed to validate source connection: ${error instanceof Error ? error.message : String(error)}`,
                );
                throw new BadRequestException(
                    `Failed to validate source connection: ${error instanceof Error ? error.message : 'Unknown error'}`,
                );
            }
        }

        // Extract source schema and table from collectors if not provided
        let sourceSchemaName = data.sourceSchema;
        let sourceTableName = data.sourceTable;
        
        // If using collectors and source info is not provided, extract from first collector
        if (data.collectors && data.collectors.length > 0 && !sourceTableName) {
            const firstCollector = data.collectors[0];
            if (firstCollector.selectedTables && firstCollector.selectedTables.length > 0) {
                // Parse schema-qualified table name (e.g., "company.companies" -> schema: "company", table: "companies")
                const firstTable = firstCollector.selectedTables[0];
                if (firstTable.includes('.')) {
                    const [schema, table] = firstTable.split('.');
                    sourceSchemaName = sourceSchemaName || schema;
                    sourceTableName = table;
                } else {
                    // If no schema prefix, use default schema or provided schema
                    sourceTableName = firstTable;
                }
            }
        }

        // 1. Create source schema
        let sourceSchema: PipelineSourceSchema;
        try {
            sourceSchema = await this.sourceSchemaRepository.create({
            orgId: data.orgId,
            userId: data.userId,
            sourceType: data.sourceType,
            sourceConnectionId: data.sourceConnectionId,
            sourceConfig: data.sourceConfig,
                sourceSchema: sourceSchemaName,
                sourceTable: sourceTableName,
            sourceQuery: data.sourceQuery,
            name: `Source for ${data.name}`,
        });
            if (!sourceSchema?.id) {
                throw new BadRequestException('Failed to create source schema - no ID returned');
            }
            this.logger.log(`Created source schema: ${sourceSchema.id}`);
        } catch (error) {
            this.logger.error(
                `Failed to create source schema: ${error instanceof Error ? error.message : String(error)}`,
            );
            throw new BadRequestException(
                `Failed to create source schema: ${error instanceof Error ? error.message : 'Unknown error'}`,
            );
        }

        // 2. Create destination schema
        // Check if transformers have destinationTable specified (existing table)
        // If destinationTable is in format "schema.table", extract it
        let destinationTableName = data.destinationTable;
        let destinationSchemaName = data.destinationSchema || 'public';
        
        // IMPORTANT: Determine if this is an existing table or a new table
        // Priority: Check transformers first, then check actual table existence in database
        let destinationTableExists = false; // Default to false (will create new table)
        
        // First, check if transformers have destinationTable specified (existing table)
        if (data.collectors) {
            const transformerWithTable = data.collectors
                .flatMap((c: any) => c.transformers || [])
                .find((t: any) => t.destinationTable);
            
            if (transformerWithTable?.destinationTable) {
                const tableParts = transformerWithTable.destinationTable.includes('.')
                    ? transformerWithTable.destinationTable.split('.')
                    : ['public', transformerWithTable.destinationTable];
                destinationSchemaName = tableParts[0] || 'public';
                destinationTableName = tableParts[1] || tableParts[0];
                // User explicitly selected a table from transformer - treat as existing
                destinationTableExists = true;
                this.logger.log(
                    `Found destination table in transformer: ${destinationSchemaName}.${destinationTableName} (treating as existing table)`,
                );
            }
        }
        
        // If no transformer table found, check if the provided destinationTable is an existing table
        // Auto-generated tables start with "pipeline_", so if it doesn't start with that, check if it exists
        if (!destinationTableExists && destinationTableName && !destinationTableName.startsWith('pipeline_')) {
            // Table name doesn't match auto-generated pattern - check if it actually exists in database
            try {
                // Get connection pool to check table existence
                let pool = this.connectionPool.getPool(data.destinationConnectionId);
                if (!pool) {
                    const connection = await this.connectionRepository.findById(
                        data.destinationConnectionId,
                        data.orgId,
                    );
                    if (connection && connection.status === 'active') {
                        const credentials = this.connectionRepository.decryptCredentials(connection);
                        pool = await this.connectionPool.createPool(
                            data.destinationConnectionId,
                            credentials,
                        );
                    }
                }
                
                if (pool) {
                    const client = await pool.connect();
                    try {
                        const tableExists = await this.destinationService.tableExists(
                            client,
                            destinationSchemaName,
                            destinationTableName,
                        );
                        destinationTableExists = tableExists;
                        this.logger.log(
                            `Checked destination table '${destinationSchemaName}.${destinationTableName}': ${tableExists ? 'EXISTS (will use existing)' : 'DOES NOT EXIST (will create new)'}`,
                        );
                    } finally {
                        client.release();
                    }
                } else {
                    // Can't check - assume it's existing if name doesn't match auto-generated pattern
                    destinationTableExists = true;
                    this.logger.warn(
                        `Cannot verify table existence. Treating '${destinationTableName}' as existing table (does not match auto-generated pattern).`,
                    );
                }
            } catch (error) {
                // Error checking table - assume it's existing if name doesn't match auto-generated pattern
                destinationTableExists = true;
                this.logger.warn(
                    `Error checking table existence: ${error instanceof Error ? error.message : String(error)}. Treating '${destinationTableName}' as existing table.`,
                );
            }
        } else if (!destinationTableExists && destinationTableName && destinationTableName.startsWith('pipeline_')) {
            // Auto-generated table name - will create new table
            destinationTableExists = false;
            this.logger.log(
                `Destination table '${destinationTableName}' matches auto-generated pattern. Will create new table.`,
            );
        }
        
        let destinationSchema: PipelineDestinationSchema;
        try {
            destinationSchema = await this.destinationSchemaRepository.create({
            orgId: data.orgId,
            userId: data.userId,
            destinationConnectionId: data.destinationConnectionId,
                destinationSchema: destinationSchemaName,
                destinationTable: destinationTableName,
                destinationTableExists: destinationTableExists, // Set based on whether user selected existing table
            columnMappings: data.columnMappings,
            writeMode: (data.writeMode as 'append' | 'upsert' | 'replace') || 'append',
            upsertKey: data.upsertKey,
            name: `Destination for ${data.name}`,
        });
            if (!destinationSchema?.id) {
                throw new BadRequestException('Failed to create destination schema - no ID returned');
            }
            this.logger.log(`Created destination schema: ${destinationSchema.id}`);
        } catch (error) {
            this.logger.error(
                `Failed to create destination schema: ${error instanceof Error ? error.message : String(error)}`,
            );
            throw new BadRequestException(
                `Failed to create destination schema: ${error instanceof Error ? error.message : 'Unknown error'}`,
            );
        }

        // 3. Create pipeline with schema references
        // Store collectors, transformers, and emitters in transformations JSONB field
        // Format: { transformations: [...], collectors: [...], emitters: [...] }
        
        // Extract all transformers from collectors and convert to legacy transformations format
        const legacyTransformations: Transformation[] = [];
        if (data.collectors) {
            this.logger.log(
                `Extracting transformers from ${data.collectors.length} collectors`,
            );
            data.collectors.forEach((collector: any) => {
                if (collector.transformers && Array.isArray(collector.transformers)) {
                    this.logger.log(
                        `Collector ${collector.id} has ${collector.transformers.length} transformers`,
                    );
                    collector.transformers.forEach((transformer: any) => {
                        // Convert transformer fieldMappings to legacy Transformation format
                        if (transformer.fieldMappings && Array.isArray(transformer.fieldMappings)) {
                            this.logger.log(
                                `Transformer ${transformer.id} has ${transformer.fieldMappings.length} field mappings`,
                            );
                            transformer.fieldMappings.forEach((mapping: { source: string; destination: string }) => {
                                legacyTransformations.push({
                                    sourceColumn: mapping.source.includes('.') 
                                        ? mapping.source.split('.').pop() || mapping.source
                                        : mapping.source,
                                    destinationColumn: mapping.destination,
                                    transformType: 'rename',
                                    transformConfig: {},
                                });
                            });
                        } else {
                            this.logger.warn(
                                `Transformer ${transformer.id} has no fieldMappings or fieldMappings is not an array`,
                            );
                        }
                    });
                } else {
                    this.logger.warn(
                        `Collector ${collector.id} has no transformers or transformers is not an array`,
                    );
                }
            });
        }
        
        // Merge with any existing transformations
        const allTransformations = [
            ...(data.transformations || []),
            ...legacyTransformations,
        ];
        
        this.logger.log(
            `Storing ${allTransformations.length} transformations in pipeline config (${legacyTransformations.length} from collectors, ${(data.transformations || []).length} from data)`,
        );
        
        const pipelineConfig: any = {};
        
        // Only include transformations if there are any
        if (allTransformations.length > 0) {
            pipelineConfig.transformations = allTransformations;
        }
        
        if (data.collectors) {
            pipelineConfig.collectors = data.collectors;
        }
        
        if (data.emitters) {
            pipelineConfig.emitters = data.emitters;
        }
        
        // Validate destinationConnectionId is provided (required for legacy column)
        if (!data.destinationConnectionId) {
            throw new BadRequestException(
                'destinationConnectionId is required. Please ensure emitters are configured with valid destination connections.',
            );
        }
        
        try {
            // Verify schemas exist before creating pipeline
            const sourceSchemaExists = await this.sourceSchemaRepository.findById(sourceSchema.id);
            if (!sourceSchemaExists) {
                throw new BadRequestException(
                    `Source schema ${sourceSchema.id} was not created successfully`,
                );
            }

            const destinationSchemaExists = await this.destinationSchemaRepository.findById(destinationSchema.id);
            if (!destinationSchemaExists) {
                throw new BadRequestException(
                    `Destination schema ${destinationSchema.id} was not created successfully`,
                );
            }

            this.logger.log(
                `Creating pipeline with source schema: ${sourceSchema.id}, destination schema: ${destinationSchema.id}, destination connection: ${data.destinationConnectionId}`,
            );
        
        // IMPORTANT: Check for duplicate pipeline before creating
        // Prevent duplicate creation by checking if a pipeline with the same configuration already exists
        // Check by name + orgId + destination schema/table combination to ensure true uniqueness
        const existingPipeline = await this.pipelineRepository.findByNameAndOrgId(
            data.name,
            data.orgId,
        );
        
        if (existingPipeline && !existingPipeline.deletedAt) {
            // Check if it's truly a duplicate by comparing key configuration
            const existingWithSchemas = await this.pipelineRepository.findByIdWithSchemas(existingPipeline.id);
            if (existingWithSchemas) {
                const { destinationSchema: existingDestSchema } = existingWithSchemas;
                const isSameDestination = existingDestSchema.destinationConnectionId === data.destinationConnectionId &&
                                        existingDestSchema.destinationSchema === (data.destinationSchema || 'public') &&
                                        existingDestSchema.destinationTable === destinationTableName;
                
                if (isSameDestination) {
                    this.logger.warn(
                        `Pipeline with name '${data.name}' and same destination already exists in org ${data.orgId}. Returning existing pipeline: ${existingPipeline.id}`,
                    );
                    // Return existing pipeline instead of creating a duplicate
                    return existingPipeline;
                }
            } else {
                // Can't verify - but pipeline with same name exists, so return it to prevent duplicate
                this.logger.warn(
                    `Pipeline with name '${data.name}' already exists in org ${data.orgId}. Returning existing pipeline: ${existingPipeline.id}`,
                );
                return existingPipeline;
            }
        }
        
        const pipeline = await this.pipelineRepository.create({
            orgId: data.orgId,
            userId: data.userId,
            name: data.name,
            description: data.description,
                sourceType: data.sourceType, // Required by database table (legacy column)
            sourceSchemaId: sourceSchema.id,
            destinationSchemaId: destinationSchema.id,
                destinationConnectionId: data.destinationConnectionId, // Legacy column - required during migration
                destinationTable: data.destinationTable, // Legacy column - required during migration
            transformations: pipelineConfig, // Store full config including collectors/emitters
            syncMode: data.syncMode || 'full',
            incrementalColumn: data.incrementalColumn,
            syncFrequency: data.syncFrequency || 'manual',
        });

            this.logger.log(`Successfully created pipeline: ${pipeline.id}`);
        return pipeline;
        } catch (error) {
            // Enhanced error logging - extract actual PostgreSQL error from Drizzle
            const errorMessage = error instanceof Error ? error.message : String(error);
            
            // Drizzle errors wrap PostgreSQL errors - try to extract the actual error
            let pgErrorCode: string | undefined;
            let pgErrorDetail: string | undefined;
            let pgErrorConstraint: string | undefined;
            let pgErrorMessage: string = errorMessage;

            // Check if it's a Drizzle error with nested cause
            const drizzleError = error as any;
            if (drizzleError?.cause) {
                const cause = drizzleError.cause;
                pgErrorCode = cause?.code;
                pgErrorDetail = cause?.detail;
                pgErrorConstraint = cause?.constraint;
                if (cause?.message) {
                    pgErrorMessage = cause.message;
                }
            } else {
                // Try direct properties
                pgErrorCode = drizzleError?.code;
                pgErrorDetail = drizzleError?.detail;
                pgErrorConstraint = drizzleError?.constraint;
            }

            this.logger.error(
                `Failed to create pipeline: ${errorMessage}`,
                error instanceof Error ? error.stack : undefined,
            );
            this.logger.error(
                `Source schema ID: ${sourceSchema.id}, Destination schema ID: ${destinationSchema.id}`,
            );
            // Log the full error structure for debugging
            this.logger.error(
                `PostgreSQL error code: ${pgErrorCode}, Detail: ${pgErrorDetail}, Constraint: ${pgErrorConstraint}`,
            );
            this.logger.error(
                `Full error object: ${JSON.stringify({
                    message: errorMessage,
                    code: pgErrorCode,
                    detail: pgErrorDetail,
                    constraint: pgErrorConstraint,
                    errorType: error?.constructor?.name,
                    errorKeys: Object.keys(drizzleError || {}),
                    causeKeys: drizzleError?.cause ? Object.keys(drizzleError.cause) : undefined,
                }, null, 2)}`,
            );

            // Check for foreign key constraint violations (23503)
            if (pgErrorCode === '23503' || pgErrorMessage.includes('foreign key') || pgErrorConstraint?.includes('_fk')) {
                throw new BadRequestException(
                    `Database constraint violation: The source or destination schema may not exist in the database. ` +
                    `Source schema ID: ${sourceSchema.id}, Destination schema ID: ${destinationSchema.id}. ` +
                    `Constraint: ${pgErrorConstraint || 'unknown'}. ` +
                    `Detail: ${pgErrorDetail || 'No additional details'}. ` +
                    `This may indicate a database migration issue. Please ensure all migrations have been run.`,
                );
            }

            // Check for not null constraint violations (23502)
            if (pgErrorCode === '23502' || pgErrorMessage.includes('not null')) {
                throw new BadRequestException(
                    `Required field is missing. Error: ${pgErrorMessage}. ` +
                    `Detail: ${pgErrorDetail || 'No additional details'}. ` +
                    `Please check that all required fields are provided.`,
                );
            }

            // Check for unique constraint violations (23505)
            if (pgErrorCode === '23505' || pgErrorMessage.includes('unique constraint')) {
                throw new BadRequestException(
                    `Unique constraint violation: ${pgErrorMessage}. ` +
                    `Detail: ${pgErrorDetail || 'No additional details'}. ` +
                    `A pipeline with this configuration may already exist.`,
                );
            }

            // Re-throw with more context
            throw new BadRequestException(
                `Failed to create pipeline: ${pgErrorMessage}. ` +
                `Source schema: ${sourceSchema.id}, Destination schema: ${destinationSchema.id}. ` +
                `PostgreSQL error code: ${pgErrorCode || 'unknown'}. ` +
                `If this persists, please check database migrations and constraints.`,
            );
        }
    }

    /**
     * Execute full pipeline: source → transform → destination
     */
    async executePipeline(pipelineId: string): Promise<PipelineRunResult> {
        this.logger.log(`Executing pipeline ${pipelineId}`);

        const pipelineWithSchemas = await this.pipelineRepository.findByIdWithSchemas(pipelineId);
        if (!pipelineWithSchemas) {
            throw new NotFoundException(`Pipeline ${pipelineId} not found`);
        }

        const { pipeline, sourceSchema, destinationSchema } = pipelineWithSchemas;

        if (pipeline.status !== 'active') {
            throw new BadRequestException(
                `Pipeline is ${pipeline.status}, cannot execute`,
            );
        }

        // Validate and fix incremental column if it's a UUID
        // UUIDs cannot be used for incremental sync as they are not sequential
        if (pipeline.incrementalColumn && pipeline.syncMode === 'incremental') {
            const fixedIncrementalColumn = await this.validateAndFixIncrementalColumn(
                pipeline,
                sourceSchema,
            );
            
            if (fixedIncrementalColumn !== pipeline.incrementalColumn) {
                this.logger.warn(
                    `Pipeline ${pipelineId}: Incremental column '${pipeline.incrementalColumn}' is UUID-based and cannot be used. Switching to '${fixedIncrementalColumn || 'none'}'`,
                );
                
                if (fixedIncrementalColumn) {
                    // Update pipeline with the correct incremental column
                    await this.pipelineRepository.update(pipeline.id, {
                        incrementalColumn: fixedIncrementalColumn,
                        lastSyncValue: null, // Reset lastSyncValue when switching columns
                    } as any);
                    pipeline.incrementalColumn = fixedIncrementalColumn;
                    pipeline.lastSyncValue = null;
                } else {
                    // No suitable column found - disable incremental sync
                    this.logger.error(
                        `Pipeline ${pipelineId}: Cannot use UUID column '${pipeline.incrementalColumn}' for incremental sync and no timestamp column found. Disabling incremental sync.`,
                    );
                    await this.pipelineRepository.update(pipeline.id, {
                        syncMode: 'full',
                        incrementalColumn: null,
                        lastSyncValue: null,
                    } as any);
                    pipeline.syncMode = 'full';
                    pipeline.incrementalColumn = null;
                    pipeline.lastSyncValue = null;
                }
            }
        }

        // Create pipeline run record first
        // Job state starts as 'setup' - will be updated to 'running' after table resolution
        const run = await this.pipelineRepository.createRun({
            pipelineId: pipeline.id,
            orgId: pipeline.orgId,
            status: 'running',
            jobState: 'setup' as any, // Start in setup phase
            jobStateUpdatedAt: new Date(),
            startedAt: new Date(),
            triggeredBy: pipeline.userId,
            triggerType: 'manual',
            runMetadata: { batchSize: 1000 },
        } as any);

        // Update pipeline migration state to 'running'
        this.logger.log(
            `[${run.id}] Setting pipeline migration state to 'running'`,
        );
        const runningPipeline = await this.pipelineRepository.update(pipeline.id, {
            migrationState: 'running',
        } as any);
        this.logger.log(
            `[${run.id}] Pipeline migration state set to: ${runningPipeline.migrationState}`,
        );

        const startTime = Date.now();
        const errors: PipelineError[] = [];

        try {
            // ============================================================================
            // SETUP PHASE: Resolve destination table BEFORE migration execution
            // ============================================================================
            // This ensures table creation happens ONLY in setup phase, not during migration
            // Field mappings are the SINGLE source of truth for schema creation
            
            this.logger.log(`[${run.id}] [SETUP] Resolving destination table and preparing schema`);
            
            // Extract field mappings from transformers in pipeline configuration
            const pipelineConfig = pipeline.transformations as any;
            const collectors = pipelineConfig?.collectors || [];
            const transformers = collectors.flatMap((c: any) => c.transformers || []);
            
            this.logger.log(
                `[${run.id}] [SETUP] Found ${transformers.length} transformers, ${collectors.length} collectors`,
            );
            
            // Log transformer details for debugging
            if (transformers.length > 0) {
                transformers.forEach((t: any, idx: number) => {
                    this.logger.log(
                        `[${run.id}] [SETUP] Transformer ${idx}: id=${t.id || 'unknown'}, hasFieldMappings=${!!t.fieldMappings}, fieldMappingsCount=${Array.isArray(t.fieldMappings) ? t.fieldMappings.length : 0}`,
                    );
                });
            } else {
                this.logger.warn(
                    `[${run.id}] [SETUP] No transformers found in collectors. Collectors: ${JSON.stringify(collectors.map((c: any) => ({ id: c.id, transformersCount: c.transformers?.length || 0 })))}`,
                );
            }
            
            // Build column mappings from transformer fieldMappings (SINGLE SOURCE OF TRUTH)
            // Check ALL transformers for fieldMappings, not just the first one
            let columnMappings: ColumnMapping[] = [];
            let foundFieldMappings: Array<{ source: string; destination: string; isPrimaryKey?: boolean }> = [];
            let primaryKeyField: string | undefined;
            
            // Search through all transformers to find fieldMappings
            for (const transformer of transformers) {
                if (transformer?.fieldMappings && Array.isArray(transformer.fieldMappings) && transformer.fieldMappings.length > 0) {
                    foundFieldMappings = transformer.fieldMappings as Array<{ source: string; destination: string; isPrimaryKey?: boolean }>;
                    primaryKeyField = transformer.primaryKeyField || foundFieldMappings.find(fm => fm.isPrimaryKey)?.destination;
                    this.logger.log(
                        `[${run.id}] [SETUP] Found ${foundFieldMappings.length} field mappings in transformer ${transformer.id || 'unknown'}`,
                    );
                    break; // Use the first transformer with field mappings
                }
            }
            
            if (foundFieldMappings.length > 0) {
                // Use field mappings from transformers
                
                this.logger.log(
                    `[${run.id}] [SETUP] Using ${foundFieldMappings.length} field mappings from transformer${primaryKeyField ? ` with primary key: ${primaryKeyField}` : ''}`,
                );
                // Ensure only ONE primary key is set
                let primaryKeySet = false;
                columnMappings = foundFieldMappings.map((fm) => {
                    // Handle schema-qualified source column names (e.g., "company.companies.id" -> "id")
                    // Extract just the column name from the source field
                    const sourceColumn = fm.source.includes('.') 
                        ? fm.source.split('.').pop() || fm.source
                        : fm.source;
                    
                    // Check if this is marked as primary key or is the primary key field
                    // Only set ONE primary key - the first one found
                    let isPrimaryKey = false;
                    if (!primaryKeySet) {
                        isPrimaryKey = fm.isPrimaryKey || fm.destination === primaryKeyField || 
                                      (fm.destination.toLowerCase() === 'id' && !primaryKeyField);
                        if (isPrimaryKey) {
                            primaryKeySet = true;
                            this.logger.log(
                                `[${run.id}] [SETUP] Setting '${fm.destination}' as primary key`,
                            );
                        }
                    }
                    
                    // Check if this is an ID field (destination column is 'id' or ends with '_id')
                    const isIdField = fm.destination.toLowerCase() === 'id' || 
                                     fm.destination.toLowerCase().endsWith('_id');
                    
                    return {
                        sourceColumn: sourceColumn,
                        destinationColumn: fm.destination,
                        dataType: isIdField || isPrimaryKey ? 'UUID' : 'TEXT', // Use UUID for ID/primary key fields
                        nullable: !isPrimaryKey, // Primary key fields should not be nullable
                        isPrimaryKey: isPrimaryKey, // Use explicit primary key flag
                    };
                });
            } else if (destinationSchema.columnMappings && destinationSchema.columnMappings.length > 0) {
                // Fall back to destination schema column mappings
                this.logger.log(
                    `[${run.id}] [SETUP] Using ${destinationSchema.columnMappings.length} column mappings from destination schema`,
                );
                columnMappings = destinationSchema.columnMappings;
            } else {
                // Last resort: throw error - field mappings are required
                this.logger.error(
                    `[${run.id}] [SETUP] No field mappings found. Transformers: ${JSON.stringify(transformers.map((t: any) => ({ id: t.id, hasFieldMappings: !!t.fieldMappings, fieldMappingsCount: Array.isArray(t.fieldMappings) ? t.fieldMappings.length : 0 })))}, Destination schema mappings: ${destinationSchema.columnMappings ? destinationSchema.columnMappings.length : 0} mappings`,
                );
                throw new BadRequestException(
                    `No field mappings found. Please configure field mappings in the transformer before running migration. Found ${transformers.length} transformer(s) but none have fieldMappings configured.`,
                );
            }
            
            if (columnMappings.length === 0) {
                throw new BadRequestException(
                    'No column mappings found. Please configure field mappings in the transformer.',
                );
            }
            
            this.logger.log(
                `[${run.id}] [SETUP] Using ${columnMappings.length} column mappings: ${columnMappings.map(m => `${m.sourceColumn} -> ${m.destinationColumn}`).join(', ')}`,
            );
            
            // ============================================================================
            // SETUP PHASE: Resolve destination table and LOCK it in run record
            // ============================================================================
            // This is the SINGLE point where table creation decisions are made
            // The resolved table is stored in the run record and becomes AUTHORITATIVE
            // Migration execution MUST read from run record, not resolve again
            
            this.logger.log(`[${run.id}] [SETUP] Resolving and locking destination table`);
            
            const tableResolution = await this.resolveAndPrepareDestinationTable(
                pipeline,
                destinationSchema,
                columnMappings,
                run.id,
            );
            
            // LOCK the resolved destination table in run record (AUTHORITATIVE)
            // This ensures migration execution cannot override or recreate tables
            await this.pipelineRepository.updateRun(run.id, {
                resolvedDestinationSchema: destinationSchema.destinationSchema ?? 'public',
                resolvedDestinationTable: destinationSchema.destinationTable,
                destinationTableWasCreated: tableResolution.tableWasCreated ? 'true' : 'false',
                resolvedColumnMappings: tableResolution.columnMappings as any,
                jobState: 'running' as any, // Set job state to running after setup
                jobStateUpdatedAt: new Date(),
            } as any);
            
            // Refresh run record to get the locked values
            const lockedRun = await this.pipelineRepository.findRunById(run.id);
            if (!lockedRun) {
                throw new NotFoundException(`Pipeline run ${run.id} not found after setup`);
            }
            
            this.logger.log(
                `[${run.id}] [SETUP] Destination table LOCKED in run record: ${lockedRun.resolvedDestinationSchema}.${lockedRun.resolvedDestinationTable} (wasCreated: ${lockedRun.destinationTableWasCreated})`,
            );
            
            // Use resolved column mappings from locked run record
            const resolvedColumnMappings = (lockedRun.resolvedColumnMappings || tableResolution.columnMappings) as ColumnMapping[];
            
            if (!resolvedColumnMappings || resolvedColumnMappings.length === 0) {
                throw new BadRequestException(
                    'Resolved column mappings not found in run record. Setup phase failed.',
                );
            }

            // ============================================================================
            // MIGRATION PHASE: Read → Transform → Write
            // ============================================================================
            
            // Step 1: Read from source
            // IMPORTANT: Use cursor from run record (AUTHORITATIVE) if available
            // Otherwise fall back to pipeline.lastSyncValue (legacy)
            // Also read resolved destination table info for later use
            const currentRun = await this.pipelineRepository.findRunById(run.id);
            if (!currentRun || !currentRun.resolvedDestinationTable) {
                throw new BadRequestException(
                    `Run record ${run.id} does not have resolved destination table. Setup phase must complete before migration.`,
                );
            }
            const syncCursor = currentRun?.lastSyncCursor || pipeline.lastSyncValue || null;
            
            this.logger.log(`[${run.id}] Step 1: Reading from source${syncCursor ? ` (cursor: ${syncCursor})` : ' (full sync)'}`);
            const sourceData = await this.readFromSource(
                pipeline,
                sourceSchema,
                syncCursor, // Use cursor from run record (AUTHORITATIVE)
                run.id,
            );

            // Step 2: Transform data
            this.logger.log(
                `[${run.id}] Step 2: Transforming ${sourceData.rows.length} rows`,
            );
            
            // Extract transformations array from pipeline config
            // pipeline.transformations is a JSONB object: { collectors: [], emitters: [], transformations: [] }
            let legacyTransformations: Transformation[] = [];
            
            try {
                if (pipeline.transformations) {
                    if (Array.isArray(pipeline.transformations)) {
                        // Legacy format: transformations is directly an array
                        legacyTransformations = pipeline.transformations;
                        this.logger.log(
                            `[${run.id}] Found legacy transformations array format with ${legacyTransformations.length} items`,
                        );
                    } else if (typeof pipeline.transformations === 'object') {
                        // New format: transformations is an object with collectors/emitters/transformations
                        const pipelineConfig = pipeline.transformations as any;
                        
                        // First, try to get transformations array from the config
                        if (pipelineConfig.transformations !== undefined) {
                            if (Array.isArray(pipelineConfig.transformations)) {
                                legacyTransformations = pipelineConfig.transformations;
                                this.logger.log(
                                    `[${run.id}] Found ${legacyTransformations.length} transformations in pipeline config`,
                                );
                            } else {
                                this.logger.warn(
                                    `[${run.id}] pipelineConfig.transformations is not an array: ${typeof pipelineConfig.transformations}`,
                                );
                            }
                        }
                        
                        // If transformations array is empty, extract from collectors
                        if (legacyTransformations.length === 0) {
                            const collectors = pipelineConfig?.collectors || [];
                            this.logger.log(
                                `[${run.id}] Extracting transformations from ${collectors.length} collectors`,
                            );
                            collectors.forEach((collector: any) => {
                                if (collector.transformers && Array.isArray(collector.transformers)) {
                                    collector.transformers.forEach((transformer: any) => {
                                        if (transformer.fieldMappings && Array.isArray(transformer.fieldMappings)) {
                                            transformer.fieldMappings.forEach((mapping: { source: string; destination: string }) => {
                                                legacyTransformations.push({
                                                    sourceColumn: mapping.source.includes('.') 
                                                        ? mapping.source.split('.').pop() || mapping.source
                                                        : mapping.source,
                                                    destinationColumn: mapping.destination,
                                                    transformType: 'rename',
                                                    transformConfig: {},
                                                });
                                            });
                                        }
                                    });
                                }
                            });
                        }
                    }
                }
            } catch (error) {
                this.logger.error(
                    `[${run.id}] Error extracting transformations: ${error instanceof Error ? error.message : String(error)}`,
                );
                // Fall back to empty array
                legacyTransformations = [];
            }
            
            // Ensure it's always an array
            if (!Array.isArray(legacyTransformations)) {
                this.logger.warn(
                    `[${run.id}] legacyTransformations is not an array, converting to empty array. Type: ${typeof legacyTransformations}`,
                );
                legacyTransformations = [];
            }
            
            this.logger.log(
                `[${run.id}] Using ${legacyTransformations.length} legacy transformations`,
            );
            
            let transformedData = await this.transformData(
                sourceData.rows,
                resolvedColumnMappings,
                legacyTransformations,
            );
            
            // Ensure ID field exists in transformed data - add it if missing
            const hasIdInMappings = resolvedColumnMappings.some(m => m.destinationColumn.toLowerCase() === 'id');
            if (hasIdInMappings && transformedData.length > 0) {
                const idMapping = resolvedColumnMappings.find(m => m.destinationColumn.toLowerCase() === 'id');
                const hasIdInData = transformedData[0].hasOwnProperty('id') || transformedData[0].hasOwnProperty('ID');
                
                if (!hasIdInData && idMapping) {
                    // Generate UUID for each row if ID is missing
                    transformedData = transformedData.map((row, index) => {
                        // Try to get ID from source if sourceColumn exists
                        const sourceId = idMapping.sourceColumn && row[idMapping.sourceColumn] 
                            ? row[idMapping.sourceColumn]
                            : randomUUID();
                        return {
                            ...row,
                            id: sourceId,
                        };
                    });
                    this.logger.log(
                        `[${run.id}] Added 'id' field to ${transformedData.length} transformed rows`,
                    );
                }
            }

            // Step 3: Write to destination (ONLY writes data - table already resolved and locked)
            // IMPORTANT: Use resolved table from run record (AUTHORITATIVE - already read in Step 1)
            this.logger.log(
                `[${run.id}] Step 3: Writing ${transformedData.length} rows to destination`,
            );
            
            // Use resolved table from run record (AUTHORITATIVE - cannot be overridden)
            // currentRun was already read and validated in Step 1
            const resolvedTable = {
                schema: currentRun.resolvedDestinationSchema || 'public',
                table: currentRun.resolvedDestinationTable,
                columnMappings: (currentRun.resolvedColumnMappings || resolvedColumnMappings) as ColumnMapping[],
            };
            
            this.logger.log(
                `[${run.id}] [MIGRATION] Using LOCKED destination table: ${resolvedTable.schema}.${resolvedTable.table} (from run record)`,
            );
            
            const writeResult = await this.writeToDestination(
                transformedData,
                pipeline,
                destinationSchema,
                resolvedTable, // Pass resolved table from run record
                run.id,
            );

            // Step 4: Update run record with execution results and cursor
            const durationSeconds = Math.floor((Date.now() - startTime) / 1000);

            // Calculate last sync cursor for incremental sync
            // IMPORTANT: For timestamp columns, we need to handle edge cases where multiple records
            // have the same timestamp. We update lastSyncCursor to the max value found, so the next
            // query using >= will catch any remaining records with that timestamp.
            let lastSyncCursor: string | null = null;
            if (pipeline.incrementalColumn && sourceData.rows.length > 0) {
                const incrementalCol = pipeline.incrementalColumn;
                const isTimestampColumn = incrementalCol.toLowerCase().includes('_at') || 
                                         incrementalCol.toLowerCase().includes('at') ||
                                         incrementalCol.toLowerCase() === 'updated' ||
                                         incrementalCol.toLowerCase() === 'created' ||
                                         incrementalCol.toLowerCase() === 'modified';

                // Find the max value of the incremental column
                // For timestamps, we need to properly compare date/time values
                // For sequential IDs, we compare as strings/numbers
                let maxValue: string | null = null;
                
                for (const row of sourceData.rows) {
                    const value = row[incrementalCol];
                    if (value !== null && value !== undefined) {
                        let strValue: string;
                        
                        if (isTimestampColumn && value instanceof Date) {
                            // For Date objects, convert to ISO string for consistent comparison
                            strValue = value.toISOString();
                        } else if (isTimestampColumn && typeof value === 'string') {
                            // For timestamp strings, ensure they're in a comparable format
                            // PostgreSQL timestamps are already in ISO format, so use as-is
                            strValue = value;
                        } else {
                            // For other types (numbers, sequential IDs), convert to string
                            strValue = String(value);
                        }
                        
                        if (maxValue === null || strValue > maxValue) {
                            maxValue = strValue;
                        }
                    }
                }
                
                if (maxValue !== null) {
                    lastSyncCursor = maxValue;
                    this.logger.log(
                        `[${run.id}] Updated lastSyncCursor for incremental column '${incrementalCol}': ${lastSyncCursor} (${isTimestampColumn ? 'timestamp' : 'sequential'})`,
                    );
                } else {
                    this.logger.warn(
                        `[${run.id}] No valid values found for incremental column '${incrementalCol}' in ${sourceData.rows.length} rows`,
                    );
                }
            } else if (pipeline.incrementalColumn && sourceData.rows.length === 0) {
                // Keep existing cursor if no new rows
                const existingRun = await this.pipelineRepository.findRunById(run.id);
                lastSyncCursor = existingRun?.lastSyncCursor || null;
                this.logger.log(
                    `[${run.id}] No new rows found for incremental sync. Keeping lastSyncCursor: ${lastSyncCursor || 'null'}`,
                );
            }

            // Determine if this is the first successful run
            const isFirstSuccessfulRun = (pipeline.totalRunsSuccessful ?? 0) === 0;

            // Update run record with execution results and job state
            await this.pipelineRepository.updateRun(run.id, {
                status: 'success',
                jobState: 'completed' as any, // Set job state to completed
                jobStateUpdatedAt: new Date(),
                lastSyncCursor: lastSyncCursor, // Store cursor in run record (AUTHORITATIVE)
                rowsRead: sourceData.rows.length,
                rowsWritten: writeResult.rowsWritten,
                rowsSkipped: writeResult.rowsSkipped,
                rowsFailed: writeResult.rowsFailed,
                completedAt: new Date(),
                durationSeconds,
            } as any);

            // Update pipeline statistics and switch to incremental mode after first successful run
            const updateData: any = {
                lastRunAt: new Date(),
                lastRunStatus: 'success',
                migrationState: 'listing', // Switch to 'listing' state after successful migration
                totalRowsProcessed:
                    (pipeline.totalRowsProcessed ?? 0) + writeResult.rowsWritten,
                totalRunsSuccessful: (pipeline.totalRunsSuccessful ?? 0) + 1,
                lastError: null,
                // Schedule next check in 1 minute after successful migration
                // This ensures we check for new records soon after migration completes
                nextSyncAt: new Date(Date.now() + 60 * 1000),
            };
            
            this.logger.log(
                `[${run.id}] Updating pipeline state to 'listing' after successful migration`,
            );

            // After first successful run, automatically switch to incremental mode
            // IMPORTANT: Prefer timestamp-based columns (updated_at, created_at) over UUIDs
            // UUIDs are not sequential and cannot be reliably used for incremental sync
            if (isFirstSuccessfulRun && pipeline.syncMode === 'full') {
                // Priority order: timestamp columns first (most reliable), then sequential IDs, avoid UUIDs
                // Timestamp columns are preferred because they represent actual time progression
                const timestampColumns = ['updated_at', 'updatedAt', 'created_at', 'createdAt', 'modified_at', 'modifiedAt'];
                const sequentialIdColumns = ['id', 'sequence_id', 'seq_id']; // Only if not UUID
                const sourceColumns = sourceData.rows.length > 0 ? Object.keys(sourceData.rows[0]) : [];
                
                // First, try to find a timestamp column
                let detectedIncrementalColumn = timestampColumns.find(col => 
                    sourceColumns.includes(col)
                );

                // If no timestamp found, check for sequential ID (but verify it's not a UUID)
                if (!detectedIncrementalColumn) {
                    const idColumn = sequentialIdColumns.find(col => sourceColumns.includes(col));
                    if (idColumn === 'id' && sourceData.rows.length > 0) {
                        // Check if the ID column contains UUIDs (UUIDs are 36 chars with dashes)
                        const sampleId = sourceData.rows[0][idColumn];
                        const isUUID = typeof sampleId === 'string' && 
                                      sampleId.length === 36 && 
                                      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(sampleId);
                        
                        if (!isUUID) {
                            detectedIncrementalColumn = idColumn;
                        } else {
                            this.logger.warn(
                                `[${run.id}] ID column appears to be UUID-based. UUIDs cannot be used for incremental sync. Please configure a timestamp column (updated_at, created_at) manually.`,
                            );
                        }
                    } else if (idColumn) {
                        detectedIncrementalColumn = idColumn;
                    }
                }

                if (detectedIncrementalColumn) {
                    updateData.syncMode = 'incremental';
                    updateData.incrementalColumn = detectedIncrementalColumn;
                    this.logger.log(
                        `[${run.id}] First successful run completed. Automatically switching to incremental mode with column: ${detectedIncrementalColumn} (${timestampColumns.includes(detectedIncrementalColumn) ? 'timestamp-based' : 'sequential ID'})`,
                    );
                } else {
                    this.logger.warn(
                        `[${run.id}] First successful run completed but could not detect a suitable incremental column. Please manually configure a timestamp column (updated_at, created_at) for incremental sync. Staying in full sync mode.`,
                    );
                }
            }

            // Update lastSyncValue if we have one (for pipeline record - legacy)
            // NOTE: The authoritative cursor is stored in run record (lastSyncCursor)
            if (lastSyncCursor !== null) {
                updateData.lastSyncValue = lastSyncCursor;
            }

            const updatedPipeline = await this.pipelineRepository.update(pipeline.id, updateData);
            this.logger.log(
                `[${run.id}] Pipeline state updated. Migration state: ${updatedPipeline.migrationState}, Last run status: ${updatedPipeline.lastRunStatus}`,
            );

            this.logger.log(
                `[${run.id}] Pipeline execution completed successfully in ${durationSeconds}s`,
            );

            return {
                runId: run.id,
                status: 'success',
                rowsRead: sourceData.rows.length,
                rowsWritten: writeResult.rowsWritten,
                rowsSkipped: writeResult.rowsSkipped,
                rowsFailed: writeResult.rowsFailed,
                durationSeconds,
                errors,
            };
        } catch (error) {
            const errorMessage =
                error instanceof Error ? error.message : 'Unknown error';
            this.logger.error(`[${run.id}] Pipeline execution failed: ${errorMessage}`);

            const durationSeconds = Math.floor((Date.now() - startTime) / 1000);

            // Update run record with error state
            await this.pipelineRepository.updateRun(run.id, {
                status: 'failed',
                jobState: 'error' as any, // Set job state to error
                jobStateUpdatedAt: new Date(),
                errorMessage,
                errorStack: error instanceof Error ? error.stack : undefined,
                completedAt: new Date(),
                durationSeconds,
            } as any);

            this.logger.log(
                `[${run.id}] Setting pipeline migration state to 'pending' due to failure`,
            );
            const failedPipeline = await this.pipelineRepository.update(pipeline.id, {
                lastRunAt: new Date(),
                lastRunStatus: 'failed',
                migrationState: 'pending', // Revert to 'pending' on failure
                lastError: errorMessage,
                totalRunsFailed: (pipeline.totalRunsFailed ?? 0) + 1,
            } as any);
            this.logger.log(
                `[${run.id}] Pipeline migration state set to: ${failedPipeline.migrationState}`,
            );

            throw error;
        }
    }

    /**
     * Lightweight check if new records exist (for cron job)
     * This is a fast existence check - the actual migration is queued separately
     * to fetch and migrate ALL new records
     */
    async hasNewRecords(pipelineId: string): Promise<boolean> {
        const pipelineWithSchemas = await this.pipelineRepository.findByIdWithSchemas(pipelineId);
        if (!pipelineWithSchemas) {
            this.logger.warn(`Pipeline ${pipelineId} not found`);
            return false;
        }

        const { pipeline, sourceSchema } = pipelineWithSchemas;

        // Only check if pipeline is active and has incremental column configured
        if (pipeline.status !== 'active') {
            return false;
        }

        // Only check pipelines in 'running' or 'listing' state
        if (pipeline.migrationState !== 'running' && pipeline.migrationState !== 'listing') {
            return false;
        }

        if (!pipeline.incrementalColumn) {
            return false;
        }

        if (sourceSchema.sourceType !== 'postgres' || !sourceSchema.sourceConnectionId || !sourceSchema.sourceTable) {
            return false;
        }

        // Check if incremental column is UUID type - if so, skip (UUIDs can't be used for incremental sync)
        try {
            let pool = this.connectionPool.getPool(sourceSchema.sourceConnectionId);
            if (!pool) {
                const connection = await this.connectionRepository.findById(
                    sourceSchema.sourceConnectionId,
                    pipeline.orgId,
                );
                if (!connection || connection.status !== 'active') {
                    return false;
                }
                const credentials = this.connectionRepository.decryptCredentials(connection);
                pool = await this.connectionPool.createPool(
                    sourceSchema.sourceConnectionId,
                    credentials,
                );
            }

            const client = await pool.connect();

            try {
                const schema = sourceSchema.sourceSchema || 'public';
                const table = sourceSchema.sourceTable;
                const incrementalCol = pipeline.incrementalColumn;

                // Check column type
                const typeQuery = `
                    SELECT c.data_type, c.udt_name
                    FROM information_schema.columns c
                    WHERE c.table_schema = $1
                      AND c.table_name = $2
                      AND c.column_name = $3
                `;

                const typeResult = await client.query(typeQuery, [schema, table, incrementalCol]);
                
                if (typeResult.rows.length > 0) {
                    const dataType = (typeResult.rows[0].data_type || '').toLowerCase();
                    const udtName = (typeResult.rows[0].udt_name || '').toLowerCase();
                    const isUUID = dataType === 'uuid' || udtName === 'uuid';

                    if (isUUID) {
                        this.logger.warn(
                            `Pipeline ${pipelineId}: Incremental column '${incrementalCol}' is UUID type. UUIDs cannot be used for incremental sync. Skipping check.`,
                        );
                        return false; // Skip UUID-based incremental columns
                    }
                }

                const lastSyncValue = pipeline.lastSyncValue;

                // Determine if this is a timestamp column
                const isTimestampColumn = incrementalCol.toLowerCase().includes('_at') || 
                                         incrementalCol.toLowerCase().includes('at') ||
                                         incrementalCol.toLowerCase() === 'updated' ||
                                         incrementalCol.toLowerCase() === 'created' ||
                                         incrementalCol.toLowerCase() === 'modified';

                let query: string;
                const params: any[] = [];

                if (lastSyncValue) {
                    // For timestamp columns, use >= to catch records with same timestamp
                    // For sequential IDs, use > to avoid re-processing
                    const comparisonOp = isTimestampColumn ? '>=' : '>';
                    // Lightweight check: count records (faster than fetching all)
                    query = `
                        SELECT COUNT(*) as count FROM "${schema}"."${table}"
                        WHERE "${incrementalCol}" ${comparisonOp} $1
                    `;
                    params.push(lastSyncValue);
                } else {
                    // First run - check if table has any records
                    query = `SELECT COUNT(*) as count FROM "${schema}"."${table}"`;
                }

                const result = await client.query(query, params);
                const count = parseInt(result.rows[0]?.count || '0', 10);

                return count > 0;
            } finally {
                client.release();
            }
        } catch (error) {
            this.logger.error(
                `Error checking for new records in pipeline ${pipelineId}: ${error.message}`,
                error.stack,
            );
            return false;
        }
    }

    /**
     * Validate and fix incremental column
     * If the incremental column is UUID type, try to find a timestamp column to use instead
     * Returns the column name to use, or null if no suitable column found
     */
    private async validateAndFixIncrementalColumn(
        pipeline: PostgresPipeline,
        sourceSchema: PipelineSourceSchema,
    ): Promise<string | null> {
        if (!pipeline.incrementalColumn || !sourceSchema.sourceTable || !sourceSchema.sourceConnectionId) {
            return pipeline.incrementalColumn || null;
        }

        try {
            // Get connection pool
            let pool = this.connectionPool.getPool(sourceSchema.sourceConnectionId);
            if (!pool) {
                const connection = await this.connectionRepository.findById(
                    sourceSchema.sourceConnectionId,
                    pipeline.orgId,
                );
                if (!connection || connection.status !== 'active') {
                    return pipeline.incrementalColumn; // Return original if can't check
                }
                const credentials = this.connectionRepository.decryptCredentials(connection);
                pool = await this.connectionPool.createPool(
                    sourceSchema.sourceConnectionId,
                    credentials,
                );
            }

            const client = await pool.connect();

            try {
                const schema = sourceSchema.sourceSchema || 'public';
                const table = sourceSchema.sourceTable;
                const incrementalCol = pipeline.incrementalColumn;

                // Query column type from information_schema
                const typeQuery = `
                    SELECT 
                        c.column_name,
                        c.data_type,
                        c.udt_name
                    FROM information_schema.columns c
                    WHERE c.table_schema = $1
                      AND c.table_name = $2
                      AND c.column_name = $3
                `;

                const typeResult = await client.query(typeQuery, [schema, table, incrementalCol]);
                
                if (typeResult.rows.length === 0) {
                    this.logger.warn(
                        `Column '${incrementalCol}' not found in table ${schema}.${table}`,
                    );
                    return pipeline.incrementalColumn; // Return original
                }

                const columnInfo = typeResult.rows[0];
                const dataType = (columnInfo.data_type || '').toLowerCase();
                const udtName = (columnInfo.udt_name || '').toLowerCase();

                // Check if it's a UUID type
                const isUUID = dataType === 'uuid' || udtName === 'uuid';

                if (!isUUID) {
                    // Column is not UUID, it's fine to use
                    return pipeline.incrementalColumn;
                }

                // Column is UUID - need to find a timestamp column
                this.logger.warn(
                    `Incremental column '${incrementalCol}' is UUID type. UUIDs cannot be used for incremental sync. Searching for timestamp column...`,
                );

                // Find timestamp columns
                const timestampQuery = `
                    SELECT 
                        c.column_name,
                        c.data_type
                    FROM information_schema.columns c
                    WHERE c.table_schema = $1
                      AND c.table_name = $2
                      AND (
                          c.data_type IN ('timestamp without time zone', 'timestamp with time zone', 'timestamp', 'timestamptz')
                          OR c.udt_name IN ('timestamp', 'timestamptz')
                          OR LOWER(c.column_name) LIKE '%_at'
                          OR LOWER(c.column_name) IN ('updated_at', 'created_at', 'modified_at', 'updatedat', 'createdat', 'modifiedat')
                      )
                    ORDER BY 
                      CASE 
                        WHEN LOWER(c.column_name) = 'updated_at' THEN 1
                        WHEN LOWER(c.column_name) = 'created_at' THEN 2
                        WHEN LOWER(c.column_name) = 'modified_at' THEN 3
                        WHEN LOWER(c.column_name) LIKE '%_at' THEN 4
                        ELSE 5
                      END,
                      c.column_name
                    LIMIT 1
                `;

                const timestampResult = await client.query(timestampQuery, [schema, table]);

                if (timestampResult.rows.length > 0) {
                    const timestampColumn = timestampResult.rows[0].column_name;
                    this.logger.log(
                        `Found timestamp column '${timestampColumn}' to use instead of UUID column '${incrementalCol}'`,
                    );
                    return timestampColumn;
                }

                // No timestamp column found
                this.logger.error(
                    `No timestamp column found in table ${schema}.${table}. Cannot use UUID column '${incrementalCol}' for incremental sync.`,
                );
                return null;
            } finally {
                client.release();
            }
        } catch (error) {
            this.logger.error(
                `Error validating incremental column: ${error.message}`,
                error.stack,
            );
            // Return original column on error to avoid breaking the pipeline
            return pipeline.incrementalColumn;
        }
    }

    /**
     * Step 1: Read data from source
     */
    private async readFromSource(
        pipeline: PostgresPipeline,
        sourceSchema: PipelineSourceSchema,
        lastSyncValue?: string | null,
        runId?: string,
    ): Promise<{ rows: any[]; totalRows: number }> {
        if (sourceSchema.sourceType !== 'postgres') {
            throw new BadRequestException(
                `Source type ${sourceSchema.sourceType} not yet supported`,
            );
        }

        if (!sourceSchema.sourceConnectionId) {
            throw new BadRequestException('Source connection ID is required');
        }

        // Get or create connection pool
        let pool = this.connectionPool.getPool(sourceSchema.sourceConnectionId);
        if (!pool) {
            // Pool doesn't exist, create it
            const logPrefix = runId ? `[${runId}]` : '';
            this.logger.log(
                `${logPrefix} Creating connection pool for source connection ${sourceSchema.sourceConnectionId}`,
            );
            const connection = await this.connectionRepository.findById(
                sourceSchema.sourceConnectionId,
                pipeline.orgId,
            );
            if (!connection) {
            throw new BadRequestException(
                    `Source connection ${sourceSchema.sourceConnectionId} not found`,
                );
            }
            if (connection.status !== 'active') {
                throw new BadRequestException(
                    `Source connection is not active (status: ${connection.status})`,
                );
            }
            const credentials = this.connectionRepository.decryptCredentials(connection);
            pool = await this.connectionPool.createPool(
                sourceSchema.sourceConnectionId,
                credentials,
            );
        }

        const client = await pool.connect();

        try {
            let query: string;
            const params: any[] = [];

            if (sourceSchema.sourceQuery) {
                // Use custom query
                query = sourceSchema.sourceQuery;
            } else if (sourceSchema.sourceTable) {
                // Build query from table
                const schema = sourceSchema.sourceSchema || 'public';
                const table = sourceSchema.sourceTable;

                if (pipeline.syncMode === 'incremental' && pipeline.incrementalColumn) {
                    // IMPORTANT: Validate that incremental column is not UUID type
                    // UUIDs cannot be used for incremental sync as they are not sequential
                    const incrementalCol = pipeline.incrementalColumn;
                    
                    // Check column type to ensure it's not UUID
                    const typeCheckQuery = `
                        SELECT c.data_type, c.udt_name
                        FROM information_schema.columns c
                        WHERE c.table_schema = $1
                          AND c.table_name = $2
                          AND c.column_name = $3
                    `;
                    const typeCheckResult = await client.query(typeCheckQuery, [schema, table, incrementalCol]);
                    
                    if (typeCheckResult.rows.length > 0) {
                        const dataType = (typeCheckResult.rows[0].data_type || '').toLowerCase();
                        const udtName = (typeCheckResult.rows[0].udt_name || '').toLowerCase();
                        const isUUID = dataType === 'uuid' || udtName === 'uuid';
                        
                        if (isUUID) {
                            throw new BadRequestException(
                                `Cannot use UUID column '${incrementalCol}' for incremental sync. UUIDs are not sequential and cannot be reliably compared. The pipeline should have been automatically switched to a timestamp column. Please check pipeline configuration.`,
                            );
                        }
                    }

                    // Incremental sync - fetch ALL records greater than lastSyncValue
                    // Use >= for timestamp columns to ensure we don't miss records with the same timestamp
                    // For sequential IDs, use > to avoid re-processing
                    const isTimestampColumn = incrementalCol.toLowerCase().includes('_at') || 
                                             incrementalCol.toLowerCase().includes('at') ||
                                             incrementalCol.toLowerCase() === 'updated' ||
                                             incrementalCol.toLowerCase() === 'created' ||
                                             incrementalCol.toLowerCase() === 'modified';
                    
                    if (lastSyncValue) {
                        // For timestamp columns, use >= to catch records with same timestamp
                        // For sequential IDs, use > to avoid re-processing
                        const comparisonOp = isTimestampColumn ? '>=' : '>';
                        query = `
              SELECT * FROM "${schema}"."${table}"
              WHERE "${incrementalCol}" ${comparisonOp} $1
              ORDER BY "${incrementalCol}" ASC
            `;
                        params.push(lastSyncValue);
                        this.logger.log(
                            `[${runId}] Incremental sync: fetching ALL records where ${incrementalCol} ${comparisonOp} ${lastSyncValue} (${isTimestampColumn ? 'timestamp-based' : 'sequential'})`,
                        );
                    } else {
                        // First incremental run - get all records
                        query = `SELECT * FROM "${schema}"."${table}" ORDER BY "${incrementalCol}" ASC`;
                        this.logger.log(
                            `[${runId}] First incremental sync: fetching all records ordered by ${incrementalCol}`,
                        );
                    }
                } else {
                    // Full sync
                    query = `SELECT * FROM "${schema}"."${table}"`;
                }
            } else {
                throw new BadRequestException(
                    'Either sourceQuery or sourceTable must be specified',
                );
            }

            const result = await client.query(query, params);

            return {
                rows: result.rows,
                totalRows: result.rowCount || 0,
            };
        } finally {
            client.release();
        }
    }

    /**
     * Step 2: Transform data (apply mappings + transformations)
     */
    private async transformData(
        sourceData: any[],
        mappings: ColumnMapping[],
        transformations: Transformation[] | any,
    ): Promise<any[]> {
        if (sourceData.length === 0) return [];

        if (mappings.length === 0) {
            this.logger.warn('No column mappings provided, returning source data as-is');
            return sourceData;
        }

        // Ensure transformations is an array
        const transformationsArray = Array.isArray(transformations) 
            ? transformations 
            : [];

        const transformed = sourceData.map((row, rowIndex) => {
            const transformedRow: any = {};

            // IMPORTANT: Only migrate mapped fields - do NOT include unmapped source columns
            // This ensures data migration strictly follows the field-to-field mapping configuration
            mappings.forEach((mapping) => {
                let value = row[mapping.sourceColumn];

                // Handle generated UUID for primary key if sourceColumn is 'id' and has defaultValue
                if (mapping.sourceColumn === 'id' && mapping.defaultValue === 'gen_random_uuid()' && !value) {
                    // Generate UUID if not present - this will be handled by database default
                    // Just set to null/undefined and let database generate it
                    value = undefined;
                }

                // Apply transformations for this column
                const transformation = transformationsArray.find(
                    (t: Transformation) => t.sourceColumn === mapping.sourceColumn,
                );

                if (transformation) {
                    value = this.applyTransformation(value, transformation);
                }

                // Only add the mapped field to transformed row
                transformedRow[mapping.destinationColumn] = value;
            });

            // Log first row for debugging
            if (rowIndex === 0) {
                this.logger.log(
                    `First transformed row keys: ${Object.keys(transformedRow).join(', ')}`,
                );
            }

            return transformedRow;
        });

        // Validate that transformed rows have at least one column
        if (transformed.length > 0 && Object.keys(transformed[0]).length === 0) {
            this.logger.error(
                `Transformed data is empty. Mappings: ${JSON.stringify(mappings)}, First source row keys: ${Object.keys(sourceData[0] || {}).join(', ')}`,
            );
            throw new BadRequestException(
                'Transformed data is empty. Please check your field mappings.',
            );
        }

        return transformed;
    }

    /**
     * Apply single transformation
     */
    private applyTransformation(value: any, transformation: Transformation): any {
        switch (transformation.transformType) {
            case 'rename':
                return value;

            case 'cast':
                const targetType = transformation.transformConfig?.targetType;
                if (targetType === 'string') return String(value);
                if (targetType === 'number') return Number(value);
                if (targetType === 'boolean') return Boolean(value);
                return value;

            case 'concat':
                const separator = transformation.transformConfig?.separator || '';
                const fields = transformation.transformConfig?.fields || [];
                return fields.map((f: string) => value[f]).join(separator);

            case 'split':
                const splitSeparator = transformation.transformConfig?.separator || ',';
                const index = transformation.transformConfig?.index || 0;
                return String(value).split(splitSeparator)[index];

            case 'custom':
                // Custom transformation logic would go here
                return value;

            default:
                return value;
        }
    }

    /**
     * DESTINATION TABLE RESOLVER
     * 
     * This method resolves and prepares the destination table BEFORE migration execution.
     * It is the SINGLE source of truth for table creation decisions.
     * 
     * Rules:
     * 1. If destinationTableExists = true → MUST use existing table, NEVER create
     * 2. If destinationTableExists = false → Create new table ONLY if auto-generated name
     * 3. Field mappings are the SINGLE source of truth for schema creation
     * 4. Table creation happens ONLY in this setup phase, NEVER during migration
     * 
     * @returns Resolved column mappings and table existence status
     */
    private async resolveAndPrepareDestinationTable(
        pipeline: PostgresPipeline,
        destinationSchema: PipelineDestinationSchema,
        columnMappings: ColumnMapping[],
        runId?: string,
    ): Promise<{
        columnMappings: ColumnMapping[];
        tableExists: boolean;
        tableWasCreated: boolean;
    }> {
        const logPrefix = runId ? `[${runId}]` : '';
        this.logger.log(
            `${logPrefix} [TABLE RESOLVER] Resolving destination table: ${destinationSchema.destinationSchema ?? 'public'}.${destinationSchema.destinationTable}`,
        );

        // Get or create connection pool
        let pool = this.connectionPool.getPool(destinationSchema.destinationConnectionId);
        if (!pool) {
            this.logger.log(
                `${logPrefix} [TABLE RESOLVER] Creating connection pool for destination connection ${destinationSchema.destinationConnectionId}`,
            );
            const connection = await this.connectionRepository.findById(
                destinationSchema.destinationConnectionId,
                pipeline.orgId,
            );
            if (!connection) {
            throw new BadRequestException(
                    `Destination connection ${destinationSchema.destinationConnectionId} not found`,
                );
            }
            if (connection.status !== 'active') {
                throw new BadRequestException(
                    `Destination connection is not active (status: ${connection.status})`,
                );
            }
            const credentials = this.connectionRepository.decryptCredentials(connection);
            pool = await this.connectionPool.createPool(
                destinationSchema.destinationConnectionId,
                credentials,
            );
        }

        const client = await pool.connect();

        try {
            await client.query('BEGIN');

            // Step 1: Resolve column mappings (field mappings are the SINGLE source of truth)
            let finalColumnMappings = columnMappings || destinationSchema.columnMappings || [];
            
            if (finalColumnMappings.length === 0) {
                throw new BadRequestException(
                    'No column mappings found. Field mappings must be configured before migration.',
                );
            }

            // Ensure 'id' field exists in mappings (required for primary key)
                const hasIdField = finalColumnMappings.some(m => m.destinationColumn.toLowerCase() === 'id');
                if (!hasIdField) {
                // Add generated UUID 'id' field as primary key
                    finalColumnMappings.unshift({
                        sourceColumn: 'id',
                        destinationColumn: 'id',
                        dataType: 'UUID',
                        nullable: false,
                        isPrimaryKey: true,
                        defaultValue: 'gen_random_uuid()',
                    });
                    this.logger.log(
                    `${logPrefix} [TABLE RESOLVER] Added generated UUID 'id' field as primary key`,
                );
            }

            // Step 2: Check if table exists in database
            const tableExists = await this.destinationService.tableExists(
                client,
                destinationSchema.destinationSchema ?? 'public',
                destinationSchema.destinationTable,
            );

            // Step 3: Resolve table creation decision (DETERMINISTIC - no ambiguity)
            const isAutoGeneratedTable = destinationSchema.destinationTable.startsWith('pipeline_');
            let shouldCreateTable = false;
            let tableWasCreated = false;
            
            if (destinationSchema.destinationTableExists === true) {
                // RULE 1: User explicitly selected existing table - MUST use it, NEVER create
                if (!tableExists) {
                    // Table doesn't exist but user selected it - this is an error
                    throw new BadRequestException(
                        `Destination table "${destinationSchema.destinationSchema ?? 'public'}.${destinationSchema.destinationTable}" does not exist. Please create the table first or select a different table.`,
                    );
                }
                
                this.logger.log(
                    `${logPrefix} [TABLE RESOLVER] Using existing table (destinationTableExists=true). Will migrate ONLY mapped fields.`,
                );
                shouldCreateTable = false;
            } else if (destinationSchema.destinationTableExists === false) {
                // RULE 2: User wants new table - create ONLY if it doesn't exist
                if (!tableExists) {
                    shouldCreateTable = true;
                    this.logger.log(
                        `${logPrefix} [TABLE RESOLVER] Creating new table (destinationTableExists=false, table does not exist).`,
                    );
                } else {
                    // Table exists but user wants new - this is a conflict
                    // Update schema to reflect reality and use existing table
                    this.logger.warn(
                        `${logPrefix} [TABLE RESOLVER] Table already exists but destinationTableExists=false. Using existing table and updating schema.`,
                    );
                    await this.destinationSchemaRepository.update(destinationSchema.id, {
                        destinationTableExists: true,
                    });
                    shouldCreateTable = false;
                }
            } else {
                // destinationTableExists is null/undefined - determine based on table existence and name pattern
                if (tableExists) {
                    // Table exists - use it
                    this.logger.log(
                        `${logPrefix} [TABLE RESOLVER] Table exists. Using existing table and updating schema.`,
                    );
                    await this.destinationSchemaRepository.update(destinationSchema.id, {
                        destinationTableExists: true,
                    });
                    shouldCreateTable = false;
                } else if (isAutoGeneratedTable) {
                    // Auto-generated name and table doesn't exist - create it
                    shouldCreateTable = true;
                    this.logger.log(
                        `${logPrefix} [TABLE RESOLVER] Auto-generated table name and table does not exist. Creating new table.`,
                    );
                } else {
                    // Not auto-generated and doesn't exist - error
                    throw new BadRequestException(
                        `Destination table "${destinationSchema.destinationSchema ?? 'public'}.${destinationSchema.destinationTable}" does not exist. Please create the table first or use an auto-generated table name.`,
                    );
                }
            }

            // Step 4: Create table if needed (ONLY in setup phase)
            if (shouldCreateTable) {
                this.logger.log(
                    `${logPrefix} [TABLE RESOLVER] Creating destination table ${destinationSchema.destinationSchema ?? 'public'}.${destinationSchema.destinationTable} with ${finalColumnMappings.length} columns from field mappings`,
                );
                
                    await this.destinationService.createDestinationTable(
                        client,
                        destinationSchema.destinationSchema ?? 'public',
                        destinationSchema.destinationTable,
                        finalColumnMappings,
                    );

                // Update destination schema to mark table as existing and store mappings
                    await this.destinationSchemaRepository.update(destinationSchema.id, {
                        destinationTableExists: true,
                        columnMappings: finalColumnMappings as any,
                    });

                tableWasCreated = true;
                this.logger.log(
                    `${logPrefix} [TABLE RESOLVER] Table created successfully. Schema locked with ${finalColumnMappings.length} mapped fields.`,
                );
            } else {
                // Step 5: Validate and evolve existing table schema
                const validation = await this.destinationService.validateSchema(
                    client,
                    destinationSchema.destinationSchema ?? 'public',
                    destinationSchema.destinationTable,
                    finalColumnMappings,
                );

                if (!validation.valid) {
                    // Add missing columns (schema evolution)
                    if (validation.missingColumns && validation.missingColumns.length > 0) {
                        const missingMappings = finalColumnMappings.filter((m) =>
                            validation.missingColumns!.includes(m.destinationColumn),
                        );
                        this.logger.log(
                            `${logPrefix} [TABLE RESOLVER] Adding ${missingMappings.length} missing columns to existing table: ${validation.missingColumns!.join(', ')}`,
                        );
                        await this.destinationService.addMissingColumns(
                            client,
                            destinationSchema.destinationSchema ?? 'public',
                            destinationSchema.destinationTable,
                            missingMappings,
                        );
                    }
                }
                
                // Update schema with resolved mappings if not already set
                if (!destinationSchema.columnMappings || destinationSchema.columnMappings.length === 0) {
                    await this.destinationSchemaRepository.update(destinationSchema.id, {
                        columnMappings: finalColumnMappings as any,
                    });
                }
            }

            await client.query('COMMIT');

            this.logger.log(
                `${logPrefix} [TABLE RESOLVER] Resolution complete. Table exists: ${!shouldCreateTable || tableWasCreated}, Mapped fields: ${finalColumnMappings.length}`,
            );

            return {
                columnMappings: finalColumnMappings,
                tableExists: !shouldCreateTable || tableWasCreated,
                tableWasCreated,
            };
        } catch (error) {
            await client.query('ROLLBACK');
            throw error;
        } finally {
            client.release();
        }
    }

    /**
     * Step 3: Write to destination
     * 
     * ARCHITECTURAL NOTE:
     * This method ONLY writes data. It NEVER creates tables.
     * 
     * The resolved destination table is passed from the run record (AUTHORITATIVE).
     * This ensures:
     * - Table was resolved and locked during setup phase
     * - Migration execution cannot override or recreate tables
     * - Field mappings are the SINGLE source of truth
     * 
     * @param resolvedTable - Resolved table from run record (AUTHORITATIVE)
     */
    private async writeToDestination(
        transformedData: any[],
        pipeline: PostgresPipeline,
        destinationSchema: PipelineDestinationSchema,
        resolvedTable: {
            schema: string;
            table: string;
            columnMappings: ColumnMapping[];
        },
        runId?: string,
    ): Promise<{
        rowsWritten: number;
        rowsSkipped: number;
        rowsFailed: number;
    }> {
        const logPrefix = runId ? `[${runId}]` : '';
        
        // Validate resolved table (must come from run record)
        if (!resolvedTable.schema || !resolvedTable.table || !resolvedTable.columnMappings) {
            throw new BadRequestException(
                'Resolved destination table information is missing. Setup phase must complete before migration.',
            );
        }
        
        this.logger.log(
            `${logPrefix} [MIGRATION] Writing to LOCKED destination: ${resolvedTable.schema}.${resolvedTable.table}`,
        );
        
        // Get or create connection pool
        let pool = this.connectionPool.getPool(destinationSchema.destinationConnectionId);
        if (!pool) {
            this.logger.log(
                `${logPrefix} Creating connection pool for destination connection ${destinationSchema.destinationConnectionId}`,
            );
            const connection = await this.connectionRepository.findById(
                destinationSchema.destinationConnectionId,
                pipeline.orgId,
            );
            if (!connection) {
                throw new BadRequestException(
                    `Destination connection ${destinationSchema.destinationConnectionId} not found`,
                );
            }
            if (connection.status !== 'active') {
                throw new BadRequestException(
                    `Destination connection is not active (status: ${connection.status})`,
                );
            }
            const credentials = this.connectionRepository.decryptCredentials(connection);
            pool = await this.connectionPool.createPool(
                destinationSchema.destinationConnectionId,
                credentials,
            );
        }

        const client = await pool.connect();

        try {
            await client.query('BEGIN');

            // IMPORTANT: Filter transformed data to ONLY include mapped columns
            // Field mappings are the SINGLE source of truth for what gets migrated
            const mappedColumns = new Set(resolvedTable.columnMappings.map(m => m.destinationColumn));
            
            if (transformedData.length > 0) {
                    const firstRow = transformedData[0];
                    const unmappedColumns = Object.keys(firstRow).filter(col => !mappedColumns.has(col));
                    
                    if (unmappedColumns.length > 0) {
                        this.logger.warn(
                            `${logPrefix} Found ${unmappedColumns.length} unmapped columns in transformed data: ${unmappedColumns.join(', ')}. These will be excluded from migration.`,
                        );
                        
                        // Filter transformed data to only include mapped columns
                        transformedData = transformedData.map(row => {
                            const filteredRow: any = {};
                            mappedColumns.forEach(col => {
                                if (row.hasOwnProperty(col)) {
                                    filteredRow[col] = row[col];
                                }
                            });
                            return filteredRow;
                        });
                        
                        this.logger.log(
                            `${logPrefix} Filtered transformed data to only include ${mappedColumns.size} mapped columns.`,
                        );
                }
            }

            // Determine write mode and upsert key
            // PRIMARY KEY RULE: Only UUID is allowed as primary key
            // Same UUID → UPDATE existing row
            // New UUID → INSERT new row
            let writeMode: 'append' | 'upsert' | 'replace' = (destinationSchema.writeMode as 'append' | 'upsert' | 'replace') || 'append';
            let upsertKey: string[] | undefined = destinationSchema.upsertKey || undefined;
            
            // Find primary key from resolved column mappings
            const primaryKeyMapping = resolvedTable.columnMappings.find(m => m.isPrimaryKey);
            if (primaryKeyMapping) {
                // PRIMARY KEY RULE: Always use upsert for UUID primary keys
                if (primaryKeyMapping.dataType === 'UUID') {
                    writeMode = 'upsert';
                    upsertKey = [primaryKeyMapping.destinationColumn];
                    this.logger.log(
                        `${logPrefix} [MIGRATION] Primary key '${primaryKeyMapping.destinationColumn}' is UUID. Using upsert mode (same UUID → UPDATE, new UUID → INSERT).`,
                    );
                } else {
                    // Non-UUID primary key - still use upsert for consistency
                    writeMode = 'upsert';
                    upsertKey = [primaryKeyMapping.destinationColumn];
                    this.logger.log(
                        `${logPrefix} [MIGRATION] Primary key '${primaryKeyMapping.destinationColumn}' found. Using upsert mode.`,
                    );
                }
            }

            // Write data to LOCKED destination table (from run record)
            // NEVER creates tables - table was resolved and locked during setup
            const result = await this.destinationService.writeData(
                client,
                resolvedTable.schema, // Use resolved schema from run record
                resolvedTable.table,  // Use resolved table from run record
                transformedData,
                writeMode,
                upsertKey,
                writeMode === 'append', // Only prevent duplicates in append mode
            );

            await client.query('COMMIT');

            return result;
        } catch (error) {
            await client.query('ROLLBACK');
            throw error;
        } finally {
            client.release();
        }
    }

    /**
     * Validate pipeline configuration before execution
     */
    async validatePipeline(pipelineId: string): Promise<ValidationResult> {
        const pipelineWithSchemas = await this.pipelineRepository.findByIdWithSchemas(pipelineId);
        if (!pipelineWithSchemas) {
            throw new NotFoundException(`Pipeline ${pipelineId} not found`);
        }

        const { pipeline, sourceSchema, destinationSchema } = pipelineWithSchemas;

        const errors: string[] = [];
        const warnings: string[] = [];

        // Validate source configuration
        if (sourceSchema.sourceType === 'postgres') {
            if (!sourceSchema.sourceConnectionId) {
                errors.push('Source connection ID is required for postgres source');
            }
            if (!sourceSchema.sourceTable && !sourceSchema.sourceQuery) {
                errors.push('Either source table or source query must be specified');
            }
        }

        // Validate destination configuration
        if (!destinationSchema.destinationConnectionId) {
            errors.push('Destination connection ID is required');
        }
        if (!destinationSchema.destinationTable) {
            errors.push('Destination table is required');
        }

        // Validate write mode
        const writeMode = destinationSchema.writeMode as 'append' | 'upsert' | 'replace';
        if (writeMode === 'upsert' && (!destinationSchema.upsertKey || destinationSchema.upsertKey.length === 0)) {
            errors.push('Upsert key is required for upsert write mode');
        }

        // Validate sync mode
        if (pipeline.syncMode === 'incremental' && !pipeline.incrementalColumn) {
            errors.push('Incremental column is required for incremental sync mode');
        }

        // Validate column mappings
        if (!destinationSchema.columnMappings || destinationSchema.columnMappings.length === 0) {
            warnings.push('No column mappings defined. Auto-mapping recommended.');
        }

        return {
            valid: errors.length === 0,
            errors,
            warnings,
        };
    }

    /**
     * Dry run: Test pipeline without writing data
     */
    async dryRunPipeline(pipelineId: string): Promise<DryRunResult> {
        this.logger.log(`Dry run for pipeline ${pipelineId}`);

        const pipelineWithSchemas = await this.pipelineRepository.findByIdWithSchemas(pipelineId);
        if (!pipelineWithSchemas) {
            throw new NotFoundException(`Pipeline ${pipelineId} not found`);
        }

        const { pipeline, sourceSchema, destinationSchema } = pipelineWithSchemas;

        const startTime = Date.now();

        // Read sample data from source
        const sourceData = await this.readFromSource(pipeline, sourceSchema, null);
        const sampleRows = sourceData.rows.slice(0, 10);

        // Transform sample data
        const transformedSample = await this.transformData(
            sampleRows,
            destinationSchema.columnMappings || [],
            pipeline.transformations || [],
        );

        const estimatedDuration = Math.ceil((Date.now() - startTime) / 1000);

        return {
            sourceRowCount: sourceData.totalRows,
            sampleRows: transformedSample,
            destinationSchemaPreview: destinationSchema.columnMappings || [],
            estimatedDuration,
        };
    }

    /**
     * Pause/Resume pipeline
     */
    async togglePipeline(
        pipelineId: string,
        status: 'active' | 'paused',
    ): Promise<void> {
        const updateData: any = { status };
        
        // When pausing, also reset migration state to pending and clear nextSyncAt
        // This ensures the cron job will not process paused pipelines
        if (status === 'paused') {
            updateData.migrationState = 'pending';
            updateData.nextSyncAt = null; // Clear next sync time to prevent cron from picking it up
            this.logger.log(
                `Pipeline ${pipelineId} paused - resetting migration state to pending and clearing nextSyncAt`,
            );
        } else if (status === 'active') {
            // When resuming, set nextSyncAt to now so it will be checked soon
            updateData.nextSyncAt = new Date();
            this.logger.log(
                `Pipeline ${pipelineId} resumed - setting nextSyncAt to now for immediate check`,
            );
        }
        
        await this.pipelineRepository.update(pipelineId, updateData);
        this.logger.log(`Pipeline ${pipelineId} ${status}`);
    }

    /**
     * Delete pipeline and cleanup
     */
    async deletePipeline(
        pipelineId: string,
        dropTable: boolean = false,
    ): Promise<void> {
        const pipelineWithSchemas = await this.pipelineRepository.findByIdWithSchemas(pipelineId);
        if (!pipelineWithSchemas) {
            throw new NotFoundException(`Pipeline ${pipelineId} not found`);
        }

        const { destinationSchema } = pipelineWithSchemas;

        // Optionally drop destination table
        if (dropTable && destinationSchema.destinationTableExists) {
            const pool = this.connectionPool.getPool(
                destinationSchema.destinationConnectionId,
            );
            if (pool) {
                const client = await pool.connect();
                try {
                    await client.query(
                        `DROP TABLE IF EXISTS "${destinationSchema.destinationSchema}"."${destinationSchema.destinationTable}"`,
                    );
                    this.logger.log(
                        `Dropped table ${destinationSchema.destinationSchema}.${destinationSchema.destinationTable}`,
                    );
                } finally {
                    client.release();
                }
            }
        }

        // Soft delete pipeline
        await this.pipelineRepository.delete(pipelineId);
        this.logger.log(`Deleted pipeline ${pipelineId}`);
    }
}
