/**
 * PostgreSQL Pipeline Service
 * Orchestrates end-to-end data pipeline execution
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
        let destinationTableExists = false; // Default to false (will create new table)
        
        // Extract destination table from transformers if specified
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
                destinationTableExists = true; // User selected existing table
            }
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

        // Create pipeline run record first
        const run = await this.pipelineRepository.createRun({
            pipelineId: pipeline.id,
            orgId: pipeline.orgId,
            status: 'running',
            startedAt: new Date(),
            triggeredBy: pipeline.userId,
            triggerType: 'manual',
            runMetadata: { batchSize: 1000 },
        });

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
            // Step 1: Read from source
            this.logger.log(`[${run.id}] Step 1: Reading from source`);
            const sourceData = await this.readFromSource(
                pipeline,
                sourceSchema,
                pipeline.lastSyncValue,
                run.id,
            );

            // Step 2: Transform data
            this.logger.log(
                `[${run.id}] Step 2: Transforming ${sourceData.rows.length} rows`,
            );
            
            // Extract field mappings from transformers in pipeline configuration
            const pipelineConfig = pipeline.transformations as any;
            const collectors = pipelineConfig?.collectors || [];
            const transformers = collectors.flatMap((c: any) => c.transformers || []);
            
            this.logger.log(
                `[${run.id}] Found ${transformers.length} transformers, ${collectors.length} collectors`,
            );
            
            // Build column mappings from transformer fieldMappings
            let columnMappings: ColumnMapping[] = [];
            if (transformers.length > 0 && transformers[0]?.fieldMappings && Array.isArray(transformers[0].fieldMappings) && transformers[0].fieldMappings.length > 0) {
                // Use field mappings from transformers
                const fieldMappings = transformers[0].fieldMappings as Array<{ source: string; destination: string; isPrimaryKey?: boolean }>;
                const primaryKeyField = transformers[0].primaryKeyField || fieldMappings.find(fm => fm.isPrimaryKey)?.destination;
                
                this.logger.log(
                    `[${run.id}] Using ${fieldMappings.length} field mappings from transformer${primaryKeyField ? ` with primary key: ${primaryKeyField}` : ''}`,
                );
                // Ensure only ONE primary key is set
                let primaryKeySet = false;
                columnMappings = fieldMappings.map((fm) => {
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
                                `[${run.id}] Setting '${fm.destination}' as primary key`,
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
                
                // Ensure 'id' field exists in mappings - if not, add it
                const hasIdField = columnMappings.some(m => m.destinationColumn.toLowerCase() === 'id');
                if (!hasIdField) {
                    // Try to find an ID field in source data
                    const sourceIdField = sourceData.rows.length > 0 
                        ? Object.keys(sourceData.rows[0]).find(col => 
                            col.toLowerCase() === 'id' || col.toLowerCase().endsWith('_id')
                          )
                        : null;
                    
                    if (sourceIdField) {
                        columnMappings.unshift({
                            sourceColumn: sourceIdField,
                            destinationColumn: 'id',
                            dataType: 'UUID',
                            nullable: false,
                            isPrimaryKey: true,
                        });
                        this.logger.log(
                            `[${run.id}] Added missing 'id' field mapping from source column '${sourceIdField}'`,
                        );
                    } else {
                        // Add a generated UUID column
                        columnMappings.unshift({
                            sourceColumn: 'id', // Will be generated
                            destinationColumn: 'id',
                            dataType: 'UUID',
                            nullable: false,
                            isPrimaryKey: true,
                            defaultValue: 'gen_random_uuid()',
                        });
                        this.logger.log(
                            `[${run.id}] Added generated UUID 'id' field as primary key`,
                        );
                    }
                }
            } else if (destinationSchema.columnMappings && destinationSchema.columnMappings.length > 0) {
                // Fall back to destination schema column mappings
                this.logger.log(
                    `[${run.id}] Using ${destinationSchema.columnMappings.length} column mappings from destination schema`,
                );
                columnMappings = destinationSchema.columnMappings;
            } else {
                // Last resort: use all source columns as-is
                if (sourceData.rows.length > 0) {
                    const sourceColumns = Object.keys(sourceData.rows[0]);
                    this.logger.log(
                        `[${run.id}] No field mappings found, using all ${sourceColumns.length} source columns as-is`,
                    );
                    columnMappings = sourceColumns.map((col) => ({
                        sourceColumn: col,
                        destinationColumn: col,
                        dataType: 'TEXT',
                        nullable: true,
                    }));
                }
            }
            
            if (columnMappings.length === 0) {
                this.logger.error(
                    `[${run.id}] No column mappings found. Transformers: ${JSON.stringify(transformers)}, Destination schema mappings: ${JSON.stringify(destinationSchema.columnMappings)}`,
                );
                throw new BadRequestException(
                    'No column mappings found. Please configure field mappings in the transformer.',
                );
            }
            
            this.logger.log(
                `[${run.id}] Using ${columnMappings.length} column mappings: ${columnMappings.map(m => `${m.sourceColumn} -> ${m.destinationColumn}`).join(', ')}`,
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
                columnMappings,
                legacyTransformations,
            );
            
            // Ensure ID field exists in transformed data - add it if missing
            const hasIdInMappings = columnMappings.some(m => m.destinationColumn.toLowerCase() === 'id');
            if (hasIdInMappings && transformedData.length > 0) {
                const idMapping = columnMappings.find(m => m.destinationColumn.toLowerCase() === 'id');
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

            // Step 3: Write to destination
            this.logger.log(
                `[${run.id}] Step 3: Writing ${transformedData.length} rows to destination`,
            );
            const writeResult = await this.writeToDestination(
                transformedData,
                pipeline,
                destinationSchema,
                run.id,
                columnMappings, // Pass column mappings so table can be created if needed
            );

            // Step 4: Update pipeline state
            const durationSeconds = Math.floor((Date.now() - startTime) / 1000);

            // Calculate last sync value for incremental sync
            let lastSyncValue: string | null = pipeline.lastSyncValue || null;
            if (pipeline.incrementalColumn && sourceData.rows.length > 0) {
                // Find the max value of the incremental column
                const incrementalCol = pipeline.incrementalColumn;
                const maxValue = sourceData.rows.reduce((max, row) => {
                    const value = row[incrementalCol];
                    if (value !== null && value !== undefined) {
                        // Convert to string for comparison
                        const strValue = String(value);
                        return max === null || strValue > max ? strValue : max;
                    }
                    return max;
                }, null as string | null);
                if (maxValue !== null) {
                    lastSyncValue = maxValue;
                    this.logger.log(
                        `[${run.id}] Updated lastSyncValue for incremental column '${incrementalCol}': ${lastSyncValue}`,
                    );
                } else {
                    this.logger.warn(
                        `[${run.id}] No valid values found for incremental column '${incrementalCol}' in ${sourceData.rows.length} rows`,
                    );
                }
            } else if (pipeline.incrementalColumn && sourceData.rows.length === 0) {
                this.logger.log(
                    `[${run.id}] No new rows found for incremental sync. Keeping lastSyncValue: ${lastSyncValue || 'null'}`,
                );
            }

            // Determine if this is the first successful run
            const isFirstSuccessfulRun = (pipeline.totalRunsSuccessful ?? 0) === 0;

            await this.pipelineRepository.updateRun(run.id, {
                status: 'success',
                rowsRead: sourceData.rows.length,
                rowsWritten: writeResult.rowsWritten,
                rowsSkipped: writeResult.rowsSkipped,
                rowsFailed: writeResult.rowsFailed,
                completedAt: new Date(),
                durationSeconds,
            });

            // Update pipeline statistics and switch to incremental mode after first successful run
            const updateData: any = {
                lastRunAt: new Date(),
                lastRunStatus: 'success',
                migrationState: 'listing', // Switch to 'listing' state after successful migration
                totalRowsProcessed:
                    (pipeline.totalRowsProcessed ?? 0) + writeResult.rowsWritten,
                totalRunsSuccessful: (pipeline.totalRunsSuccessful ?? 0) + 1,
                lastError: null,
            };
            
            this.logger.log(
                `[${run.id}] Updating pipeline state to 'listing' after successful migration`,
            );

            // After first successful run, automatically switch to incremental mode
            if (isFirstSuccessfulRun && pipeline.syncMode === 'full') {
                // Try to detect incremental column (prefer 'id', 'created_at', 'updated_at')
                const possibleIncrementalColumns = ['id', 'created_at', 'updated_at', 'createdAt', 'updatedAt'];
                const sourceColumns = sourceData.rows.length > 0 ? Object.keys(sourceData.rows[0]) : [];
                const detectedIncrementalColumn = possibleIncrementalColumns.find(col => 
                    sourceColumns.includes(col)
                );

                if (detectedIncrementalColumn) {
                    updateData.syncMode = 'incremental';
                    updateData.incrementalColumn = detectedIncrementalColumn;
                    this.logger.log(
                        `[${run.id}] First successful run completed. Automatically switching to incremental mode with column: ${detectedIncrementalColumn}`,
                    );
                } else {
                    this.logger.warn(
                        `[${run.id}] First successful run completed but could not detect incremental column. Staying in full sync mode.`,
                    );
                }
            }

            // Update lastSyncValue if we have one
            if (lastSyncValue !== null) {
                updateData.lastSyncValue = lastSyncValue;
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

            await this.pipelineRepository.updateRun(run.id, {
                status: 'failed',
                errorMessage,
                errorStack: error instanceof Error ? error.stack : undefined,
                completedAt: new Date(),
                durationSeconds,
            });

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
                    // Incremental sync - use > to avoid re-processing the last value
                    // ON CONFLICT on primary key will prevent any duplicates
                    if (lastSyncValue) {
                        query = `
              SELECT * FROM "${schema}"."${table}"
              WHERE "${pipeline.incrementalColumn}" > $1
              ORDER BY "${pipeline.incrementalColumn}" ASC
            `;
                        params.push(lastSyncValue);
                        this.logger.log(
                            `[${runId}] Incremental sync: fetching records where ${pipeline.incrementalColumn} > ${lastSyncValue}`,
                        );
                    } else {
                        // First incremental run - get all records
                        query = `SELECT * FROM "${schema}"."${table}" ORDER BY "${pipeline.incrementalColumn}" ASC`;
                        this.logger.log(
                            `[${runId}] First incremental sync: fetching all records ordered by ${pipeline.incrementalColumn}`,
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

            // Apply column mappings
            mappings.forEach((mapping) => {
                let value = row[mapping.sourceColumn];

                // Apply transformations for this column
                const transformation = transformationsArray.find(
                    (t: Transformation) => t.sourceColumn === mapping.sourceColumn,
                );

                if (transformation) {
                    value = this.applyTransformation(value, transformation);
                }

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
     * Step 3: Write to destination
     */
    private async writeToDestination(
        transformedData: any[],
        pipeline: PostgresPipeline,
        destinationSchema: PipelineDestinationSchema,
        runId?: string,
        columnMappings?: ColumnMapping[],
    ): Promise<{
        rowsWritten: number;
        rowsSkipped: number;
        rowsFailed: number;
    }> {
        // Get or create connection pool
        let pool = this.connectionPool.getPool(destinationSchema.destinationConnectionId);
        if (!pool) {
            // Pool doesn't exist, create it
            const logPrefix = runId ? `[${runId}]` : '';
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

            // Check if table exists
            const tableExists = await this.destinationService.tableExists(
                client,
                destinationSchema.destinationSchema ?? 'public',
                destinationSchema.destinationTable,
            );

            // Use provided column mappings or fall back to destinationSchema.columnMappings
            // If neither exists, generate from transformed data
            let finalColumnMappings = columnMappings || destinationSchema.columnMappings || [];
            if (finalColumnMappings.length === 0 && transformedData.length > 0) {
                const logPrefix = runId ? `[${runId}]` : '';
                this.logger.log(
                    `${logPrefix} No column mappings available, generating from transformed data`,
                );
                // Generate column mappings from first row of transformed data
                const firstRow = transformedData[0];
                const columns = Object.keys(firstRow);
                finalColumnMappings = columns.map((col) => {
                    const isIdField = col.toLowerCase() === 'id' || col.toLowerCase().endsWith('_id');
                    return {
                        sourceColumn: col,
                        destinationColumn: col,
                        dataType: isIdField ? 'UUID' : 'TEXT', // Use UUID for ID fields
                        nullable: !isIdField, // ID fields should not be nullable
                        isPrimaryKey: col.toLowerCase() === 'id', // Mark 'id' as primary key
                    };
                });
                
                // Ensure 'id' field exists - if not, add it
                const hasIdField = finalColumnMappings.some(m => m.destinationColumn.toLowerCase() === 'id');
                if (!hasIdField) {
                    finalColumnMappings.unshift({
                        sourceColumn: 'id',
                        destinationColumn: 'id',
                        dataType: 'UUID',
                        nullable: false,
                        isPrimaryKey: true,
                        defaultValue: 'gen_random_uuid()',
                    });
                    this.logger.log(
                        `${logPrefix} Added generated UUID 'id' field as primary key`,
                    );
                }
                
                // Update destination schema with generated mappings
                await this.destinationSchemaRepository.update(destinationSchema.id, {
                    columnMappings: finalColumnMappings as any,
                });
            } else if (finalColumnMappings.length > 0) {
                // Ensure 'id' field exists in mappings
                const hasIdField = finalColumnMappings.some(m => m.destinationColumn.toLowerCase() === 'id');
                if (!hasIdField) {
                    finalColumnMappings.unshift({
                        sourceColumn: 'id',
                        destinationColumn: 'id',
                        dataType: 'UUID',
                        nullable: false,
                        isPrimaryKey: true,
                        defaultValue: 'gen_random_uuid()',
                    });
                    this.logger.log(
                        `${runId ? `[${runId}]` : ''} Added generated UUID 'id' field as primary key to existing mappings`,
                    );
                }
            }

            // Create table if not exists
            // Only create if destinationTableExists is false (user wants to create new table)
            // If destinationTableExists is true, assume table should exist and don't create
            // Also check if table name matches the pattern for auto-generated tables (pipeline_*)
            const isAutoGeneratedTable = destinationSchema.destinationTable.startsWith('pipeline_');
            const shouldCreateTable = !tableExists && (destinationSchema.destinationTableExists === false || isAutoGeneratedTable);
            
            if (shouldCreateTable) {
                const logPrefix = runId ? `[${runId}]` : '';
                this.logger.log(
                    `${logPrefix} Creating new destination table ${destinationSchema.destinationSchema ?? 'public'}.${destinationSchema.destinationTable} with ${finalColumnMappings.length} columns`,
                );
                await this.destinationService.createDestinationTable(
                    client,
                    destinationSchema.destinationSchema ?? 'public',
                    destinationSchema.destinationTable,
                    finalColumnMappings,
                );

                // Update destination schema to mark table as existing
                await this.destinationSchemaRepository.update(destinationSchema.id, {
                    destinationTableExists: true,
                    columnMappings: finalColumnMappings as any,
                });
            } else if (!tableExists && destinationSchema.destinationTableExists === true) {
                // Table doesn't exist but user selected an existing table
                const logPrefix = runId ? `[${runId}]` : '';
                throw new BadRequestException(
                    `Destination table "${destinationSchema.destinationSchema ?? 'public'}.${destinationSchema.destinationTable}" does not exist. Please create the table first or select a different table.`,
                );
            } else if (tableExists) {
                const logPrefix = runId ? `[${runId}]` : '';
                this.logger.log(
                    `${logPrefix} Using existing destination table ${destinationSchema.destinationSchema ?? 'public'}.${destinationSchema.destinationTable}`,
                );
            }

            // Validate schema if table exists
            if (tableExists && finalColumnMappings.length > 0) {
                const validation = await this.destinationService.validateSchema(
                    client,
                    destinationSchema.destinationSchema ?? 'public',
                    destinationSchema.destinationTable,
                    finalColumnMappings,
                );

                if (!validation.valid) {
                    // Add missing columns
                    if (validation.missingColumns && validation.missingColumns.length > 0) {
                        const missingMappings = finalColumnMappings.filter((m) =>
                            validation.missingColumns!.includes(m.destinationColumn),
                        );
                        await this.destinationService.addMissingColumns(
                            client,
                            destinationSchema.destinationSchema ?? 'public',
                            destinationSchema.destinationTable,
                            missingMappings,
                        );
                    }
                }
            }

            // Write data with duplicate prevention enabled
            const writeMode = (destinationSchema.writeMode as 'append' | 'upsert' | 'replace') || 'append';
            const result = await this.destinationService.writeData(
                client,
                destinationSchema.destinationSchema ?? 'public',
                destinationSchema.destinationTable,
                transformedData,
                writeMode,
                destinationSchema.upsertKey || undefined,
                true, // preventDuplicates = true
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
        await this.pipelineRepository.update(pipelineId, { status });
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
