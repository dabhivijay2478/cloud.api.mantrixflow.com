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
    }): Promise<PostgresPipeline> {
        // 1. Create source schema
        const sourceSchema = await this.sourceSchemaRepository.create({
            orgId: data.orgId,
            userId: data.userId,
            sourceType: data.sourceType,
            sourceConnectionId: data.sourceConnectionId,
            sourceConfig: data.sourceConfig,
            sourceSchema: data.sourceSchema,
            sourceTable: data.sourceTable,
            sourceQuery: data.sourceQuery,
            name: `Source for ${data.name}`,
        });

        // 2. Create destination schema
        const destinationSchema = await this.destinationSchemaRepository.create({
            orgId: data.orgId,
            userId: data.userId,
            destinationConnectionId: data.destinationConnectionId,
            destinationSchema: data.destinationSchema || 'public',
            destinationTable: data.destinationTable,
            destinationTableExists: false,
            columnMappings: data.columnMappings,
            writeMode: (data.writeMode as 'append' | 'upsert' | 'replace') || 'append',
            upsertKey: data.upsertKey,
            name: `Destination for ${data.name}`,
        });

        // 3. Create pipeline with schema references
        const pipeline = await this.pipelineRepository.create({
            orgId: data.orgId,
            userId: data.userId,
            name: data.name,
            description: data.description,
            sourceSchemaId: sourceSchema.id,
            destinationSchemaId: destinationSchema.id,
            transformations: data.transformations,
            syncMode: data.syncMode || 'full',
            incrementalColumn: data.incrementalColumn,
            syncFrequency: data.syncFrequency || 'manual',
        });

        return pipeline;
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

        // Create pipeline run record
        const run = await this.pipelineRepository.createRun({
            pipelineId: pipeline.id,
            orgId: pipeline.orgId,
            status: 'running',
            startedAt: new Date(),
            triggeredBy: pipeline.userId,
            triggerType: 'manual',
            runMetadata: { batchSize: 1000 },
        });

        const startTime = Date.now();
        const errors: PipelineError[] = [];

        try {
            // Step 1: Read from source
            this.logger.log(`[${run.id}] Step 1: Reading from source`);
            const sourceData = await this.readFromSource(
                pipeline,
                sourceSchema,
                pipeline.lastSyncValue,
            );

            // Step 2: Transform data
            this.logger.log(
                `[${run.id}] Step 2: Transforming ${sourceData.rows.length} rows`,
            );
            const transformedData = await this.transformData(
                sourceData.rows,
                destinationSchema.columnMappings || [],
                pipeline.transformations || [],
            );

            // Step 3: Write to destination
            this.logger.log(
                `[${run.id}] Step 3: Writing ${transformedData.length} rows to destination`,
            );
            const writeResult = await this.writeToDestination(
                transformedData,
                pipeline,
                destinationSchema,
            );

            // Step 4: Update pipeline state
            const durationSeconds = Math.floor((Date.now() - startTime) / 1000);

            await this.pipelineRepository.updateRun(run.id, {
                status: 'success',
                rowsRead: sourceData.rows.length,
                rowsWritten: writeResult.rowsWritten,
                rowsSkipped: writeResult.rowsSkipped,
                rowsFailed: writeResult.rowsFailed,
                completedAt: new Date(),
                durationSeconds,
            });

            // Update pipeline statistics
            await this.pipelineRepository.update(pipeline.id, {
                lastRunAt: new Date(),
                lastRunStatus: 'success',
                totalRowsProcessed:
                    (pipeline.totalRowsProcessed ?? 0) + writeResult.rowsWritten,
                totalRunsSuccessful: (pipeline.totalRunsSuccessful ?? 0) + 1,
                lastError: null,
            });

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

            await this.pipelineRepository.update(pipeline.id, {
                lastRunAt: new Date(),
                lastRunStatus: 'failed',
                lastError: errorMessage,
                totalRunsFailed: (pipeline.totalRunsFailed ?? 0) + 1,
            });

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
    ): Promise<{ rows: any[]; totalRows: number }> {
        if (sourceSchema.sourceType !== 'postgres') {
            throw new BadRequestException(
                `Source type ${sourceSchema.sourceType} not yet supported`,
            );
        }

        if (!sourceSchema.sourceConnectionId) {
            throw new BadRequestException('Source connection ID is required');
        }

        const pool = this.connectionPool.getPool(sourceSchema.sourceConnectionId);
        if (!pool) {
            throw new BadRequestException(
                `Source connection pool not found for ${sourceSchema.sourceConnectionId}`,
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
                    // Incremental sync
                    if (lastSyncValue) {
                        query = `
              SELECT * FROM "${schema}"."${table}"
              WHERE "${pipeline.incrementalColumn}" > $1
              ORDER BY "${pipeline.incrementalColumn}" ASC
            `;
                        params.push(lastSyncValue);
                    } else {
                        query = `SELECT * FROM "${schema}"."${table}" ORDER BY "${pipeline.incrementalColumn}" ASC`;
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
        transformations: Transformation[],
    ): Promise<any[]> {
        if (sourceData.length === 0) return [];

        return sourceData.map((row) => {
            const transformedRow: any = {};

            // Apply column mappings
            mappings.forEach((mapping) => {
                let value = row[mapping.sourceColumn];

                // Apply transformations for this column
                const transformation = transformations.find(
                    (t) => t.sourceColumn === mapping.sourceColumn,
                );

                if (transformation) {
                    value = this.applyTransformation(value, transformation);
                }

                transformedRow[mapping.destinationColumn] = value;
            });

            return transformedRow;
        });
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
    ): Promise<{
        rowsWritten: number;
        rowsSkipped: number;
        rowsFailed: number;
    }> {
        const pool = this.connectionPool.getPool(destinationSchema.destinationConnectionId);
        if (!pool) {
            throw new BadRequestException(
                `Destination connection pool not found for ${destinationSchema.destinationConnectionId}`,
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

            // Create table if not exists
            if (!tableExists && destinationSchema.columnMappings) {
                await this.destinationService.createDestinationTable(
                    client,
                    destinationSchema.destinationSchema ?? 'public',
                    destinationSchema.destinationTable,
                    destinationSchema.columnMappings,
                );

                // Update destination schema
                // Note: We could update the destinationSchema record here if needed
            }

            // Validate schema if table exists
            if (tableExists && destinationSchema.columnMappings) {
                const validation = await this.destinationService.validateSchema(
                    client,
                    destinationSchema.destinationSchema ?? 'public',
                    destinationSchema.destinationTable,
                    destinationSchema.columnMappings,
                );

                if (!validation.valid) {
                    // Add missing columns
                    if (validation.missingColumns && validation.missingColumns.length > 0) {
                        const missingMappings = destinationSchema.columnMappings.filter((m) =>
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

            // Write data
            const writeMode = (destinationSchema.writeMode as 'append' | 'upsert' | 'replace') || 'append';
            const result = await this.destinationService.writeData(
                client,
                destinationSchema.destinationSchema ?? 'public',
                destinationSchema.destinationTable,
                transformedData,
                writeMode,
                destinationSchema.upsertKey || undefined,
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
