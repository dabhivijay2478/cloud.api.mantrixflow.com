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
import { PostgresPipelineRepository } from './repositories/postgres-pipeline.repository';
import { PostgresConnectionRepository } from '../data-sources/postgres/repositories/postgres-connection.repository';
import {
    PipelineRunResult,
    PipelineError,
    ValidationResult,
    DryRunResult,
    ColumnMapping,
    Transformation,
} from '../data-sources/postgres/postgres.types';
import type { PostgresPipeline } from '@db/schema';

@Injectable()
export class PostgresPipelineService {
    private readonly logger = new Logger(PostgresPipelineService.name);

    constructor(
        private readonly pipelineRepository: PostgresPipelineRepository,
        private readonly connectionRepository: PostgresConnectionRepository,
        private readonly connectionPool: PostgresConnectionPoolService,
        private readonly queryExecutor: PostgresQueryExecutorService,
        private readonly destinationService: PostgresDestinationService,
        private readonly schemaMapper: PostgresSchemaMapperService,
    ) { }

    /**
     * Execute full pipeline: source → transform → destination
     */
    async executePipeline(pipelineId: string): Promise<PipelineRunResult> {
        this.logger.log(`Executing pipeline ${pipelineId}`);

        const pipeline = await this.pipelineRepository.findById(pipelineId);
        if (!pipeline) {
            throw new NotFoundException(`Pipeline ${pipelineId} not found`);
        }

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
                pipeline.lastSyncValue,
            );

            // Step 2: Transform data
            this.logger.log(
                `[${run.id}] Step 2: Transforming ${sourceData.rows.length} rows`,
            );
            const transformedData = await this.transformData(
                sourceData.rows,
                pipeline.columnMappings || [],
                pipeline.transformations || [],
            );

            // Step 3: Write to destination
            this.logger.log(
                `[${run.id}] Step 3: Writing ${transformedData.length} rows to destination`,
            );
            const writeResult = await this.writeToDestination(
                transformedData,
                pipeline,
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
        lastSyncValue?: string | null,
    ): Promise<{ rows: any[]; totalRows: number }> {
        if (pipeline.sourceType !== 'postgres') {
            throw new BadRequestException(
                `Source type ${pipeline.sourceType} not yet supported`,
            );
        }

        if (!pipeline.sourceConnectionId) {
            throw new BadRequestException('Source connection ID is required');
        }

        const pool = this.connectionPool.getPool(pipeline.sourceConnectionId);
        if (!pool) {
            throw new BadRequestException(
                `Source connection pool not found for ${pipeline.sourceConnectionId}`,
            );
        }

        const client = await pool.connect();

        try {
            let query: string;
            const params: any[] = [];

            if (pipeline.sourceQuery) {
                // Use custom query
                query = pipeline.sourceQuery;
            } else if (pipeline.sourceTable) {
                // Build query from table
                const schema = pipeline.sourceSchema || 'public';
                const table = pipeline.sourceTable;

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
    ): Promise<{
        rowsWritten: number;
        rowsSkipped: number;
        rowsFailed: number;
    }> {
        const pool = this.connectionPool.getPool(pipeline.destinationConnectionId);
        if (!pool) {
            throw new BadRequestException(
                `Destination connection pool not found for ${pipeline.destinationConnectionId}`,
            );
        }

        const client = await pool.connect();

        try {
            await client.query('BEGIN');

            // Check if table exists
            const tableExists = await this.destinationService.tableExists(
                client,
                pipeline.destinationSchema ?? 'public',
                pipeline.destinationTable,
            );

            // Create table if not exists
            if (!tableExists && pipeline.columnMappings) {
                await this.destinationService.createDestinationTable(
                    client,
                    pipeline.destinationSchema ?? 'public',
                    pipeline.destinationTable,
                    pipeline.columnMappings,
                );

                await this.pipelineRepository.update(pipeline.id, {
                    destinationTableExists: true,
                });
            }

            // Validate schema if table exists
            if (tableExists && pipeline.columnMappings) {
                const validation = await this.destinationService.validateSchema(
                    client,
                    pipeline.destinationSchema ?? 'public',
                    pipeline.destinationTable,
                    pipeline.columnMappings,
                );

                if (!validation.valid) {
                    // Add missing columns
                    if (validation.missingColumns.length > 0) {
                        const missingMappings = pipeline.columnMappings.filter((m) =>
                            validation.missingColumns.includes(m.destinationColumn),
                        );
                        await this.destinationService.addMissingColumns(
                            client,
                            pipeline.destinationSchema ?? 'public',
                            pipeline.destinationTable,
                            missingMappings,
                        );
                    }
                }
            }

            // Write data
            const result = await this.destinationService.writeData(
                client,
                pipeline.destinationSchema ?? 'public',
                pipeline.destinationTable,
                transformedData,
                pipeline.writeMode ?? 'append',
                pipeline.upsertKey || undefined,
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
        const pipeline = await this.pipelineRepository.findById(pipelineId);
        if (!pipeline) {
            throw new NotFoundException(`Pipeline ${pipelineId} not found`);
        }

        const errors: string[] = [];
        const warnings: string[] = [];

        // Validate source configuration
        if (pipeline.sourceType === 'postgres') {
            if (!pipeline.sourceConnectionId) {
                errors.push('Source connection ID is required for postgres source');
            }
            if (!pipeline.sourceTable && !pipeline.sourceQuery) {
                errors.push('Either source table or source query must be specified');
            }
        }

        // Validate destination configuration
        if (!pipeline.destinationConnectionId) {
            errors.push('Destination connection ID is required');
        }
        if (!pipeline.destinationTable) {
            errors.push('Destination table is required');
        }

        // Validate write mode
        if (pipeline.writeMode === 'upsert' && (!pipeline.upsertKey || pipeline.upsertKey.length === 0)) {
            errors.push('Upsert key is required for upsert write mode');
        }

        // Validate sync mode
        if (pipeline.syncMode === 'incremental' && !pipeline.incrementalColumn) {
            errors.push('Incremental column is required for incremental sync mode');
        }

        // Validate column mappings
        if (!pipeline.columnMappings || pipeline.columnMappings.length === 0) {
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

        const pipeline = await this.pipelineRepository.findById(pipelineId);
        if (!pipeline) {
            throw new NotFoundException(`Pipeline ${pipelineId} not found`);
        }

        const startTime = Date.now();

        // Read sample data from source
        const sourceData = await this.readFromSource(pipeline, null);
        const sampleRows = sourceData.rows.slice(0, 10);

        // Transform sample data
        const transformedSample = await this.transformData(
            sampleRows,
            pipeline.columnMappings || [],
            pipeline.transformations || [],
        );

        const estimatedDuration = Math.ceil((Date.now() - startTime) / 1000);

        return {
            sourceRowCount: sourceData.totalRows,
            sampleRows: transformedSample,
            destinationSchemaPreview: pipeline.columnMappings || [],
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
        const pipeline = await this.pipelineRepository.findById(pipelineId);
        if (!pipeline) {
            throw new NotFoundException(`Pipeline ${pipelineId} not found`);
        }

        // Optionally drop destination table
        if (dropTable && pipeline.destinationTableExists) {
            const pool = this.connectionPool.getPool(
                pipeline.destinationConnectionId,
            );
            if (pool) {
                const client = await pool.connect();
                try {
                    await client.query(
                        `DROP TABLE IF EXISTS "${pipeline.destinationSchema}"."${pipeline.destinationTable}"`,
                    );
                    this.logger.log(
                        `Dropped table ${pipeline.destinationSchema}.${pipeline.destinationTable}`,
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
