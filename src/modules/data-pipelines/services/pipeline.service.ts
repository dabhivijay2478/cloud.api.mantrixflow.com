/**
 * Pipeline Service
 * Main orchestration service for managing data pipelines
 * Works with all data source types using generic collector, transformer, and emitter
 *
 * Architecture: Collector → Emitter (with transformation) → Transformer (post-processing)
 */

import {
  BadRequestException,
  ForbiddenException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import type {
  PipelineDestinationSchema,
  PipelineSourceSchema,
  Pipeline,
  PipelineRun,
} from '../../../database/schemas';
import { ActivityLogService } from '../../activity-logs/activity-log.service';
import {
  PIPELINE_ACTIONS,
  PIPELINE_RUN_ACTIONS,
} from '../../activity-logs/constants/activity-log-types';
import { DataSourceRepository } from '../../data-sources/repositories/data-source.repository';
import { OrganizationRoleService } from '../../organizations/services/organization-role.service';
import { CollectorService } from './collector.service';
import { EmitterService } from './emitter.service';
import { TransformerService } from './transformer.service';
import { PipelineLifecycleService } from './pipeline-lifecycle.service';
import type {
  ColumnMapping,
  DryRunResult,
  ValidationResult,
  BatchOptions,
  PipelineError,
} from '../types/common.types';
import { PipelineStatus, PipelineCheckpoint } from '../types/pipeline-lifecycle.types';
import { PipelineRepository } from '../repositories/pipeline.repository';
import { PipelineSourceSchemaRepository } from '../repositories/pipeline-source-schema.repository';
import { PipelineDestinationSchemaRepository } from '../repositories/pipeline-destination-schema.repository';
import type { CreatePipelineDto, UpdatePipelineDto } from '../dto';

/**
 * Internal DTO for creating pipelines (with organizationId and userId)
 */
export interface CreatePipelineInput extends CreatePipelineDto {
  organizationId: string;
  userId: string;
}

/**
 * Default batch size for processing
 */
const DEFAULT_BATCH_SIZE = 1000;
const MAX_BATCH_SIZE = 10000;
const RETRY_ATTEMPTS = 3;
const RETRY_DELAY_MS = 2000;

@Injectable()
export class PipelineService {
  private readonly logger = new Logger(PipelineService.name);

  constructor(
    private readonly pipelineRepository: PipelineRepository,
    private readonly sourceSchemaRepository: PipelineSourceSchemaRepository,
    private readonly destinationSchemaRepository: PipelineDestinationSchemaRepository,
    private readonly dataSourceRepository: DataSourceRepository,
    private readonly collectorService: CollectorService,
    private readonly transformerService: TransformerService,
    private readonly emitterService: EmitterService,
    private readonly activityLogService: ActivityLogService,
    private readonly roleService: OrganizationRoleService,
    private readonly lifecycleService: PipelineLifecycleService,
  ) {}

  /**
   * Create a new pipeline
   * Validates source and destination schemas before creation
   */
  async createPipeline(dto: CreatePipelineInput): Promise<Pipeline> {
    const { organizationId, userId, sourceSchemaId, destinationSchemaId } = dto;

    // AUTHORIZATION: Check if user can manage pipelines
    await this.checkPipelineManagePermission(userId, organizationId);

    // Validate source schema exists and belongs to organization
    const sourceSchema = await this.sourceSchemaRepository.findById(sourceSchemaId);
    if (!sourceSchema) {
      throw new NotFoundException(`Source schema ${sourceSchemaId} not found`);
    }
    if (sourceSchema.organizationId !== organizationId) {
      throw new ForbiddenException('Source schema does not belong to this organization');
    }

    // Validate destination schema exists and belongs to organization
    const destinationSchema = await this.destinationSchemaRepository.findById(destinationSchemaId);
    if (!destinationSchema) {
      throw new NotFoundException(`Destination schema ${destinationSchemaId} not found`);
    }
    if (destinationSchema.organizationId !== organizationId) {
      throw new ForbiddenException('Destination schema does not belong to this organization');
    }

    // Validate data sources exist and are accessible
    if (sourceSchema.dataSourceId) {
      const sourceDataSource = await this.dataSourceRepository.findById(sourceSchema.dataSourceId);
      if (!sourceDataSource || sourceDataSource.organizationId !== organizationId) {
        throw new BadRequestException('Source data source not found or not accessible');
      }
    }

    const destDataSource = await this.dataSourceRepository.findById(destinationSchema.dataSourceId);
    if (!destDataSource || destDataSource.organizationId !== organizationId) {
      throw new BadRequestException('Destination data source not found or not accessible');
    }

    // Check for duplicate name
    const existing = await this.pipelineRepository.findByNameAndOrganizationId(
      dto.name,
      organizationId,
    );
    if (existing && !existing.deletedAt) {
      throw new BadRequestException(`Pipeline with name "${dto.name}" already exists`);
    }

    // Create pipeline
    const pipeline = await this.pipelineRepository.create({
      organizationId,
      createdBy: userId,
      name: dto.name,
      description: dto.description,
      sourceSchemaId,
      destinationSchemaId,
      transformations: dto.transformations || null,
      syncMode: dto.syncMode || 'full',
      incrementalColumn: dto.incrementalColumn || null,
      syncFrequency: dto.syncFrequency || 'manual',
      status: PipelineStatus.IDLE,
    });

    // Log activity
    await this.activityLogService.logPipelineAction(
      organizationId,
      userId,
      PIPELINE_ACTIONS.CREATED,
      pipeline.id,
      pipeline.name,
      {
        sourceSchemaId,
        destinationSchemaId,
        syncMode: pipeline.syncMode,
        syncFrequency: pipeline.syncFrequency,
      },
    );

    this.logger.log(`Pipeline created: ${pipeline.id} - ${pipeline.name}`);
    return pipeline;
  }

  /**
   * Get pipelines by organization
   */
  async findByOrganization(organizationId: string, userId?: string): Promise<Pipeline[]> {
    if (userId) {
      await this.checkPipelineViewPermission(userId, organizationId);
    }

    const pipelines = await this.pipelineRepository.findByOrganization(organizationId);
    return pipelines;
  }

  /**
   * Get pipeline by ID
   */
  async findById(id: string, organizationId?: string): Promise<Pipeline | null> {
    return await this.pipelineRepository.findById(id, organizationId);
  }

  /**
   * Get pipeline with schemas loaded
   */
  async findByIdWithSchemas(
    id: string,
    organizationId?: string,
  ): Promise<{
    pipeline: Pipeline;
    sourceSchema: PipelineSourceSchema;
    destinationSchema: PipelineDestinationSchema;
  } | null> {
    return await this.pipelineRepository.findByIdWithSchemas(id, organizationId);
  }

  /**
   * Update pipeline
   */
  async updatePipeline(id: string, updates: UpdatePipelineDto, userId: string): Promise<Pipeline> {
    const pipeline = await this.pipelineRepository.findById(id);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${id} not found`);
    }

    // AUTHORIZATION
    await this.checkPipelineManagePermission(userId, pipeline.organizationId);

    // Check for duplicate name if name is being updated
    if (updates.name && updates.name !== pipeline.name) {
      const existing = await this.pipelineRepository.findByNameAndOrganizationId(
        updates.name,
        pipeline.organizationId,
      );
      if (existing && existing.id !== id && !existing.deletedAt) {
        throw new BadRequestException(`Pipeline with name "${updates.name}" already exists`);
      }
    }

    const updated = await this.pipelineRepository.update(id, updates);

    // Log activity
    await this.activityLogService.logPipelineAction(
      pipeline.organizationId,
      userId,
      PIPELINE_ACTIONS.UPDATED,
      id,
      updated.name,
      { changes: updates },
    );

    this.logger.log(`Pipeline updated: ${id}`);
    return updated;
  }

  /**
   * Delete pipeline (soft delete)
   */
  async deletePipeline(id: string, userId: string): Promise<void> {
    const pipeline = await this.pipelineRepository.findById(id);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${id} not found`);
    }

    // AUTHORIZATION
    await this.checkPipelineManagePermission(userId, pipeline.organizationId);

    await this.pipelineRepository.delete(id);

    // Log activity
    await this.activityLogService.logPipelineAction(
      pipeline.organizationId,
      userId,
      PIPELINE_ACTIONS.UPDATED,
      id,
      pipeline.name,
      { action: 'deleted' },
    );

    this.logger.log(`Pipeline deleted: ${id}`);
  }

  /**
   * Run pipeline with batching and retry support
   */
  async runPipeline(
    pipelineId: string,
    userId: string,
    triggerType: 'manual' | 'scheduled' | 'api' = 'manual',
    options?: BatchOptions,
  ): Promise<PipelineRun> {
    const pipelineWithSchemas = await this.pipelineRepository.findByIdWithSchemas(pipelineId);
    if (!pipelineWithSchemas) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    const { pipeline, sourceSchema, destinationSchema } = pipelineWithSchemas;

    // AUTHORIZATION
    await this.checkPipelineManagePermission(userId, pipeline.organizationId);

    // Check pipeline status
    if (pipeline.status === 'paused') {
      throw new BadRequestException('Pipeline is paused. Resume it before running.');
    }

    // Create run record
    const run = await this.pipelineRepository.createRun({
      pipelineId,
      organizationId: pipeline.organizationId,
      status: 'pending',
      jobState: 'pending',
      triggerType,
      triggeredBy: userId,
      startedAt: new Date(),
    });

    // Update parent pipeline status to running immediately for UI feedback
    await this.pipelineRepository.update(pipelineId, {
      lastRunStatus: 'running',
      lastRunAt: new Date(),
    });

    this.logger.log(`Updated pipeline ${pipelineId} status to running`);

    // Log activity
    await this.activityLogService.logPipelineRunAction(
      pipeline.organizationId,
      userId,
      PIPELINE_RUN_ACTIONS.STARTED,
      run.id,
      pipelineId,
      pipeline.name,
      { triggerType },
    );

    // Execute pipeline asynchronously
    this.executePipelineAsync(
      run.id,
      pipeline,
      sourceSchema,
      destinationSchema,
      userId,
      options,
    ).catch((error) => {
      this.logger.error(
        `Pipeline execution failed: ${error instanceof Error ? error.message : String(error)}`,
        error instanceof Error ? error.stack : undefined,
      );
    });

    return run;
  }

  /**
   * Execute pipeline asynchronously with batching and retries
   */
  private async executePipelineAsync(
    runId: string,
    pipeline: Pipeline,
    sourceSchema: PipelineSourceSchema,
    destinationSchema: PipelineDestinationSchema,
    userId: string,
    options?: BatchOptions,
  ): Promise<void> {
    const startTime = Date.now();

    // Determine sync type
    const checkpoint = await this.lifecycleService.getCheckpoint(pipeline.id);
    const isFullSync = !checkpoint?.lastSyncValue && pipeline.syncMode !== 'incremental';
    const syncType = isFullSync ? 'full' : 'incremental';

    // Log startup
    console.log(`\n${'='.repeat(60)}`);
    console.log(`🚀 STARTING PIPELINE: ${pipeline.name}`);
    console.log(`${'='.repeat(60)}`);
    console.log(`   Run ID:     ${runId}`);
    console.log(`   Sync Type:  ${syncType.toUpperCase()}`);
    console.log(`   Batch Size: ${options?.batchSize || DEFAULT_BATCH_SIZE}`);
    console.log(
      `   Source:     ${sourceSchema.sourceType} - ${sourceSchema.sourceTable || 'query'}`,
    );
    console.log(`${'='.repeat(60)}\n`);

    const batchSize = Math.min(options?.batchSize || DEFAULT_BATCH_SIZE, MAX_BATCH_SIZE);
    const retryAttempts = options?.retryAttempts || RETRY_ATTEMPTS;

    let totalRowsRead = 0;
    let totalRowsWritten = 0;
    let totalRowsSkipped = 0;
    let totalRowsFailed = 0;
    let batchCount = 0;
    let estimatedTotalRows: number | undefined;
    const allErrors: PipelineError[] = [];

    try {
      // Set pipeline to RUNNING status
      await this.lifecycleService.startRunning(pipeline.id, userId, isFullSync);

      // Update run status
      await this.pipelineRepository.updateRun(runId, {
        status: 'running',
        jobState: 'running',
      });

      // Validate data sources
      if (!sourceSchema.dataSourceId || !destinationSchema.dataSourceId) {
        throw new BadRequestException('Source and destination must have data source IDs');
      }

      // Get column mappings
      const columnMappings = (destinationSchema.columnMappings as ColumnMapping[]) || [];
      const transformations = (pipeline.transformations as any[]) || [];

      // Collect data with batching
      let hasMore = true;
      let offset = checkpoint?.offset || 0;
      let cursor: string | undefined = checkpoint?.cursor;

      // Log checkpoint restoration if applicable
      if (checkpoint?.rowsProcessed) {
        console.log(
          `📂 Resuming from checkpoint: ${checkpoint.rowsProcessed.toLocaleString()} rows already processed`,
        );
        await this.activityLogService.logPipelineAction(
          pipeline.organizationId,
          userId,
          PIPELINE_ACTIONS.CHECKPOINT_SAVED,
          pipeline.id,
          pipeline.name,
          { action: 'restored', checkpoint },
        );
      }

      while (hasMore) {
        batchCount++;
        const batchStartTime = Date.now();

        // STEP 1: Collect data from source (with retry)
        let sourceData: { rows: any[]; totalRows?: number; nextCursor?: string; hasMore?: boolean };

        console.log(`\n📦 Batch ${batchCount}: Collecting data...`);

        for (let attempt = 0; attempt < retryAttempts; attempt++) {
          try {
            sourceData = await this.collectorService.collect({
              sourceSchema,
              organizationId: pipeline.organizationId,
              userId,
              limit: batchSize,
              offset,
              cursor,
              incrementalColumn: pipeline.incrementalColumn || undefined,
              lastSyncValue: isFullSync
                ? undefined
                : checkpoint?.lastSyncValue?.toString() || pipeline.lastSyncValue || undefined,
            });
            break;
          } catch (error) {
            if (attempt === retryAttempts - 1) {
              throw error;
            }
            console.log(
              `   ⚠️  Collect attempt ${attempt + 1} failed, retrying in ${RETRY_DELAY_MS * (attempt + 1)}ms...`,
            );
            this.logger.warn(`Collect attempt ${attempt + 1} failed, retrying...`);
            await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS * (attempt + 1)));
          }
        }

        if (!sourceData! || sourceData!.rows.length === 0) {
          console.log(`   ✓ No more data to process`);
          break;
        }

        // Update estimated total if available
        if (sourceData.totalRows && !estimatedTotalRows) {
          estimatedTotalRows = sourceData.totalRows;
          console.log(`   📊 Total rows in source: ${estimatedTotalRows.toLocaleString()}`);
        }

        totalRowsRead += sourceData.rows.length;

        // Calculate progress
        const percentage = estimatedTotalRows
          ? Math.min(100, Math.round((totalRowsRead / estimatedTotalRows) * 100))
          : undefined;

        console.log(
          `   📥 Collected ${sourceData.rows.length.toLocaleString()} rows (Total: ${totalRowsRead.toLocaleString()}${estimatedTotalRows ? `/${estimatedTotalRows.toLocaleString()}` : ''}${percentage ? ` - ${percentage}%` : ''})`,
        );

        const primaryKeys = columnMappings
          .filter((m) => m.isPrimaryKey)
          .map((m) => m.destinationColumn);

        // Determine write mode based on usage intent
        let effectiveWriteMode =
          (destinationSchema.writeMode as 'append' | 'upsert' | 'replace') || 'append';
        let effectiveUpsertKey = (destinationSchema.upsertKey as string[]) || undefined;

        // Force UPSERT if incremental and we have keys (prevents duplicates)
        if (pipeline.syncMode === 'incremental' && primaryKeys.length > 0) {
          effectiveWriteMode = 'upsert';
          effectiveUpsertKey = primaryKeys;
        }
        // Force REPLACE if full sync (fresh start) - only on first batch
        else if (pipeline.syncMode === 'full' && batchCount === 1) {
          effectiveWriteMode = 'replace';
        }

        console.log(
          `   📤 Writing ${sourceData.rows.length.toLocaleString()} rows (mode: ${effectiveWriteMode})...`,
        );

        // STEP 2: Emit data to destination (with internal transformation)
        for (let attempt = 0; attempt < retryAttempts; attempt++) {
          try {
            const writeResult = await this.emitterService.emit({
              destinationSchema,
              organizationId: pipeline.organizationId,
              userId,
              rows: sourceData.rows,
              writeMode: effectiveWriteMode,
              upsertKey: effectiveUpsertKey,
              columnMappings,
              transformations,
            });

            totalRowsWritten += writeResult.rowsWritten;
            totalRowsSkipped += writeResult.rowsSkipped;
            totalRowsFailed += writeResult.rowsFailed;

            const batchDuration = ((Date.now() - batchStartTime) / 1000).toFixed(1);
            const rate = (
              writeResult.rowsWritten / Math.max(parseFloat(batchDuration), 0.1)
            ).toFixed(0);

            console.log(
              `   ✅ Written: ${writeResult.rowsWritten.toLocaleString()} | Skipped: ${writeResult.rowsSkipped} | Failed: ${writeResult.rowsFailed} | ${batchDuration}s (${rate} rows/sec)`,
            );
            console.log(`   📈 PROGRESS: ${totalRowsWritten.toLocaleString()} rows written so far`);

            if (writeResult.errors && writeResult.errors.length > 0) {
              console.log(`   ⚠️  ${writeResult.errors.length} errors in batch`);
              allErrors.push(...writeResult.errors);
            }
            break;
          } catch (error) {
            if (attempt === retryAttempts - 1) {
              throw error;
            }
            console.log(
              `   ⚠️  Emit attempt ${attempt + 1} failed, retrying in ${RETRY_DELAY_MS * (attempt + 1)}ms...`,
            );
            this.logger.warn(`Emit attempt ${attempt + 1} failed, retrying...`);
            await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS * (attempt + 1)));
          }
        }

        // Update pagination
        hasMore = sourceData.hasMore || sourceData.rows.length === batchSize;
        offset += batchSize;
        cursor = sourceData.nextCursor;

        // Save checkpoint after each batch for resumability
        const currentCheckpoint: PipelineCheckpoint = {
          lastSyncAt: new Date().toISOString(),
          offset,
          cursor,
          rowsProcessed: totalRowsWritten,
          totalRows: estimatedTotalRows,
          currentBatch: batchCount,
        };

        await this.lifecycleService.saveCheckpoint(pipeline.id, currentCheckpoint, userId);

        // Update progress in database
        await this.pipelineRepository.updateRun(runId, {
          rowsRead: totalRowsRead,
          rowsWritten: totalRowsWritten,
          rowsSkipped: totalRowsSkipped,
          rowsFailed: totalRowsFailed,
        });

        // Log to activity every 5 batches or 5000 rows
        if (batchCount % 5 === 0 || totalRowsWritten % 5000 < batchSize) {
          await this.activityLogService.logPipelineRunAction(
            pipeline.organizationId,
            userId,
            PIPELINE_ACTIONS.BATCH_COMPLETED,
            runId,
            pipeline.id,
            pipeline.name,
            {
              batchNumber: batchCount,
              rowsProcessed: totalRowsWritten,
              totalRows: estimatedTotalRows,
              percentage,
            },
          );
        }
      }

      // Update run with final results
      const durationSeconds = Math.floor((Date.now() - startTime) / 1000);
      await this.pipelineRepository.updateRun(runId, {
        status: totalRowsFailed > 0 && totalRowsWritten === 0 ? 'failed' : 'success',
        jobState: 'completed',
        rowsRead: totalRowsRead,
        rowsWritten: totalRowsWritten,
        rowsSkipped: totalRowsSkipped,
        rowsFailed: totalRowsFailed,
        completedAt: new Date(),
        durationSeconds,
      });

      // Update pipeline statistics
      await this.pipelineRepository.update(pipeline.id, {
        lastRunAt: new Date(),
        lastRunStatus: totalRowsFailed > 0 && totalRowsWritten === 0 ? 'failed' : 'success',
        totalRowsProcessed: (pipeline.totalRowsProcessed || 0) + totalRowsWritten,
        totalRunsSuccessful: (pipeline.totalRunsSuccessful || 0) + (totalRowsFailed === 0 ? 1 : 0),
        totalRunsFailed:
          (pipeline.totalRunsFailed || 0) + (totalRowsFailed > 0 && totalRowsWritten === 0 ? 1 : 0),
      });

      // Log activity
      await this.activityLogService.logPipelineRunAction(
        pipeline.organizationId,
        userId,
        PIPELINE_RUN_ACTIONS.COMPLETED,
        runId,
        pipeline.id,
        pipeline.name,
        {
          rowsRead: totalRowsRead,
          rowsWritten: totalRowsWritten,
          rowsSkipped: totalRowsSkipped,
          rowsFailed: totalRowsFailed,
          durationSeconds,
        },
      );

      this.logger.log(
        `Pipeline ${pipeline.id} run ${runId} completed: ${totalRowsWritten} rows written in ${durationSeconds}s`,
      );
    } catch (error) {
      const durationSeconds = Math.floor((Date.now() - startTime) / 1000);
      const errorMessage = error instanceof Error ? error.message : String(error);

      await this.pipelineRepository.updateRun(runId, {
        status: 'failed',
        jobState: 'failed',
        completedAt: new Date(),
        durationSeconds,
        errorMessage,
        errorStack: error instanceof Error ? error.stack : undefined,
      });

      await this.pipelineRepository.update(pipeline.id, {
        lastRunAt: new Date(),
        lastRunStatus: 'failed',
        lastError: errorMessage,
        totalRunsFailed: (pipeline.totalRunsFailed || 0) + 1,
      });

      // Log activity
      await this.activityLogService.logPipelineRunAction(
        pipeline.organizationId,
        userId,
        PIPELINE_RUN_ACTIONS.FAILED,
        runId,
        pipeline.id,
        pipeline.name,
        {
          error: errorMessage,
          durationSeconds,
          rowsRead: totalRowsRead,
          rowsWritten: totalRowsWritten,
        },
      );

      this.logger.error(`Pipeline ${pipeline.id} run ${runId} failed: ${errorMessage}`);
      throw error;
    }
  }

  /**
   * Pause pipeline
   */
  async pausePipeline(pipelineId: string, userId: string): Promise<Pipeline> {
    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    await this.checkPipelineManagePermission(userId, pipeline.organizationId);

    const updated = await this.pipelineRepository.update(pipelineId, {
      status: 'paused',
      migrationState: 'pending',
      nextSyncAt: null,
    });

    await this.activityLogService.logPipelineAction(
      pipeline.organizationId,
      userId,
      PIPELINE_ACTIONS.RUN_PAUSED,
      pipelineId,
      pipeline.name,
    );

    return updated;
  }

  /**
   * Resume pipeline
   */
  async resumePipeline(pipelineId: string, userId: string): Promise<Pipeline> {
    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    await this.checkPipelineManagePermission(userId, pipeline.organizationId);

    const updated = await this.pipelineRepository.update(pipelineId, {
      status: PipelineStatus.IDLE,
      nextSyncAt: new Date(),
    });

    await this.activityLogService.logPipelineAction(
      pipeline.organizationId,
      userId,
      PIPELINE_ACTIONS.RUN_RESUMED,
      pipelineId,
      pipeline.name,
    );

    return updated;
  }

  /**
   * Validate pipeline configuration
   */
  async validatePipeline(pipelineId: string, userId?: string): Promise<ValidationResult> {
    const pipelineWithSchemas = await this.pipelineRepository.findByIdWithSchemas(pipelineId);
    if (!pipelineWithSchemas) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    const { pipeline, sourceSchema, destinationSchema } = pipelineWithSchemas;

    if (userId) {
      await this.checkPipelineViewPermission(userId, pipeline.organizationId);
    }

    const errors: string[] = [];
    const warnings: string[] = [];

    // Validate source schema
    if (!sourceSchema.dataSourceId) {
      errors.push('Source schema must have a data source');
    }
    if (!sourceSchema.sourceTable && !sourceSchema.sourceQuery) {
      errors.push('Source schema must have a source table or query');
    }

    // Validate destination schema
    if (!destinationSchema.dataSourceId) {
      errors.push('Destination schema must have a data source');
    }
    if (!destinationSchema.destinationTable) {
      errors.push('Destination schema must have a destination table');
    }

    // Validate column mappings
    const columnMappings = (destinationSchema.columnMappings as ColumnMapping[]) || [];
    if (columnMappings.length === 0) {
      warnings.push('No column mappings defined - will attempt to map columns by name');
    } else {
      const mappingValidation = this.transformerService.validate(
        columnMappings,
        (pipeline.transformations as any[]) || undefined,
      );
      errors.push(...mappingValidation.errors);
      if (mappingValidation.warnings) {
        warnings.push(...mappingValidation.warnings);
      }
    }

    // Validate incremental sync configuration
    if (pipeline.syncMode === 'incremental' && !pipeline.incrementalColumn) {
      errors.push('Incremental sync requires an incremental column');
    }

    // Validate upsert configuration
    if (destinationSchema.writeMode === 'upsert') {
      const upsertKey = destinationSchema.upsertKey as string[];
      if (!upsertKey || upsertKey.length === 0) {
        errors.push('Upsert mode requires upsert key columns');
      }
    }

    return {
      valid: errors.length === 0,
      errors,
      warnings: warnings.length > 0 ? warnings : undefined,
    };
  }

  /**
   * Dry run pipeline (test without writing)
   */
  async dryRunPipeline(
    pipelineId: string,
    userId: string,
    sampleSize: number = 10,
  ): Promise<DryRunResult> {
    const pipelineWithSchemas = await this.pipelineRepository.findByIdWithSchemas(pipelineId);
    if (!pipelineWithSchemas) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    const { pipeline, sourceSchema, destinationSchema } = pipelineWithSchemas;

    await this.checkPipelineViewPermission(userId, pipeline.organizationId);

    // Collect sample data
    const sourceData = await this.collectorService.collect({
      sourceSchema,
      organizationId: pipeline.organizationId,
      userId,
      limit: sampleSize,
    });

    // Transform sample data
    const columnMappings = (destinationSchema.columnMappings as ColumnMapping[]) || [];
    const transformations = (pipeline.transformations as any[]) || [];

    const transformedSample = await this.transformerService.transform(
      sourceData.rows,
      columnMappings,
      transformations,
    );

    return {
      wouldWrite: transformedSample.length,
      sourceRowCount: sourceData.totalRows,
      sampleRows: sourceData.rows,
      transformedSample,
      errors: [],
    };
  }

  /**
   * Get pipeline runs
   */
  async getPipelineRuns(
    pipelineId: string,
    limit: number = 20,
    offset: number = 0,
  ): Promise<PipelineRun[]> {
    return await this.pipelineRepository.findRunsByPipeline(pipelineId, limit, offset);
  }

  /**
   * Get pipeline run by ID
   */
  async getPipelineRunById(runId: string): Promise<PipelineRun | null> {
    return await this.pipelineRepository.findRunById(runId);
  }

  /**
   * Get pipeline statistics
   */
  async getPipelineStats(pipelineId: string): Promise<{
    totalRowsProcessed: number;
    totalRunsSuccessful: number;
    totalRunsFailed: number;
    lastSuccessfulRun?: Date;
    averageDuration: number;
  }> {
    return await this.pipelineRepository.getStats(pipelineId);
  }

  /**
   * Cancel a running pipeline
   */
  async cancelPipelineRun(runId: string, userId: string): Promise<PipelineRun> {
    const run = await this.pipelineRepository.findRunById(runId);
    if (!run) {
      throw new NotFoundException(`Pipeline run ${runId} not found`);
    }

    const pipeline = await this.pipelineRepository.findById(run.pipelineId);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${run.pipelineId} not found`);
    }

    await this.checkPipelineManagePermission(userId, pipeline.organizationId);

    if (run.status !== 'running' && run.status !== 'pending') {
      throw new BadRequestException('Can only cancel running or pending runs');
    }

    const updated = await this.pipelineRepository.updateRun(runId, {
      status: 'cancelled',
      jobState: 'completed',
      completedAt: new Date(),
    });

    await this.activityLogService.logPipelineRunAction(
      pipeline.organizationId,
      userId,
      PIPELINE_RUN_ACTIONS.CANCELLED,
      runId,
      pipeline.id,
      pipeline.name,
    );

    return updated;
  }

  // ============================================================================
  // AUTHORIZATION HELPERS
  // ============================================================================

  private async checkPipelineViewPermission(userId: string, organizationId: string): Promise<void> {
    const canView = await this.roleService.canViewOrganization(userId, organizationId);
    if (!canView) {
      throw new ForbiddenException('You are not a member of this organization');
    }
  }

  private async checkPipelineManagePermission(
    userId: string,
    organizationId: string,
  ): Promise<void> {
    const canManage = await this.roleService.canManageDataSources(userId, organizationId);
    if (!canManage) {
      throw new ForbiddenException('Only OWNER, ADMIN, and EDITOR can manage pipelines');
    }
  }
}
