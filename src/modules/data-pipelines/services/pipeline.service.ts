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
import { ConnectionService } from '../../data-sources/connection.service';
import { OrganizationRoleService } from '../../organizations/services/organization-role.service';
import { PythonETLService } from './python-etl.service';
import { PipelineLifecycleService } from './pipeline-lifecycle.service';
import type {
  DryRunResult,
  ValidationResult,
  BatchOptions,
  PipelineError,
  WriteResult,
} from '../types/common.types';
import type { SchemaInfo } from '../types/common.types';
import { PipelineStatus, PipelineCheckpoint } from '../types/pipeline-lifecycle.types';
import { PipelineRepository } from '../repositories/pipeline.repository';
import { PipelineSourceSchemaRepository } from '../repositories/pipeline-source-schema.repository';
import { PipelineDestinationSchemaRepository } from '../repositories/pipeline-destination-schema.repository';
import type { CreatePipelineDto, UpdatePipelineDto } from '../dto';
import { ScheduleType } from '../dto/create-pipeline.dto';
import { PipelineSchedulerService } from './pipeline-scheduler.service';
import { PipelineQueueService } from '../../queue/pipeline-queue.service';

/**
 * Internal DTO for creating pipelines (with organizationId and userId)
 */
export interface CreatePipelineInput extends CreatePipelineDto {
  organizationId: string;
  userId: string;
}

/**
 * Default batch size for processing
 * Configurable per-pipeline via options.batchSize
 */
const DEFAULT_BATCH_SIZE = 500;
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
    private readonly pythonETLService: PythonETLService,
    private readonly activityLogService: ActivityLogService,
    private readonly roleService: OrganizationRoleService,
    private readonly lifecycleService: PipelineLifecycleService,
    private readonly schedulerService: PipelineSchedulerService,
    private readonly pipelineQueueService: PipelineQueueService,
    private readonly connectionService: ConnectionService,
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

    // Determine schedule type
    const scheduleType = (dto.scheduleType as ScheduleType) || ScheduleType.NONE;
    const scheduleValue = dto.scheduleValue;
    const scheduleTimezone = dto.scheduleTimezone || 'UTC';

    // Create pipeline with scheduling fields
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
      scheduleType,
      scheduleValue,
      scheduleTimezone,
    });

    // Set up scheduling if configured
    if (scheduleType !== ScheduleType.NONE) {
      try {
        const { nextRunAt } = await this.schedulerService.schedulePipeline(
          pipeline.id,
          organizationId,
          { scheduleType, scheduleValue, timezone: scheduleTimezone },
        );

        // Update pipeline with next scheduled run time
        await this.pipelineRepository.update(pipeline.id, {
          nextScheduledRunAt: nextRunAt,
        });

        this.logger.log(
          `Pipeline ${pipeline.id} scheduled: ${this.schedulerService.getHumanReadableSchedule(scheduleType, scheduleValue, scheduleTimezone)}`,
        );
      } catch (error) {
        this.logger.warn(`Failed to schedule pipeline ${pipeline.id}: ${error}`);
      }
    }

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
        scheduleType,
        scheduleValue,
        scheduleTimezone,
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

    // Handle schedule updates
    const scheduleType =
      (updates.scheduleType as ScheduleType) ||
      (pipeline.scheduleType as ScheduleType) ||
      ScheduleType.NONE;
    const scheduleValue =
      updates.scheduleValue !== undefined ? updates.scheduleValue : pipeline.scheduleValue;
    const scheduleTimezone = updates.scheduleTimezone || pipeline.scheduleTimezone || 'UTC';

    // Check if schedule has changed
    const scheduleChanged =
      updates.scheduleType !== undefined ||
      updates.scheduleValue !== undefined ||
      updates.scheduleTimezone !== undefined;

    let nextScheduledRunAt = pipeline.nextScheduledRunAt;

    if (scheduleChanged) {
      if (scheduleType === ScheduleType.NONE) {
        // Unschedule the pipeline
        await this.schedulerService.unschedulePipeline(id);
        nextScheduledRunAt = null;
        this.logger.log(`Pipeline ${id} schedule removed`);
      } else {
        // Schedule or reschedule the pipeline
        try {
          const { nextRunAt } = await this.schedulerService.schedulePipeline(
            id,
            pipeline.organizationId,
            { scheduleType, scheduleValue: scheduleValue || undefined, timezone: scheduleTimezone },
          );
          nextScheduledRunAt = nextRunAt;
          this.logger.log(
            `Pipeline ${id} rescheduled: ${this.schedulerService.getHumanReadableSchedule(scheduleType, scheduleValue || undefined, scheduleTimezone)}`,
          );
        } catch (error) {
          this.logger.warn(`Failed to reschedule pipeline ${id}: ${error}`);
          throw error;
        }
      }
    }

    // Update pipeline with all fields including schedule
    const updateData: any = {
      ...updates,
      scheduleType,
      scheduleValue,
      scheduleTimezone,
      nextScheduledRunAt,
    };

    const updated = await this.pipelineRepository.update(id, updateData);

    // Log activity
    await this.activityLogService.logPipelineAction(
      pipeline.organizationId,
      userId,
      PIPELINE_ACTIONS.UPDATED,
      id,
      updated.name,
      {
        changes: updates,
        scheduleType,
        scheduleValue,
        scheduleTimezone,
      },
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
    triggerType: 'manual' | 'scheduled' | 'api' | 'polling' = 'manual',
    options?: BatchOptions,
  ): Promise<PipelineRun> {
    const pipelineWithSchemas = await this.pipelineRepository.findByIdWithSchemas(pipelineId);
    if (!pipelineWithSchemas) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    const { pipeline, sourceSchema, destinationSchema } = pipelineWithSchemas;

    // AUTHORIZATION - Skip for scheduled/polling triggers (system-initiated)
    // These are internal operations and the pipeline creator has already been authorized
    if (triggerType === 'manual' || triggerType === 'api') {
      await this.checkPipelineManagePermission(userId, pipeline.organizationId);
    }

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

    // Determine sync mode - Python handles all CDC/incremental logic
    // NestJS only orchestrates: calls Python with sync mode and checkpoint
    let checkpoint = await this.lifecycleService.getCheckpoint(pipeline.id);

    // Determine sync mode from pipeline configuration
    // Python will handle CDC detection, incremental logic, checkpoint management
    const syncMode = pipeline.syncMode || 'full';
    const isFullSync = syncMode === 'full' || !checkpoint;
    const syncType = isFullSync ? 'full' : 'incremental';
    const syncReason = isFullSync
      ? 'Full sync (Python handles all data collection)'
      : 'Incremental sync (Python handles CDC and checkpoint management)';

    // Log startup with detailed sync info
    console.log(`\n${'='.repeat(60)}`);
    console.log(`🚀 STARTING PIPELINE: ${pipeline.name}`);
    console.log(`${'='.repeat(60)}`);
    console.log(`   Run ID:          ${runId}`);
    console.log(`   Sync Mode:       ${pipeline.syncMode}`);
    console.log(`   Sync Type:       ${syncType.toUpperCase()}`);
    console.log(`   Sync Reason:     ${syncReason}`);
    console.log(`   Batch Size:      ${options?.batchSize || DEFAULT_BATCH_SIZE}`);
    console.log(
      `   Source:          ${sourceSchema.sourceType} - ${sourceSchema.sourceTable || 'query'}`,
    );
    console.log(`${'='.repeat(60)}\n`);

    // ROOT FIX: Publish starting status via Socket.io for real-time UI update
    if (this.pipelineQueueService.isReady()) {
      await this.pipelineQueueService.publishStatusUpdate({
        pipelineId: pipeline.id,
        organizationId: pipeline.organizationId,
        status: 'running',
        rowsProcessed: 0,
        timestamp: new Date().toISOString(),
      });
    }

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
      // For full sync, clear checkpoint to start fresh
      // Python will handle all checkpoint management
      if (isFullSync && checkpoint) {
        this.logger.log('Full sync detected - clearing checkpoint to start from beginning');
        await this.lifecycleService.clearCheckpoint(pipeline.id, userId);
      }

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

      // Get transform script
      const transformScript = destinationSchema.transformScript;

      if (!transformScript || !transformScript.trim()) {
        throw new BadRequestException('Transform script is required for destination schema');
      }

      // Collect data with batching
      // Python handles all pagination, checkpoint management, and CDC logic
      let hasMore = true;
      let offset = isFullSync ? 0 : checkpoint?.offset || 0;
      let cursor: string | undefined = isFullSync ? undefined : checkpoint?.cursor;

      // Log checkpoint restoration if applicable
      if (!isFullSync && checkpoint?.rowsProcessed) {
        console.log(
          `📂 Resuming from checkpoint: ${checkpoint.rowsProcessed.toLocaleString()} rows already processed`,
        );
      } else if (isFullSync) {
        console.log(`🔄 Full sync: Starting from beginning`);
      }

      while (hasMore) {
        batchCount++;
        const batchStartTime = Date.now();

        // STEP 1: Collect data from source (with retry)
        let sourceData: { rows: any[]; totalRows?: number; nextCursor?: string; hasMore?: boolean };

        console.log(`\n📦 Batch ${batchCount}: Collecting data...`);

        // Get connection config for source
        const sourceConnectionConfig = await this.connectionService.getDecryptedConnection(
          pipeline.organizationId,
          sourceSchema.dataSourceId!,
          userId,
        );

        for (let attempt = 0; attempt < retryAttempts; attempt++) {
          try {
            // Call Python ETL service - Python handles all CDC/incremental logic
            // Python will determine WAL CDC, checkpoint management, incremental detection, etc.
            sourceData = await this.pythonETLService.collect({
              sourceSchema,
              connectionConfig: sourceConnectionConfig,
              organizationId: pipeline.organizationId,
              userId,
              syncMode: syncType,
              checkpoint: checkpoint || undefined, // Pass current checkpoint to Python
              limit: batchSize,
              offset,
              cursor,
            });

            // Python returns updated checkpoint in metadata - use it for next batch and save it
            const resultMetadata = (sourceData as any).metadata;
            if (resultMetadata?.checkpoint) {
              // Merge Python checkpoint with current progress tracking
              checkpoint = {
                ...(resultMetadata.checkpoint as PipelineCheckpoint),
                rowsProcessed: totalRowsWritten, // Keep track of actual rows written so far
                totalRows: sourceData.totalRows || estimatedTotalRows, // Use Python's total_rows if available
              };
              // Save checkpoint immediately so it persists for next run (CDC support)
              await this.lifecycleService.saveCheckpoint(pipeline.id, checkpoint, userId);
              this.logger.debug(`Checkpoint saved: ${JSON.stringify(checkpoint).slice(0, 200)}...`);
            }
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
        // Python returns total_rows which is the actual total in source
        if (sourceData.totalRows && (!estimatedTotalRows || estimatedTotalRows !== sourceData.totalRows)) {
          estimatedTotalRows = sourceData.totalRows;
          console.log(`   📊 Total rows in source: ${estimatedTotalRows.toLocaleString()}`);
        }
        
        // Also check checkpoint for total_records from Python bookmarks (more authoritative)
        const resultMetadataForTotal = (sourceData as any).metadata;
        if (resultMetadataForTotal?.checkpoint) {
          const pythonCheckpoint = resultMetadataForTotal.checkpoint as any;
          // Python stores total_records in bookmarks.stream_id.total_records
          if (pythonCheckpoint?.bookmarks) {
            const bookmarks = pythonCheckpoint.bookmarks;
            const streamId = Object.keys(bookmarks)[0]; // Get first stream
            if (streamId && bookmarks[streamId]?.total_records) {
              const pythonTotalRecords = bookmarks[streamId].total_records;
              if (pythonTotalRecords && (!estimatedTotalRows || estimatedTotalRows !== pythonTotalRecords)) {
                estimatedTotalRows = pythonTotalRecords;
                console.log(`   📊 Total rows from checkpoint: ${pythonTotalRecords.toLocaleString()}`);
              }
            }
          }
        }

        totalRowsRead += sourceData.rows.length;

        // Calculate progress
        const percentage = estimatedTotalRows
          ? Math.min(100, Math.round((totalRowsRead / estimatedTotalRows) * 100))
          : undefined;

        console.log(
          `   📥 Collected ${sourceData.rows.length.toLocaleString()} rows (Total: ${totalRowsRead.toLocaleString()}${estimatedTotalRows ? `/${estimatedTotalRows.toLocaleString()}` : ''}${percentage ? ` - ${percentage}%` : ''})`,
        );

        // Primary keys are determined from destination schema upsertKey if available
        const primaryKeys = (destinationSchema.upsertKey as string[]) || [];

        // ROOT FIX: Determine write mode for CDC-friendly data preservation
        // Priority:
        // 1. If explicit upsertKey configured in destination, use UPSERT
        // 2. If primary keys mapped, use UPSERT (prevents duplicates on re-runs)
        // 3. Use destination schema writeMode (append/upsert/replace)
        // 4. Default to APPEND (never truncate by default)
        const configuredWriteMode = destinationSchema.writeMode as
          | 'append'
          | 'upsert'
          | 'replace'
          | undefined;
        const configuredUpsertKey = (destinationSchema.upsertKey as string[]) || undefined;

        let effectiveWriteMode: 'append' | 'upsert' | 'replace' = 'append';
        let effectiveUpsertKey: string[] | undefined = configuredUpsertKey;

        // CDC FIX: Always prefer UPSERT when we have keys to prevent duplicates
        if (configuredUpsertKey && configuredUpsertKey.length > 0) {
          // Explicit upsert key configured - use UPSERT
          effectiveWriteMode = 'upsert';
          this.logger.log(
            `Using UPSERT mode with configured key: ${configuredUpsertKey.join(', ')}`,
          );
        } else if (primaryKeys.length > 0) {
          // Primary keys from column mappings - use UPSERT for data integrity
          effectiveWriteMode = 'upsert';
          effectiveUpsertKey = primaryKeys;
          this.logger.log(`Using UPSERT mode with primary keys: ${primaryKeys.join(', ')}`);
        } else if (configuredWriteMode) {
          // Use configured write mode (only REPLACE if explicitly set by user)
          effectiveWriteMode = configuredWriteMode;
          // WARN: replace mode truncates table - should only be explicit choice
          if (configuredWriteMode === 'replace' && batchCount > 1) {
            // Don't truncate on subsequent batches
            effectiveWriteMode = 'append';
          }
        }
        // else: default remains 'append' - safest default for data preservation

        console.log(
          `   📤 Writing ${sourceData.rows.length.toLocaleString()} rows (mode: ${effectiveWriteMode})...`,
        );

        // STEP 2: Emit data to destination (with internal transformation)
        // Get connection config for destination
        const destConnectionConfig = await this.connectionService.getDecryptedConnection(
          pipeline.organizationId,
          destinationSchema.dataSourceId!,
          userId,
        );

        for (let attempt = 0; attempt < retryAttempts; attempt++) {
          try {
            // Transform data first using transform script
            const transformResult = await this.pythonETLService.transform({
              rows: sourceData.rows,
              transformScript: transformScript,
            });

            const writeResult = await this.pythonETLService.emit({
              destinationSchema,
              connectionConfig: destConnectionConfig,
              organizationId: pipeline.organizationId,
              userId,
              rows: transformResult.transformedRows,
              writeMode: effectiveWriteMode,
              upsertKey: effectiveUpsertKey,
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

            // ROOT FIX: Publish real-time progress update via Socket.io
            if (this.pipelineQueueService.isReady()) {
              await this.pipelineQueueService.publishStatusUpdate({
                pipelineId: pipeline.id,
                organizationId: pipeline.organizationId,
                status: 'running',
                rowsProcessed: totalRowsWritten,
                newRowsCount: writeResult.rowsWritten,
                timestamp: new Date().toISOString(),
              });
            }

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
        // ROOT FIX: Correct pagination logic - ensure all records are processed
        // hasMore = true if collector says there's more OR we got a full batch (might be more)
        // hasMore = false if we got partial batch (< batchSize) AND collector says no more
        hasMore = sourceData.hasMore === true || sourceData.rows.length === batchSize;
        offset += sourceData.rows.length; // Use actual rows collected, not batchSize
        cursor = sourceData.nextCursor;

        // Debug: log pagination state
        console.log(
          `   🔄 Pagination: hasMore=${hasMore}, nextOffset=${offset}, cursor=${cursor || 'none'}, rowsThisBatch=${sourceData.rows.length}, batchSize=${batchSize}, sourceHasMore=${sourceData.hasMore}`,
        );

        // Safety check: If we got fewer rows than batchSize, we've likely reached the end
        // But trust the collector's hasMore flag if it's explicitly set
        if (sourceData.rows.length < batchSize && sourceData.hasMore !== true) {
          hasMore = false;
          console.log(
            `   ✓ End of data detected (got ${sourceData.rows.length}/${batchSize} rows)`,
          );
        }

        // Python returns updated checkpoint in metadata - save it
        const resultMetadata = (sourceData as any).metadata;
        if (resultMetadata?.checkpoint) {
          // Extract total_records from Python checkpoint bookmarks if available
          const pythonCheckpoint = resultMetadata.checkpoint as any;
          let pythonTotalRecords = sourceData.totalRows || estimatedTotalRows;
          if (pythonCheckpoint?.bookmarks) {
            const bookmarks = pythonCheckpoint.bookmarks;
            const streamId = Object.keys(bookmarks)[0]; // Get first stream
            if (streamId && bookmarks[streamId]?.total_records) {
              pythonTotalRecords = bookmarks[streamId].total_records;
            }
          }
          
          // Use checkpoint returned from Python (Python handles all CDC/checkpoint logic)
          // Merge with current progress to ensure rowsProcessed is accurate
          const updatedCheckpoint: PipelineCheckpoint = {
            ...(resultMetadata.checkpoint as PipelineCheckpoint),
            rowsProcessed: totalRowsWritten, // Always use actual rows written, not Python's value
            totalRows: pythonTotalRecords, // Use Python's total_records from bookmarks as authoritative source
            lastSyncAt: new Date().toISOString(),
            offset,
            cursor,
            currentBatch: batchCount,
          };
          await this.lifecycleService.saveCheckpoint(pipeline.id, updatedCheckpoint, userId);
          checkpoint = updatedCheckpoint;
        } else {
          // Fallback: update basic checkpoint info if Python didn't return one
          const currentCheckpoint: PipelineCheckpoint = {
            ...(checkpoint || {}),
            lastSyncAt: new Date().toISOString(),
            offset,
            cursor,
            rowsProcessed: totalRowsWritten,
            totalRows: estimatedTotalRows,
            currentBatch: batchCount,
          };
          await this.lifecycleService.saveCheckpoint(pipeline.id, currentCheckpoint, userId);
          checkpoint = currentCheckpoint;
        }

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

      // Get the final checkpoint (Python may have updated it)
      const finalCheckpoint = await this.lifecycleService.getCheckpoint(pipeline.id);

      // ROOT FIX: For incremental/CDC pipelines, set status to 'listing' so CDC polling picks them up
      // every 5 min (regardless of schedule). For full-only pipelines, use 'idle'.
      const targetStatus =
        pipeline.syncMode === 'incremental' ? PipelineStatus.LISTING : PipelineStatus.IDLE;

      // Update totalRowsProcessed - cumulative across all runs
      const newTotalRowsProcessed = (pipeline.totalRowsProcessed || 0) + totalRowsWritten;

      // Calculate next scheduled run time based on pipeline schedule configuration
      // Default to 2 minutes for CDC/incremental polling if no schedule configured
      let nextScheduledRunAt: Date | null = null;
      const scheduleType = pipeline.scheduleType || 'none';
      const scheduleValue = pipeline.scheduleValue || '';

      if (scheduleType !== 'none') {
        nextScheduledRunAt = this.calculateNextScheduledRun(scheduleType, scheduleValue);
      } else if (pipeline.syncMode === 'incremental') {
        // Default 2-minute polling for incremental/CDC mode
        nextScheduledRunAt = new Date(Date.now() + 2 * 60 * 1000);
      }

      // Update pipeline with final checkpoint from Python
      await this.pipelineRepository.update(pipeline.id, {
        lastRunAt: new Date(),
        lastRunStatus: totalRowsFailed > 0 && totalRowsWritten === 0 ? 'failed' : 'success',
        status: targetStatus,
        totalRowsProcessed: newTotalRowsProcessed,
        totalRunsSuccessful: (pipeline.totalRunsSuccessful || 0) + (totalRowsFailed === 0 ? 1 : 0),
        totalRunsFailed:
          (pipeline.totalRunsFailed || 0) + (totalRowsFailed > 0 && totalRowsWritten === 0 ? 1 : 0),
        lastSyncAt: new Date(),
        // Store checkpoint returned from Python (Python handles all CDC/checkpoint logic)
        checkpoint: finalCheckpoint || undefined,
        // Schedule next run
        nextScheduledRunAt: nextScheduledRunAt,
        nextSyncAt: nextScheduledRunAt,
      });

      // Log completion summary
      console.log(`\n${'='.repeat(60)}`);
      console.log(`✅ PIPELINE COMPLETED: ${pipeline.name}`);
      console.log(`${'='.repeat(60)}`);
      console.log(`   Sync Type:       ${syncType.toUpperCase()}`);
      console.log(`   Rows Read:       ${totalRowsRead.toLocaleString()}`);
      console.log(`   Rows Written:    ${totalRowsWritten.toLocaleString()}`);
      console.log(`   Rows Skipped:    ${totalRowsSkipped.toLocaleString()}`);
      console.log(`   Rows Failed:     ${totalRowsFailed.toLocaleString()}`);
      console.log(`   Duration:        ${durationSeconds}s`);
      console.log(`   Final Status:    ${targetStatus}`);
      console.log(`${'='.repeat(60)}\n`);

      this.logger.log(`Pipeline ${pipeline.id} status set to ${targetStatus} after completion`);

      // Log activity
      await this.activityLogService.logPipelineRunAction(
        pipeline.organizationId,
        userId,
        PIPELINE_RUN_ACTIONS.COMPLETED,
        runId,
        pipeline.id,
        pipeline.name,
        {
          syncType,
          rowsRead: totalRowsRead,
          rowsWritten: totalRowsWritten,
          rowsSkipped: totalRowsSkipped,
          rowsFailed: totalRowsFailed,
          durationSeconds,
          finalStatus: targetStatus,
        },
      );

      this.logger.log(
        `Pipeline ${pipeline.id} run ${runId} completed: ${totalRowsWritten} rows written in ${durationSeconds}s`,
      );

      // ROOT FIX: Publish completion status via Socket.io for real-time UI update
      // Use newTotalRowsProcessed to show cumulative total in UI
      if (this.pipelineQueueService.isReady()) {
        await this.pipelineQueueService.publishStatusUpdate({
          pipelineId: pipeline.id,
          organizationId: pipeline.organizationId,
          status: targetStatus,
          rowsProcessed: newTotalRowsProcessed, // Cumulative total, not just this run
          newRowsCount: totalRowsWritten, // New rows in this run
          timestamp: new Date().toISOString(),
        });
      }
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

      // ROOT FIX: Publish failure status via Socket.io for real-time UI update
      if (this.pipelineQueueService.isReady()) {
        await this.pipelineQueueService.publishStatusUpdate({
          pipelineId: pipeline.id,
          organizationId: pipeline.organizationId,
          status: 'failed',
          rowsProcessed: totalRowsWritten,
          error: errorMessage,
          timestamp: new Date().toISOString(),
        });
      }

      throw error;
    }
  }

  /**
   * Pause pipeline
   * Pause pipeline - Python handles checkpoint preservation
   */
  async pausePipeline(pipelineId: string, userId: string): Promise<Pipeline> {
    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    await this.checkPipelineManagePermission(userId, pipeline.organizationId);

    // Get current checkpoint to preserve it
    const checkpoint = await this.lifecycleService.getCheckpoint(pipelineId);
    const pauseTimestamp = new Date().toISOString();

    // Update checkpoint with pause timestamp
    if (checkpoint) {
      await this.lifecycleService.saveCheckpoint(
        pipelineId,
        {
          ...checkpoint,
          pauseTimestamp,
        },
        userId,
      );
    }

    const updated = await this.pipelineRepository.update(pipelineId, {
      status: 'paused',
      migrationState: 'pending',
      nextSyncAt: null,
      nextScheduledRunAt: null, // Clear scheduled run time when paused
      pauseTimestamp: new Date(), // Store pause timestamp in pipeline
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
   * Resume pipeline - Python handles delta calculation and checkpoint management
   */
  async resumePipeline(pipelineId: string, userId: string): Promise<Pipeline> {
    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    await this.checkPipelineManagePermission(userId, pipeline.organizationId);

    // Get checkpoint - Python will handle delta calculation and checkpoint management
    const _checkpoint = await this.lifecycleService.getCheckpoint(pipelineId);

    // Recalculate next scheduled run time if the pipeline has a schedule
    let nextScheduledRunAt: Date | null = null;
    const scheduleType = pipeline.scheduleType as string;

    if (scheduleType && scheduleType !== 'none') {
      try {
        const result = await this.schedulerService.schedulePipeline(
          pipelineId,
          pipeline.organizationId,
          {
            scheduleType: scheduleType as any,
            scheduleValue: pipeline.scheduleValue || undefined,
            timezone: pipeline.scheduleTimezone || 'UTC',
          },
        );
        nextScheduledRunAt = result.nextRunAt;
        this.logger.log(
          `Pipeline ${pipelineId} rescheduled on resume: next run at ${nextScheduledRunAt?.toISOString()}`,
        );
      } catch (error) {
        this.logger.warn(`Failed to reschedule pipeline ${pipelineId} on resume: ${error}`);
      }
    }

    // ROOT FIX: Don't clear pauseTimestamp - it's needed for delta calculation
    // The checkpoint already has pauseTimestamp, which will be used in collectIncremental
    const updated = await this.pipelineRepository.update(pipelineId, {
      status: PipelineStatus.IDLE,
      nextSyncAt: new Date(),
      nextScheduledRunAt: nextScheduledRunAt,
      // Keep pauseTimestamp in pipeline for reference, but checkpoint has the authoritative one
    });

    // Python handles all delta calculation and checkpoint management
    this.logger.log(
      `Pipeline ${pipelineId} resumed. Python will handle delta calculation and checkpoint management.`,
    );

    await this.activityLogService.logPipelineAction(
      pipeline.organizationId,
      userId,
      PIPELINE_ACTIONS.RUN_RESUMED,
      pipelineId,
      pipeline.name,
      { nextScheduledRunAt: nextScheduledRunAt?.toISOString() },
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

    // Validate transform script
    if (!destinationSchema.transformScript || !destinationSchema.transformScript.trim()) {
      errors.push('Transform script is required');
    }

    // Python handles incremental sync validation - no need to validate here

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

    // Get connection configs
    const sourceConnectionConfig = await this.connectionService.getDecryptedConnection(
      pipeline.organizationId,
      sourceSchema.dataSourceId!,
      userId,
    );

    // Collect sample data
    const sourceData = await this.pythonETLService.collect({
      sourceSchema,
      connectionConfig: sourceConnectionConfig,
      organizationId: pipeline.organizationId,
      userId,
      limit: sampleSize,
    });

    // Transform sample data
    const transformScript = destinationSchema.transformScript;

    if (!transformScript || !transformScript.trim()) {
      throw new BadRequestException('Transform script is required for destination schema');
    }

    const transformResult = await this.pythonETLService.transform({
      rows: sourceData.rows,
      transformScript: transformScript,
    });
    const transformedSample = transformResult.transformedRows;

    // Log sample transformed data
    if (transformedSample.length > 0) {
      this.logger.log(
        `Dry run sample transformed data: ${JSON.stringify(transformedSample[0], null, 2)}`,
      );
    }

    return {
      wouldWrite: transformedSample.length,
      sourceRowCount: sourceData.totalRows,
      sampleRows: sourceData.rows,
      transformedSample,
      errors: [],
      appliedMappings: [],
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
  // BIDIRECTIONAL PIPELINE EXECUTION (NoSQL ↔ SQL)
  // ============================================================================

  /**
   * Execute a pipeline with bidirectional transformation support
   * Handles complex transformations between NoSQL and SQL sources
   *
   * Use this for:
   * - MongoDB → PostgreSQL (flattening nested documents)
   * - PostgreSQL → MongoDB (embedding related data)
   */
  async executeBidirectionalPipeline(
    pipelineId: string,
    userId: string,
    options?: {
      batchSize?: number;
      upsertKeys?: Record<string, string[]>;
    },
  ): Promise<PipelineRun> {
    const pipelineWithSchemas = await this.pipelineRepository.findByIdWithSchemas(pipelineId);
    if (!pipelineWithSchemas) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    const { pipeline, sourceSchema, destinationSchema } = pipelineWithSchemas;

    // Validate source and destination
    if (!sourceSchema.dataSourceId || !destinationSchema.dataSourceId) {
      throw new BadRequestException('Source and destination must have data source IDs');
    }

    // Get source and destination data source info
    const sourceDataSource = await this.dataSourceRepository.findById(sourceSchema.dataSourceId);
    const destDataSource = await this.dataSourceRepository.findById(destinationSchema.dataSourceId);

    if (!sourceDataSource || !destDataSource) {
      throw new BadRequestException('Source or destination data source not found');
    }

    // Determine schema types
    const relationalTypes = [
      'postgres',
      'postgresql',
      'mysql',
      'mariadb',
      'sqlite',
      'mssql',
      'oracle',
    ];
    const isRelational = (type: string) => relationalTypes.includes(type?.toLowerCase());

    const sourceSchemaInfo: SchemaInfo = {
      columns: [],
      primaryKeys: [],
      isRelational: isRelational(sourceDataSource.sourceType),
      sourceType: sourceDataSource.sourceType,
      entityName: sourceSchema.sourceTable || undefined,
    };

    const destSchemaInfo: SchemaInfo = {
      columns: [],
      primaryKeys: [],
      isRelational: isRelational(destDataSource.sourceType),
      sourceType: destDataSource.sourceType,
      entityName: destinationSchema.destinationTable || undefined,
    };

    this.logger.log(
      `Bidirectional pipeline: ${sourceDataSource.sourceType} (${sourceSchemaInfo.isRelational ? 'SQL' : 'NoSQL'}) → ` +
        `${destDataSource.sourceType} (${destSchemaInfo.isRelational ? 'SQL' : 'NoSQL'})`,
    );

    // Create run record
    const run = await this.pipelineRepository.createRun({
      pipelineId,
      organizationId: pipeline.organizationId,
      status: 'pending',
      jobState: 'pending',
      triggerType: 'manual',
      triggeredBy: userId,
      startedAt: new Date(),
    });

    // Execute asynchronously
    this.executeBidirectionalAsync(
      run.id,
      pipeline,
      sourceSchema,
      destinationSchema,
      sourceSchemaInfo,
      destSchemaInfo,
      userId,
      options,
    ).catch((error) => {
      this.logger.error(
        `Bidirectional pipeline execution failed: ${error instanceof Error ? error.message : String(error)}`,
      );
    });

    return run;
  }

  /**
   * Execute bidirectional pipeline asynchronously
   */
  private async executeBidirectionalAsync(
    runId: string,
    pipeline: Pipeline,
    sourceSchema: PipelineSourceSchema,
    destinationSchema: PipelineDestinationSchema,
    sourceSchemaInfo: SchemaInfo,
    destSchemaInfo: SchemaInfo,
    userId: string,
    options?: {
      batchSize?: number;
      upsertKeys?: Record<string, string[]>;
    },
  ): Promise<void> {
    const startTime = Date.now();
    const batchSize = options?.batchSize || 1000;
    let totalRowsRead = 0;
    let totalRowsWritten = 0;

    try {
      // Update run to running
      await this.pipelineRepository.updateRun(runId, {
        status: 'running',
        jobState: 'running',
      });

      // Get transform script
      const transformScript = destinationSchema.transformScript;

      // Basic validation
      if (!transformScript || !transformScript.trim()) {
        throw new BadRequestException('Transform script is required');
      }

      // Get connection configs
      const sourceConnectionConfig = await this.connectionService.getDecryptedConnection(
        pipeline.organizationId,
        sourceSchema.dataSourceId!,
        userId,
      );

      // Collect all data (for simplicity, batching can be added later)
      console.log(`\n📦 Collecting data from ${sourceSchemaInfo.sourceType}...`);

      const sourceData = await this.pythonETLService.collect({
        sourceSchema,
        connectionConfig: sourceConnectionConfig,
        organizationId: pipeline.organizationId,
        userId,
        limit: batchSize * 10, // Collect more for batch processing
        offset: 0,
      });

      if (!sourceData || sourceData.rows.length === 0) {
        console.log(`   ✓ No data to transform`);
        await this.pipelineRepository.updateRun(runId, {
          status: 'success',
          jobState: 'completed',
          rowsRead: 0,
          rowsWritten: 0,
          completedAt: new Date(),
          durationSeconds: Math.floor((Date.now() - startTime) / 1000),
        });
        return;
      }

      totalRowsRead = sourceData.rows.length;
      console.log(`   📥 Collected ${totalRowsRead.toLocaleString()} rows`);

      // Transform using Python service
      console.log(
        `\n🔄 Transforming data (${sourceSchemaInfo.isRelational ? 'SQL' : 'NoSQL'} → ${destSchemaInfo.isRelational ? 'SQL' : 'NoSQL'})...`,
      );

      const transformResult = await this.pythonETLService.transform({
        rows: sourceData.rows,
        transformScript: transformScript || '',
      });

      // Group by entity if needed (simplified - assumes single entity for now)
      const transformedData: Record<string, any[]> = {
        default: transformResult.transformedRows,
      };

      // Log transformation results
      for (const [entity, rows] of Object.entries(transformedData)) {
        console.log(`   📋 Entity '${entity}': ${rows.length} rows`);
      }

      // Emit to destination
      console.log(`\n📤 Writing to ${destSchemaInfo.sourceType}...`);

      const destConnectionConfig = await this.connectionService.getDecryptedConnection(
        pipeline.organizationId,
        destinationSchema.dataSourceId!,
        userId,
      );

      const writeResults: Record<string, WriteResult> = {};
      for (const [entity, rows] of Object.entries(transformedData)) {
        const result = await this.pythonETLService.emit({
          destinationSchema,
          connectionConfig: destConnectionConfig,
          organizationId: pipeline.organizationId,
          userId,
          rows,
          writeMode: (destinationSchema.writeMode as 'append' | 'upsert' | 'replace') || 'append',
          upsertKey: options?.upsertKeys?.[entity],
        });
        writeResults[entity] = result;
      }

      // Calculate totals
      for (const [entity, result] of Object.entries(writeResults)) {
        totalRowsWritten += result.rowsWritten;
        console.log(`   ✓ ${entity}: ${result.rowsWritten} rows written`);
      }

      const duration = Date.now() - startTime;
      console.log(
        `\n✅ Pipeline completed: ${totalRowsWritten.toLocaleString()} rows written in ${(duration / 1000).toFixed(1)}s\n`,
      );

      // Update run as success
      await this.pipelineRepository.updateRun(runId, {
        status: 'success',
        jobState: 'completed',
        rowsRead: totalRowsRead,
        rowsWritten: totalRowsWritten,
        completedAt: new Date(),
        durationSeconds: Math.floor(duration / 1000),
      });

      // Update pipeline
      await this.pipelineRepository.update(pipeline.id, {
        lastRunStatus: 'success',
        lastRunAt: new Date(),
      });
    } catch (error) {
      const duration = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : String(error);

      console.log(`\n❌ Pipeline failed: ${errorMessage}\n`);
      this.logger.error(`Bidirectional pipeline failed: ${errorMessage}`);

      // Update run as failed
      await this.pipelineRepository.updateRun(runId, {
        status: 'failed',
        jobState: 'failed',
        rowsRead: totalRowsRead,
        rowsWritten: totalRowsWritten,
        completedAt: new Date(),
        durationSeconds: Math.floor(duration / 1000),
        errorMessage: errorMessage,
      });

      // Update pipeline
      await this.pipelineRepository.update(pipeline.id, {
        lastRunStatus: 'failed',
        lastRunAt: new Date(),
      });
    }
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

  // ============================================================================
  // SCHEDULING HELPERS
  // ============================================================================

  /**
   * Calculate next scheduled run time based on schedule type and value
   * Used for CDC/incremental sync scheduling
   */
  private calculateNextScheduledRun(scheduleType: string, scheduleValue: string | null): Date {
    const now = new Date();
    const value = scheduleValue || '';

    switch (scheduleType) {
      case 'minutes': {
        // Value is number of minutes (default: 2 for CDC)
        const minutes = parseInt(value, 10) || 2;
        return new Date(now.getTime() + minutes * 60 * 1000);
      }
      case 'hourly': {
        // Value is number of hours (default: 1)
        const hours = parseInt(value, 10) || 1;
        return new Date(now.getTime() + hours * 60 * 60 * 1000);
      }
      case 'daily': {
        // Value is time in HH:MM format (default: 00:00)
        const [hours, minutes] = (value || '00:00').split(':').map(Number);
        const next = new Date(now);
        next.setDate(next.getDate() + 1);
        next.setHours(hours || 0, minutes || 0, 0, 0);
        return next;
      }
      case 'weekly': {
        // Run once a week
        return new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000);
      }
      case 'monthly': {
        // Run once a month
        const next = new Date(now);
        next.setMonth(next.getMonth() + 1);
        return next;
      }
      case 'custom_cron': {
        // For now, default to 1 hour (cron parsing would require a library)
        return new Date(now.getTime() + 60 * 60 * 1000);
      }
      default:
        // Default to 2 minutes for CDC/incremental
        return new Date(now.getTime() + 2 * 60 * 1000);
    }
  }
}
