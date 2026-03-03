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
import { ActivityLoggerService } from '../../../common/logger';
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
import { PgmqQueueService } from '../../queue';

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
    private readonly pipelineQueueService: PgmqQueueService,
    private readonly connectionService: ConnectionService,
    private readonly activity: ActivityLoggerService,
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
   * Get pipelines by organization with pagination.
   * Uses cursor-based pagination when cursor is provided (efficient for 1M+ pipelines).
   */
  async findByOrganizationPaginated(
    organizationId: string,
    userId: string | undefined,
    limit: number = 20,
    offset: number = 0,
    cursor?: string,
  ) {
    if (userId) {
      await this.checkPipelineViewPermission(userId, organizationId);
    }

    if (cursor) {
      return this.pipelineRepository.findByOrganizationPaginatedCursor(
        organizationId,
        limit,
        cursor,
      );
    }
    return this.pipelineRepository.findByOrganizationPaginated(organizationId, limit, offset);
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
   * Get sync state (cursor/LSN/binlog) for incremental/CDC pipelines.
   * NestJS owns state — stored in pipeline.checkpoint.
   */
  async getSyncState(pipelineId: string, organizationId: string, userId: string): Promise<{
    pipeline_id: string;
    state: Record<string, unknown> | null;
    message: string;
  }> {
    await this.checkPipelineManagePermission(userId, organizationId);
    const pipeline = await this.pipelineRepository.findById(pipelineId, organizationId);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }
    return {
      pipeline_id: pipelineId,
      state: (pipeline.checkpoint as Record<string, unknown>) ?? null,
      message: pipeline.checkpoint
        ? 'Sync state found — next run will resume from cursor'
        : 'No sync state — will do full sync on first run',
    };
  }

  /**
   * Reset sync state — next run will do a full sync.
   */
  async resetSyncState(pipelineId: string, organizationId: string, userId: string): Promise<{
    pipeline_id: string;
    deleted: boolean;
    message: string;
  }> {
    await this.checkPipelineManagePermission(userId, organizationId);
    const pipeline = await this.pipelineRepository.findById(pipelineId, organizationId);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }
    await this.pipelineRepository.update(pipelineId, {
      checkpoint: null,
      lastSyncValue: null,
      updatedAt: new Date(),
    });
    return {
      pipeline_id: pipelineId,
      deleted: true,
      message: 'Sync state reset — next run will do a full sync',
    };
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
    this.activity.info(
      syncType === 'full' ? 'sync.full_started' : 'sync.incremental_started',
      `Pipeline started: ${pipeline.name}`,
      {
        pipelineId: pipeline.id,
        runId,
        organizationId: pipeline.organizationId,
        userId,
        metadata: {
          syncMode: pipeline.syncMode,
          syncType,
          syncReason,
          batchSize: options?.batchSize || DEFAULT_BATCH_SIZE,
          source: `${sourceSchema.sourceType} - ${sourceSchema.sourceTable || 'query'}`,
        },
      },
    );

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

    let totalRowsRead = 0;
    let totalRowsWritten = 0;
    let totalRowsSkipped = 0;
    let totalRowsFailed = 0;

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

      // dlt-based sync: single runSync call (replaces collect -> transform -> emit)
      const sourceConnectionConfig = await this.connectionService.getDecryptedConnection(
        pipeline.organizationId,
        sourceSchema.dataSourceId!,
        userId,
      );
      const destConnectionConfig = await this.connectionService.getDecryptedConnection(
        pipeline.organizationId,
        destinationSchema.dataSourceId!,
        userId,
      );

      const configuredWriteMode = (destinationSchema.writeMode as 'append' | 'upsert' | 'replace') || 'append';
      const configuredUpsertKey = (destinationSchema.upsertKey as string[]) || undefined;
      const effectiveWriteMode =
        configuredUpsertKey && configuredUpsertKey.length > 0 ? 'upsert' : configuredWriteMode;

      const syncResult = await this.pythonETLService.runSync({
        jobId: runId,
        pipelineId: pipeline.id,
        organizationId: pipeline.organizationId,
        sourceSchema,
        destinationSchema,
        sourceConnectionConfig,
        destConnectionConfig,
        userId,
        syncMode: syncType as 'full' | 'incremental' | 'cdc',
        writeMode: effectiveWriteMode,
        upsertKey: configuredUpsertKey,
        cursorField: pipeline.incrementalColumn || undefined,
        checkpoint: checkpoint || undefined,
        columnMap: this.normalizeColumnMap(pipeline.transformations),
      });

      totalRowsWritten = syncResult.rowsSynced;
      totalRowsRead = syncResult.rowsSynced;
      if (syncResult.error) {
        throw new Error(syncResult.userMessage || syncResult.error);
      }
      if (syncResult.newState) {
        await this.lifecycleService.saveCheckpoint(pipeline.id, syncResult.newState, userId);
      }

      if (this.pipelineQueueService.isReady()) {
        await this.pipelineQueueService.publishStatusUpdate({
          pipelineId: pipeline.id,
          organizationId: pipeline.organizationId,
          status: 'running',
          rowsProcessed: totalRowsWritten,
          newRowsCount: totalRowsWritten,
          timestamp: new Date().toISOString(),
        });
      }

      // Update progress in database
      await this.pipelineRepository.updateRun(runId, {
        rowsRead: totalRowsRead,
        rowsWritten: totalRowsWritten,
        rowsSkipped: totalRowsSkipped,
        rowsFailed: totalRowsFailed,
      });

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
        pipeline.syncMode === 'incremental' || pipeline.syncMode === 'cdc'
          ? PipelineStatus.LISTING
          : PipelineStatus.IDLE;

      // Update totalRowsProcessed - cumulative across all runs
      const newTotalRowsProcessed = (pipeline.totalRowsProcessed || 0) + totalRowsWritten;

      // Calculate next scheduled run time based on pipeline schedule configuration
      // Default to 2 minutes for CDC/incremental polling if no schedule configured
      let nextScheduledRunAt: Date | null = null;
      const scheduleType = pipeline.scheduleType || 'none';
      const scheduleValue = pipeline.scheduleValue || '';

      if (scheduleType !== 'none') {
        nextScheduledRunAt = this.calculateNextScheduledRun(scheduleType, scheduleValue);
      } else if (pipeline.syncMode === 'incremental' || pipeline.syncMode === 'cdc') {
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
      this.activity.info('pipeline.completed', `Pipeline completed: ${pipeline.name}`, {
        pipelineId: pipeline.id,
        runId,
        organizationId: pipeline.organizationId,
        userId,
        metadata: {
          syncType,
          rowsRead: totalRowsRead,
          rowsWritten: totalRowsWritten,
          rowsSkipped: totalRowsSkipped,
          rowsFailed: totalRowsFailed,
          durationSeconds,
          finalStatus: targetStatus,
        },
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
      const rawErrorMessage = error instanceof Error ? error.message : String(error);
      // Truncate to prevent DB column overflow / excessively long query params
      const errorMessage = rawErrorMessage.length > 2000 ? rawErrorMessage.substring(0, 2000) : rawErrorMessage;
      const rawStack = error instanceof Error ? error.stack : undefined;
      const errorStack = rawStack && rawStack.length > 4000 ? rawStack.substring(0, 4000) : rawStack;

      try {
        await this.pipelineRepository.updateRun(runId, {
          status: 'failed',
          jobState: 'failed',
          completedAt: new Date(),
          durationSeconds,
          errorMessage,
          errorStack,
        });
      } catch (dbError) {
        this.logger.error(`Failed to persist run error for ${runId}: ${dbError}`);
      }

      try {
        await this.pipelineRepository.update(pipeline.id, {
          lastRunAt: new Date(),
          lastRunStatus: 'failed',
          lastError: errorMessage.length > 1000 ? errorMessage.substring(0, 1000) : errorMessage,
          totalRunsFailed: (pipeline.totalRunsFailed || 0) + 1,
        });
      } catch (dbError) {
        this.logger.error(`Failed to persist pipeline error for ${pipeline.id}: ${dbError}`);
      }

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

    // dlt handles transform via schema hints — no customSql/dbt required

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
   * Dry run pipeline (preview source data without writing — dlt-based)
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

    const { pipeline, sourceSchema } = pipelineWithSchemas;

    await this.checkPipelineViewPermission(userId, pipeline.organizationId);

    const sourceConnectionConfig = await this.connectionService.getDecryptedConnection(
      pipeline.organizationId,
      sourceSchema.dataSourceId!,
      userId,
    );

    const preview = await this.pythonETLService.preview({
      sourceSchema,
      connectionConfig: sourceConnectionConfig,
      limit: sampleSize,
    });

    if (preview.records.length > 0) {
      this.logger.log(
        `Dry run preview sample: ${JSON.stringify(preview.records[0], null, 2)}`,
      );
    }

    return {
      wouldWrite: preview.total,
      sourceRowCount: preview.total,
      sampleRows: preview.records,
      transformedSample: preview.records,
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
   * Execute bidirectional pipeline asynchronously (dlt runSync)
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
    let totalRowsWritten = 0;

    try {
      await this.pipelineRepository.updateRun(runId, {
        status: 'running',
        jobState: 'running',
      });

      const sourceConnectionConfig = await this.connectionService.getDecryptedConnection(
        pipeline.organizationId,
        sourceSchema.dataSourceId!,
        userId,
      );
      const destConnectionConfig = await this.connectionService.getDecryptedConnection(
        pipeline.organizationId,
        destinationSchema.dataSourceId!,
        userId,
      );

      const configuredWriteMode = (destinationSchema.writeMode as 'append' | 'upsert' | 'replace') || 'append';
      const configuredUpsertKey = (destinationSchema.upsertKey as string[]) || options?.upsertKeys?.default;
      const effectiveWriteMode =
        configuredUpsertKey && configuredUpsertKey.length > 0 ? 'upsert' : configuredWriteMode;

      this.activity.info('sync.collect', `Syncing from ${sourceSchemaInfo.sourceType} → ${destSchemaInfo.sourceType}`, {
        pipelineId: pipeline.id,
        runId,
      });

      const syncResult = await this.pythonETLService.runSync({
        jobId: runId,
        pipelineId: pipeline.id,
        organizationId: pipeline.organizationId,
        sourceSchema,
        destinationSchema,
        sourceConnectionConfig,
        destConnectionConfig,
        userId,
        syncMode: 'full',
        writeMode: effectiveWriteMode,
        upsertKey: configuredUpsertKey,
        cursorField: pipeline.incrementalColumn || undefined,
        columnMap: this.normalizeColumnMap(pipeline.transformations),
      });

      totalRowsWritten = syncResult.rowsSynced;
      if (syncResult.error) {
        throw new Error(syncResult.userMessage || syncResult.error);
      }

      const duration = Date.now() - startTime;
      this.activity.info(
        'pipeline.completed',
        `Bidirectional pipeline completed: ${totalRowsWritten} rows written in ${(duration / 1000).toFixed(1)}s`,
        {
          pipelineId: pipeline.id,
          runId,
          organizationId: pipeline.organizationId,
          metadata: { totalRowsWritten, durationMs: duration },
        },
      );

      // Update run as success
      await this.pipelineRepository.updateRun(runId, {
        status: 'success',
        jobState: 'completed',
        rowsRead: totalRowsWritten,
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

      this.activity.error('pipeline.failed', `Bidirectional pipeline failed: ${errorMessage}`, {
        pipelineId: pipeline.id,
        runId,
        organizationId: pipeline.organizationId,
        metadata: { errorMessage, totalRowsWritten, durationMs: duration },
      });

      // Update run as failed
      await this.pipelineRepository.updateRun(runId, {
        status: 'failed',
        jobState: 'failed',
        rowsRead: totalRowsWritten,
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

  /**
   * Normalize transformations to dlt column_map format [{ from_col, to_col }]
   */
  private normalizeColumnMap(
    transformations: unknown,
  ): Array<{ from_col: string; to_col: string }> | undefined {
    if (!transformations || !Array.isArray(transformations)) return undefined;
    const mapped = transformations
      .map((t: any) => {
        if (t?.from_col && t?.to_col) return { from_col: t.from_col, to_col: t.to_col };
        if (t?.sourceColumn && t?.destinationColumn)
          return { from_col: t.sourceColumn, to_col: t.destinationColumn };
        return null;
      })
      .filter((x): x is { from_col: string; to_col: string } => x !== null);
    return mapped.length > 0 ? mapped : undefined;
  }
}
