/**
 * Pipeline Service
 * Main orchestration service for managing data pipelines
 * Uses Meltano run-meltano-pipeline for data movement (Clean Engine)
 */

const SUPPORTED_DIRECTIONS = [
  'postgres-to-mongodb',
  'mongodb-to-postgres',
  'postgres-to-postgres',
  'mysql-to-postgres',
] as const;

type MeltanoDirection = (typeof SUPPORTED_DIRECTIONS)[number];

function getDirectionForPipeline(
  sourceType: string,
  destType: string,
): MeltanoDirection | null {
  const s = sourceType?.toLowerCase();
  const d = destType?.toLowerCase();
  if (s === 'postgresql' && d === 'mongodb') return 'postgres-to-mongodb';
  if (s === 'mongodb' && d === 'postgresql') return 'mongodb-to-postgres';
  if (s === 'postgresql' && d === 'postgresql') return 'postgres-to-postgres';
  if ((s === 'mysql' || s === 'mariadb') && d === 'postgresql') return 'mysql-to-postgres';
  return null;
}

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
} from '../types/common.types';
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
   * Get pipelines by organization with pagination
   */
  async findByOrganizationPaginated(
    organizationId: string,
    userId: string | undefined,
    limit: number = 20,
    offset: number = 0,
  ) {
    if (userId) {
      await this.checkPipelineViewPermission(userId, organizationId);
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

      // Get source and dest types, map to Meltano direction
      const sourceDataSource = await this.dataSourceRepository.findById(sourceSchema.dataSourceId);
      const destDataSource = await this.dataSourceRepository.findById(destinationSchema.dataSourceId);
      if (!sourceDataSource || !destDataSource) {
        throw new BadRequestException('Source or destination data source not found');
      }

      const direction = getDirectionForPipeline(
        sourceDataSource.sourceType,
        destDataSource.sourceType,
      );
      if (!direction) {
        throw new BadRequestException(
          `Pipeline direction not supported. Supported: ${SUPPORTED_DIRECTIONS.join(', ')}. ` +
            `Your pipeline: ${sourceDataSource.sourceType} → ${destDataSource.sourceType}`,
        );
      }

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

      const effectiveWriteMode =
        (destinationSchema.writeMode as 'append' | 'upsert' | 'replace') || 'upsert';
      const effectiveUpsertKey = (destinationSchema.upsertKey as string[]) || [];

      const dbtModels = (destinationSchema.dbtModels as string[] | null) ?? undefined;
      const result = await this.pythonETLService.runMeltanoPipeline({
        direction,
        sourceConnectionConfig,
        destConnectionConfig,
        sourceTable: sourceSchema.sourceTable || undefined,
        sourceSchema: sourceSchema.sourceSchema || 'public',
        destTable: destinationSchema.destinationTable || undefined,
        destSchema: destinationSchema.destinationSchema || 'public',
        syncMode: syncType,
        writeMode: effectiveWriteMode,
        upsertKey: effectiveUpsertKey.length > 0 ? effectiveUpsertKey : undefined,
        stateId: `pipeline_${pipeline.id}`,
        checkpoint: checkpoint || undefined,
        limit: undefined,
        dbtModels: dbtModels?.length ? dbtModels : undefined,
      });

      totalRowsRead = result.rowsRead;
      totalRowsWritten = result.rowsWritten;
      totalRowsSkipped = result.rowsSkipped;
      totalRowsFailed = result.rowsFailed;
      if (result.errors?.length) {
        allErrors.push(...result.errors);
      }

      await this.lifecycleService.saveCheckpoint(
        pipeline.id,
        {
          ...(checkpoint || {}),
          ...result.checkpoint,
          rowsProcessed: result.rowsWritten,
          lastSyncAt: new Date().toISOString(),
        },
        userId,
      );

      if (this.pipelineQueueService.isReady()) {
        await this.pipelineQueueService.publishStatusUpdate({
          pipelineId: pipeline.id,
          organizationId: pipeline.organizationId,
          status: 'running',
          rowsProcessed: result.rowsWritten,
          newRowsCount: result.rowsWritten,
          timestamp: new Date().toISOString(),
        });
      }

      const duration = Date.now() - startTime;
      this.activity.info(
        'pipeline.completed',
        `Pipeline completed: ${result.rowsWritten} rows written in ${(duration / 1000).toFixed(1)}s`,
        {
          pipelineId: pipeline.id,
          runId,
          organizationId: pipeline.organizationId,
          metadata: {
            totalRowsRead: result.rowsRead,
            totalRowsWritten: result.rowsWritten,
            durationMs: duration,
          },
        },
      );

      await this.pipelineRepository.updateRun(runId, {
        status: 'success',
        jobState: 'completed',
        rowsRead: result.rowsRead,
        rowsWritten: result.rowsWritten,
        rowsSkipped: result.rowsSkipped,
        rowsFailed: result.rowsFailed,
        completedAt: new Date(),
        durationSeconds: Math.floor(duration / 1000),
      });

      await this.pipelineRepository.update(pipeline.id, {
        lastRunStatus: 'success',
        lastRunAt: new Date(),
      });

      await this.lifecycleService.markCompleted(pipeline.id, userId, {
        rowsProcessed: result.rowsWritten,
        durationSeconds: Math.floor(duration / 1000),
      });
    } catch (error) {
      const duration = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.logger.error(`Pipeline execution failed: ${errorMessage}`, error instanceof Error ? error.stack : undefined);

      await this.pipelineRepository.updateRun(runId, {
        status: 'failed',
        jobState: 'failed',
        rowsRead: totalRowsRead,
        rowsWritten: totalRowsWritten,
        rowsSkipped: totalRowsSkipped,
        rowsFailed: totalRowsFailed,
        completedAt: new Date(),
        durationSeconds: Math.floor(duration / 1000),
        errorMessage: errorMessage,
      });

      await this.pipelineRepository.update(pipeline.id, {
        lastRunStatus: 'failed',
        lastRunAt: new Date(),
      });

      await this.lifecycleService.markFailed(pipeline.id, userId, errorMessage);

      await this.activityLogService.logPipelineRunAction(
        pipeline.organizationId,
        userId,
        PIPELINE_RUN_ACTIONS.FAILED,
        runId,
        pipeline.id,
        pipeline.name,
        { error: errorMessage },
      );

      if (this.pipelineQueueService.isReady()) {
        await this.pipelineQueueService.publishStatusUpdate({
          pipelineId: pipeline.id,
          organizationId: pipeline.organizationId,
          status: 'failed',
          rowsProcessed: totalRowsWritten,
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

    // Transform script deprecated - Meltano uses dbt

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
   * Collects sample data only; transformations use dbt in Meltano pipeline.
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

    const sourceData = await this.pythonETLService.collect({
      sourceSchema,
      connectionConfig: sourceConnectionConfig,
      organizationId: pipeline.organizationId,
      userId,
      limit: sampleSize,
    });

    return {
      wouldWrite: sourceData.rows.length,
      sourceRowCount: sourceData.totalRows,
      sampleRows: sourceData.rows,
      transformedSample: sourceData.rows,
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
