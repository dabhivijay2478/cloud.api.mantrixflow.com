/**
 * Pipeline Service
 * Main orchestration service for managing data pipelines.
 * All sync execution is dispatched via PGMQ to Singer-based ETL pods.
 * Results come back asynchronously via POST /internal/etl-callback.
 */

import {
  BadRequestException,
  ForbiddenException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { ActivityLoggerService } from '../../../common/logger';
import {
  areSourceDbMutationsAllowed,
  SOURCE_DB_MUTATION_POLICY_MESSAGE,
} from '../../../common/utils/source-db-mutation-policy';
import type {
  Pipeline,
  PipelineDestinationSchema,
  PipelineRun,
  PipelineSourceSchema,
} from '../../../database/schemas';
import type { DiscoveredColumn } from '../../../database/schemas/data-pipelines/source-schemas/pipeline-source-schemas.schema';
import { ActivityLogService } from '../../activity-logs/activity-log.service';
import {
  PIPELINE_ACTIONS,
  PIPELINE_RUN_ACTIONS,
} from '../../activity-logs/constants/activity-log-types';
import { ConnectionService } from '../../data-sources/connection.service';
import { DataSourceRepository } from '../../data-sources/repositories/data-source.repository';
import { DataSourceConnectionRepository } from '../../data-sources/repositories/data-source-connection.repository';
import { OrganizationRoleService } from '../../organizations/services/organization-role.service';
import { PgmqQueueService } from '../../queue';
import type { CreatePipelineDto, UpdatePipelineDto } from '../dto';
import { ScheduleType } from '../dto/create-pipeline.dto';
import { PipelineRepository } from '../repositories/pipeline.repository';
import { PipelineDestinationSchemaRepository } from '../repositories/pipeline-destination-schema.repository';
import { PipelineSourceSchemaRepository } from '../repositories/pipeline-source-schema.repository';
import type {
  BatchOptions,
  DryRunResult,
  SchemaValidationResult,
  Transformation,
  ValidationResult,
} from '../types/common.types';
import {
  checkTypeCompatibility,
  singerTypeToPgType,
  validateSingerVsDestination,
} from '../types/common.types';
import { PipelineStatus } from '../types/pipeline-lifecycle.types';
import { parseTransformOutputMappings } from '../utils/transform-parser';
import { DestinationSchemaService } from './destination-schema.service';
import { PipelineLifecycleService } from './pipeline-lifecycle.service';
import { PipelineSchedulerService } from './pipeline-scheduler.service';
import { PythonETLService } from './python-etl.service';

/**
 * Internal DTO for creating pipelines (with organizationId and userId)
 */
export interface CreatePipelineInput extends CreatePipelineDto {
  organizationId: string;
  userId: string;
}

@Injectable()
export class PipelineService {
  private readonly logger = new Logger(PipelineService.name);

  constructor(
    private readonly pipelineRepository: PipelineRepository,
    private readonly sourceSchemaRepository: PipelineSourceSchemaRepository,
    private readonly destinationSchemaRepository: PipelineDestinationSchemaRepository,
    private readonly dataSourceRepository: DataSourceRepository,
    private readonly connectionRepository: DataSourceConnectionRepository,
    private readonly pythonETLService: PythonETLService,
    private readonly activityLogService: ActivityLogService,
    private readonly roleService: OrganizationRoleService,
    private readonly lifecycleService: PipelineLifecycleService,
    private readonly schedulerService: PipelineSchedulerService,
    private readonly pipelineQueueService: PgmqQueueService,
    private readonly connectionService: ConnectionService,
    private readonly activity: ActivityLoggerService,
    private readonly destinationSchemaService: DestinationSchemaService,
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

    // --- BLOCKING: Singer vs destination type validation ---
    // When the destination table already exists, target-postgres (with
    // allow_column_alter=False) will crash on any type mismatch.  We
    // detect this upfront so the user gets a clear 400 error.
    if (destinationSchema.destinationTableExists && sourceSchema.discoveredColumns) {
      await this.validateSingerVsDestinationTypes(sourceSchema, destinationSchema, organizationId);
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
   * Run pipeline — creates a run record and enqueues to PGMQ.
   * All execution happens via the ETL pod pool (no in-process execution).
   */
  async runPipeline(
    pipelineId: string,
    userId: string,
    triggerType: 'manual' | 'scheduled' | 'api' | 'polling' = 'manual',
    _options?: BatchOptions,
  ): Promise<PipelineRun> {
    const pipelineWithSchemas = await this.pipelineRepository.findByIdWithSchemas(pipelineId);
    if (!pipelineWithSchemas) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    const { pipeline } = pipelineWithSchemas;

    if (triggerType === 'manual' || triggerType === 'api') {
      await this.checkPipelineManagePermission(userId, pipeline.organizationId);
    }

    if (pipeline.status === 'paused') {
      throw new BadRequestException('Pipeline is paused. Resume it before running.');
    }

    // CDC verification guard: block LOG_BASED/INCREMENTAL_SYNC when CDC not verified
    const isCdcOrLogBased = pipeline.syncMode === 'cdc' || pipeline.syncMode === 'log_based';
    if (isCdcOrLogBased && !areSourceDbMutationsAllowed()) {
      throw new BadRequestException(SOURCE_DB_MUTATION_POLICY_MESSAGE);
    }
    const needsInitialSync = isCdcOrLogBased && !pipeline.fullRefreshCompletedAt;
    if (isCdcOrLogBased && !needsInitialSync) {
      const sourceSchema = pipelineWithSchemas.sourceSchema;
      if (sourceSchema?.dataSourceId) {
        const connection = await this.connectionRepository.findByDataSourceId(
          sourceSchema.dataSourceId,
        );
        const cdcStatus = connection?.cdcPrerequisitesStatus as
          | { overall?: string }
          | null
          | undefined;
        if (cdcStatus?.overall !== 'verified') {
          throw new BadRequestException(
            'Complete the Log-Based Sync setup for this source connection before running.',
          );
        }
      }
    }

    // Create run record (pending — ETL callback will update it)
    const run = await this.pipelineRepository.createRun({
      pipelineId,
      organizationId: pipeline.organizationId,
      status: 'pending',
      jobState: 'pending',
      triggerType,
      triggeredBy: userId,
      startedAt: new Date(),
    });

    await this.pipelineRepository.update(pipelineId, {
      lastRunStatus: 'running',
      lastRunAt: new Date(),
    });

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

    // Enqueue to PGMQ — PipelineJobProcessor will dequeue and dispatch to ETL pod
    // Route by syncMode: full → FullSync; cdc/log_based → FullSync if no initial sync yet, else IncrementalSync
    if (this.pipelineQueueService.isReady()) {
      const isCdcOrLogBased = pipeline.syncMode === 'cdc' || pipeline.syncMode === 'log_based';
      const needsInitialSync = isCdcOrLogBased && !pipeline.fullRefreshCompletedAt;
      const isIncrementalCursor = pipeline.syncMode === 'incremental';

      if (isCdcOrLogBased && needsInitialSync) {
        // "Run Initial Sync" — enqueue FULL_SYNC (FULL_TABLE)
        await this.pipelineQueueService.enqueueFullSync({
          pipelineId,
          runId: run.id,
          organizationId: pipeline.organizationId,
          userId,
          triggerType,
        });
        this.logger.log(
          `Pipeline ${pipelineId} run ${run.id} enqueued to FULL_SYNC (Run Initial Sync)`,
        );
      } else if (isCdcOrLogBased || isIncrementalCursor) {
        await this.pipelineQueueService.enqueueIncrementalSync({
          pipelineId,
          runId: run.id,
          organizationId: pipeline.organizationId,
          userId,
          triggerType,
        });
        this.logger.log(`Pipeline ${pipelineId} run ${run.id} enqueued to INCREMENTAL_SYNC`);
      } else {
        await this.pipelineQueueService.enqueueFullSync({
          pipelineId,
          runId: run.id,
          organizationId: pipeline.organizationId,
          userId,
          triggerType,
        });
        this.logger.log(`Pipeline ${pipelineId} run ${run.id} enqueued to PGMQ`);
      }
    } else {
      this.logger.warn('PGMQ not ready — run will wait for processor to pick it up');
    }

    // Publish pending status via Socket.io
    if (this.pipelineQueueService.isReady()) {
      await this.pipelineQueueService.publishStatusUpdate({
        pipelineId: pipeline.id,
        organizationId: pipeline.organizationId,
        status: 'pending',
        rowsProcessed: 0,
        timestamp: new Date().toISOString(),
      });
    }

    return run;
  }

  /**
   * Get sync state (cursor/LSN/binlog) for incremental/CDC pipelines.
   * NestJS owns state — stored in pipeline.checkpoint.
   */
  async getSyncState(
    pipelineId: string,
    organizationId: string,
    userId: string,
  ): Promise<{
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
  async resetSyncState(
    pipelineId: string,
    organizationId: string,
    userId: string,
  ): Promise<{
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

  // executePipelineAsync removed — all execution dispatched via PGMQ to ETL pods.
  // Results come back via POST /internal/etl-callback.

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

    const { pipeline, sourceSchema, destinationSchema } = pipelineWithSchemas;

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
      destinationSchema: destinationSchema ?? undefined,
    });

    if (preview.records.length > 0) {
      this.logger.log(`Dry run preview sample: ${JSON.stringify(preview.records[0], null, 2)}`);
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
  // TRANSFORMATION VALIDATION
  // ============================================================================

  /**
   * Validate cast transformations against source column types.
   * Returns a list of warning/error messages for unsafe casts.
   */
  private validateCastTransformations(
    transformations: Transformation[],
    sourceColumns: DiscoveredColumn[],
  ): string[] {
    const messages: string[] = [];
    const colMap = new Map(sourceColumns.map((c) => [c.name.toLowerCase(), c]));

    for (const t of transformations) {
      if (t.transformType !== 'cast') continue;
      const targetType = t.transformConfig?.targetType;
      if (!targetType) continue;

      const src = colMap.get(t.sourceColumn.toLowerCase());
      if (!src) {
        messages.push(`Cast on "${t.sourceColumn}": source column not found in discovered schema`);
        continue;
      }

      const compat = checkTypeCompatibility(src.dataType, targetType);
      if (compat === 'cross_family_unsafe') {
        messages.push(
          `Cast on "${t.sourceColumn}": ${src.dataType} -> ${targetType} is unsafe and may cause data loss`,
        );
      } else if (compat === 'unsafe_narrowing') {
        messages.push(
          `Cast on "${t.sourceColumn}": ${src.dataType} -> ${targetType} is a narrowing conversion — potential overflow`,
        );
      } else if (compat === 'unknown') {
        messages.push(
          `Cast on "${t.sourceColumn}": ${src.dataType} -> ${targetType} — verify compatibility`,
        );
      }
    }

    return messages;
  }

  // ============================================================================
  // SINGER VS DESTINATION VALIDATION
  // ============================================================================

  /**
   * Compare Singer source types against the real PG types of an existing
   * destination table.  With target-postgres allow_column_alter=False, any
   * type mismatch causes a fatal crash at sync time.  This method detects
   * mismatches upfront and throws BadRequestException with actionable guidance.
   */
  private async validateSingerVsDestinationTypes(
    sourceSchema: PipelineSourceSchema,
    destinationSchema: PipelineDestinationSchema,
    organizationId: string,
  ): Promise<void> {
    const discoveredColumns = sourceSchema.discoveredColumns as DiscoveredColumn[] | null;
    if (!discoveredColumns || discoveredColumns.length === 0) return;

    const destTable = destinationSchema.destinationTable;
    const destSchemaName = destinationSchema.destinationSchema || 'public';
    if (!destTable) return;

    let destConnectionConfig: any;
    try {
      destConnectionConfig = await this.connectionService.getDecryptedConnection(
        organizationId,
        destinationSchema.dataSourceId,
        'system',
      );
    } catch {
      this.logger.warn('Could not decrypt dest connection — skipping type validation');
      return;
    }

    let introspected: Awaited<ReturnType<PythonETLService['introspectTable']>>;
    try {
      introspected = await this.pythonETLService.introspectTable({
        connectionConfig: destConnectionConfig,
        schemaName: destSchemaName,
        tableName: destTable,
      });
    } catch (err: any) {
      this.logger.warn(`Destination introspection failed (non-blocking): ${err?.message}`);
      return;
    }

    if (!introspected.columns || introspected.columns.length === 0) return;

    let singerCols: Array<{ name: string; type: string }>;

    const transformScript = destinationSchema.transformScript;
    if (transformScript) {
      const mappings = parseTransformOutputMappings(transformScript);
      const srcMap = new Map(discoveredColumns.map((c) => [c.name.toLowerCase(), c]));
      singerCols = [];
      for (const [outCol, srcCol] of mappings) {
        const src = srcMap.get(srcCol.toLowerCase());
        const singerType = src?.dataType ?? 'string';
        singerCols.push({ name: outCol, type: singerType });
      }
      if (singerCols.length === 0) {
        singerCols = discoveredColumns.map((c) => ({
          name: c.name,
          type: c.dataType,
        }));
      }
    } else {
      singerCols = discoveredColumns.map((c) => ({
        name: c.name,
        type: c.dataType,
      }));
    }

    const { errors, mismatches } = validateSingerVsDestination(singerCols, introspected.columns);

    if (errors.length > 0) {
      this.logger.error(
        `Pipeline blocked: ${mismatches.length} type mismatch(es) in ${destSchemaName}.${destTable}`,
      );
      throw new BadRequestException(
        `Destination table "${destSchemaName}.${destTable}" has incompatible column types:\n` +
          errors.join('\n'),
      );
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
