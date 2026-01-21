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
import type { SchemaInfo } from '../types/source-handler.types';
import { PipelineStatus, PipelineCheckpoint } from '../types/pipeline-lifecycle.types';
import { PipelineRepository } from '../repositories/pipeline.repository';
import { PipelineSourceSchemaRepository } from '../repositories/pipeline-source-schema.repository';
import { PipelineDestinationSchemaRepository } from '../repositories/pipeline-destination-schema.repository';
import type { CreatePipelineDto, UpdatePipelineDto } from '../dto';
import { ScheduleType } from '../dto/create-pipeline.dto';
import { PipelineSchedulerService } from './pipeline-scheduler.service';

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
    private readonly collectorService: CollectorService,
    private readonly transformerService: TransformerService,
    private readonly emitterService: EmitterService,
    private readonly activityLogService: ActivityLogService,
    private readonly roleService: OrganizationRoleService,
    private readonly lifecycleService: PipelineLifecycleService,
    private readonly schedulerService: PipelineSchedulerService,
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
        
        this.logger.log(`Pipeline ${pipeline.id} scheduled: ${this.schedulerService.getHumanReadableSchedule(scheduleType, scheduleValue, scheduleTimezone)}`);
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
    const scheduleType = (updates.scheduleType as ScheduleType) || (pipeline.scheduleType as ScheduleType) || ScheduleType.NONE;
    const scheduleValue = updates.scheduleValue !== undefined ? updates.scheduleValue : pipeline.scheduleValue;
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
          this.logger.log(`Pipeline ${id} rescheduled: ${this.schedulerService.getHumanReadableSchedule(scheduleType, scheduleValue || undefined, scheduleTimezone)}`);
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

    // ROOT FIX: Always check checkpoint existence and sync_mode to determine sync type
    // This fixes "always full sync" issue by enforcing conditional logic
    let checkpoint = await this.lifecycleService.getCheckpoint(pipeline.id);
    
    // Determine sync type based on checkpoint existence and sync_mode
    // CRITICAL: If checkpoint exists AND syncMode is incremental AND not paused/failed, force incremental
    let isFullSync: boolean;
    let syncReason: string;
    
    // Check if pipeline was paused (has pauseTimestamp in checkpoint or pipeline)
    const pauseTimestamp = checkpoint?.pauseTimestamp || 
      (pipeline.status === 'paused' ? pipeline.updatedAt?.toISOString() : undefined);
    
    if (pipeline.syncMode === 'full') {
      // Explicit full sync mode - always full
      isFullSync = true;
      syncReason = 'syncMode is explicitly set to "full"';
    } else if (pipeline.syncMode === 'incremental') {
      // ROOT FIX: Check checkpoint existence FIRST
      if (checkpoint && checkpoint.lastSyncValue && pipeline.incrementalColumn) {
        // Checkpoint exists with lastSyncValue - MUST do incremental (unless explicitly forced full)
        isFullSync = false;
        syncReason = `incremental sync: checkpoint exists with lastSyncValue=${checkpoint.lastSyncValue}`;
        this.logger.log(`Pipeline ${pipeline.id}: Using incremental sync (checkpoint found)`);
      } else if (!pipeline.incrementalColumn) {
        // No incremental column configured - fallback to full
        isFullSync = true;
        syncReason = 'incremental mode but no incrementalColumn configured - falling back to full';
        this.logger.warn(`Pipeline ${pipeline.id}: ${syncReason}`);
      } else {
        // First run (no checkpoint) - do full sync, will switch to incremental after
        isFullSync = true;
        syncReason = 'first run (no checkpoint) - initial full sync before incremental';
        this.logger.log(`Pipeline ${pipeline.id}: First run - performing full sync`);
      }
    } else {
      // Default/unknown mode - treat as full
      isFullSync = true;
      syncReason = `unknown syncMode "${pipeline.syncMode}" - defaulting to full`;
      this.logger.warn(`Pipeline ${pipeline.id}: ${syncReason}`);
    }
    
    const syncType = isFullSync ? 'full' : 'incremental';
    
    // Get effective last sync value for incremental queries
    const effectiveLastSyncValue = checkpoint?.lastSyncValue || pipeline.lastSyncValue;
    const watermarkField = pipeline.incrementalColumn || checkpoint?.watermarkField;

    // Log startup with detailed sync info
    console.log(`\n${'='.repeat(60)}`);
    console.log(`🚀 STARTING PIPELINE: ${pipeline.name}`);
    console.log(`${'='.repeat(60)}`);
    console.log(`   Run ID:          ${runId}`);
    console.log(`   Sync Mode:       ${pipeline.syncMode}`);
    console.log(`   Sync Type:       ${syncType.toUpperCase()}`);
    console.log(`   Sync Reason:     ${syncReason}`);
    console.log(`   Batch Size:      ${options?.batchSize || DEFAULT_BATCH_SIZE}`);
    console.log(`   Source:          ${sourceSchema.sourceType} - ${sourceSchema.sourceTable || 'query'}`);
    if (pipeline.incrementalColumn) {
      console.log(`   Incremental Col: ${pipeline.incrementalColumn}`);
    }
    if (effectiveLastSyncValue && !isFullSync) {
      console.log(`   Last Sync Value: ${effectiveLastSyncValue}`);
    }
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
      // For full sync, clear checkpoint to start fresh
      // ROOT FIX: Only clear checkpoint if explicitly doing full sync
      if (isFullSync && checkpoint) {
        this.logger.log('Full sync detected - clearing checkpoint to start from beginning');
        await this.lifecycleService.clearCheckpoint(pipeline.id, userId);
        checkpoint = null; // Reset checkpoint reference
      }
      
      // For incremental sync, ensure we have required fields
      if (!isFullSync && (!watermarkField || !effectiveLastSyncValue)) {
        this.logger.warn('Incremental sync requested but missing watermarkField or lastSyncValue - falling back to full');
        isFullSync = true;
        syncReason = 'incremental sync failed validation - missing watermarkField or lastSyncValue';
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

      // Get column mappings and enhance with auto-detected transformations
      let columnMappings = (destinationSchema.columnMappings as ColumnMapping[]) || [];
      
      // Auto-enhance mappings for MongoDB ObjectId -> UUID conversion
      // This ensures _id fields are automatically converted to UUIDs
      if (sourceSchema.sourceType === 'mongodb') {
        columnMappings = this.transformerService.enhanceColumnMappings(columnMappings);
      }
      
      const transformations = (pipeline.transformations as any[]) || [];

      // Log applied mappings for visibility
      if (columnMappings.length > 0) {
        const mappedFields = this.transformerService.getMappedFieldsList(columnMappings);
        // Get destination type from data source
        const destDataSource = await this.dataSourceRepository.findById(destinationSchema.dataSourceId);
        const destType = destDataSource?.sourceType || 'unknown';
        this.logger.log(
          `Mapping applied to ${sourceSchema.sourceType}→${destType}: ${mappedFields.length} fields transformed`,
        );
        this.logger.log(
          `Mapped fields: ${mappedFields.map((f) => `${f.sourcePath} → ${f.destPath}`).join(', ')}`,
        );
      }

      // Collect data with batching
      let hasMore = true;
      // For full sync, always start from offset 0 (ignore checkpoint offset)
      // For incremental sync, use checkpoint offset to resume
      let offset = isFullSync ? 0 : (checkpoint?.offset || 0);
      let cursor: string | undefined = isFullSync ? undefined : checkpoint?.cursor;

      // Log checkpoint restoration if applicable (only for incremental sync)
      if (!isFullSync && checkpoint?.rowsProcessed) {
        console.log(
          `📂 Resuming from checkpoint: ${checkpoint.rowsProcessed.toLocaleString()} rows already processed (offset: ${offset})`,
        );
        await this.activityLogService.logPipelineAction(
          pipeline.organizationId,
          userId,
          PIPELINE_ACTIONS.CHECKPOINT_SAVED,
          pipeline.id,
          pipeline.name,
          { action: 'restored', checkpoint },
        );
      } else if (isFullSync) {
        console.log(`🔄 Full sync: Starting from beginning (offset: 0)`);
      }

      while (hasMore) {
        batchCount++;
        const batchStartTime = Date.now();

        // STEP 1: Collect data from source (with retry)
        let sourceData: { rows: any[]; totalRows?: number; nextCursor?: string; hasMore?: boolean };

        console.log(`\n📦 Batch ${batchCount}: Collecting data...`);

        for (let attempt = 0; attempt < retryAttempts; attempt++) {
          try {
            // ROOT FIX: For incremental sync, use collectIncremental with strict filtering
            if (!isFullSync && watermarkField && effectiveLastSyncValue) {
              console.log(`   🔄 Incremental sync: using strict filtering where ${watermarkField} > ${effectiveLastSyncValue}`);
              
              sourceData = await this.collectorService.collectIncremental({
                sourceSchema,
                organizationId: pipeline.organizationId,
                userId,
                checkpoint: {
                  watermarkField,
                  lastValue: effectiveLastSyncValue,
                  pauseTimestamp: pauseTimestamp,
                },
                limit: batchSize,
                offset,
                cursor,
              });
            } else {
              // Full sync - collect all data
              sourceData = await this.collectorService.collect({
                sourceSchema,
                organizationId: pipeline.organizationId,
                userId,
                limit: batchSize,
                offset,
                cursor,
              });
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
        offset += sourceData.rows.length; // Use actual rows collected, not batchSize
        cursor = sourceData.nextCursor;
        
        // Debug: log pagination state
        console.log(`   🔄 Pagination: hasMore=${hasMore}, nextOffset=${offset}, cursor=${cursor || 'none'}, rowsThisBatch=${sourceData.rows.length}, batchSize=${batchSize}`);

        // Extract the max value of the incremental column from this batch
        // This is needed for BOTH full and incremental modes:
        // - Full mode: will become the starting point for next incremental run
        // - Incremental mode: updates the checkpoint for subsequent runs
        let batchMaxSyncValue: string | number | undefined;
        if (pipeline.incrementalColumn && sourceData.rows.length > 0) {
          const incrementalValues = sourceData.rows
            .map((row: any) => row[pipeline.incrementalColumn!])
            .filter((val: any) => val !== null && val !== undefined);
          if (incrementalValues.length > 0) {
            // Get the max value - works for both dates and numbers
            batchMaxSyncValue = incrementalValues.reduce((max: any, val: any) => {
              // Handle date strings, timestamps, and numbers
              if (val && typeof val === 'object' && val.constructor === Date) {
                return max && typeof max === 'object' && max.constructor === Date 
                  ? (val > max ? val : max) 
                  : val;
              }
              return val > max ? val : max;
            });
            // Convert Date to ISO string for storage
            if (batchMaxSyncValue && typeof batchMaxSyncValue === 'object' && (batchMaxSyncValue as any).constructor === Date) {
              batchMaxSyncValue = (batchMaxSyncValue as Date).toISOString();
            }
          }
        }
        
        // Track the overall max sync value across all batches
        // Compare with existing checkpoint value to ensure we always have the highest
        let overallMaxSyncValue: string | number | undefined = batchMaxSyncValue;
        if (checkpoint?.lastSyncValue && batchMaxSyncValue) {
          // Keep the higher value
          if (String(checkpoint.lastSyncValue) > String(batchMaxSyncValue)) {
            overallMaxSyncValue = checkpoint.lastSyncValue;
          }
        }
        
        if (batchMaxSyncValue) {
          console.log(`   📌 Batch max ${pipeline.incrementalColumn}: ${batchMaxSyncValue}`);
        }

        // Save checkpoint after each batch for resumability
        // ROOT FIX: Include watermarkField in checkpoint for incremental sync
        const currentCheckpoint: PipelineCheckpoint = {
          lastSyncAt: new Date().toISOString(),
          offset,
          cursor,
          rowsProcessed: totalRowsWritten,
          totalRows: estimatedTotalRows,
          currentBatch: batchCount,
          // Save the incremental sync value for next run - use overall max
          lastSyncValue: overallMaxSyncValue,
          // Save watermark field for incremental queries
          watermarkField: watermarkField || undefined,
          // Preserve pause timestamp if exists
          pauseTimestamp: pauseTimestamp || checkpoint?.pauseTimestamp,
        };

        await this.lifecycleService.saveCheckpoint(pipeline.id, currentCheckpoint, userId);
        
        // Update local checkpoint reference so subsequent batches use the updated value
        checkpoint = currentCheckpoint;

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

      // Get the final lastSyncValue from checkpoint
      const finalCheckpoint = await this.lifecycleService.getCheckpoint(pipeline.id);
      
      // Determine target status after completion
      // For incremental sync mode, transition to LISTING mode (polling)
      // For full sync mode, go to IDLE (ready for manual run or scheduling)
      const targetStatus = pipeline.syncMode === 'incremental' 
        ? PipelineStatus.LISTING 
        : PipelineStatus.IDLE;

      // Update pipeline statistics and status
      const finalLastSyncValue = finalCheckpoint?.lastSyncValue?.toString() || pipeline.lastSyncValue;
      
      // ROOT FIX: After successful full sync in incremental mode, transition to LISTING
      // This allows pg_cron polling to automatically detect and enqueue incremental syncs
      await this.pipelineRepository.update(pipeline.id, {
        lastRunAt: new Date(),
        lastRunStatus: totalRowsFailed > 0 && totalRowsWritten === 0 ? 'failed' : 'success',
        status: targetStatus, // LISTING for incremental mode, IDLE for full mode
        totalRowsProcessed: (pipeline.totalRowsProcessed || 0) + totalRowsWritten,
        totalRunsSuccessful: (pipeline.totalRunsSuccessful || 0) + (totalRowsFailed === 0 ? 1 : 0),
        totalRunsFailed:
          (pipeline.totalRunsFailed || 0) + (totalRowsFailed > 0 && totalRowsWritten === 0 ? 1 : 0),
        // Persist the lastSyncValue for incremental syncs - CRITICAL for next run
        lastSyncValue: finalLastSyncValue,
        lastSyncAt: new Date(),
        // Update checkpoint with watermarkField for incremental queries
        checkpoint: finalCheckpoint ? {
          ...finalCheckpoint,
          watermarkField: watermarkField || finalCheckpoint.watermarkField,
        } : undefined,
      });
      
      // ROOT FIX: If transitioning to LISTING mode after full sync, pg_cron will automatically
      // detect this pipeline and enqueue incremental sync jobs via PGMQ
      if (targetStatus === PipelineStatus.LISTING) {
        this.logger.log(
          `Pipeline ${pipeline.id} transitioned to LISTING mode. pg_cron will automatically poll and enqueue incremental syncs.`,
        );
      }
      
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
      if (finalLastSyncValue && pipeline.incrementalColumn) {
        console.log(`   Last Sync Value: ${finalLastSyncValue} (${pipeline.incrementalColumn})`);
        console.log(`   ➡️  Next incremental run will sync records where ${pipeline.incrementalColumn} > ${finalLastSyncValue}`);
      }
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
          lastSyncValue: finalLastSyncValue,
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
   * ROOT FIX: Store pause_timestamp in checkpoint to preserve state for resume
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
      await this.lifecycleService.saveCheckpoint(pipelineId, {
        ...checkpoint,
        pauseTimestamp,
      }, userId);
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
   * ROOT FIX: Use pause_timestamp to calculate delta since pause, preserving checkpoint
   */
  async resumePipeline(pipelineId: string, userId: string): Promise<Pipeline> {
    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    await this.checkPipelineManagePermission(userId, pipeline.organizationId);

    // Get checkpoint (which should have pauseTimestamp)
    const checkpoint = await this.lifecycleService.getCheckpoint(pipelineId);
    
    // ROOT FIX: Preserve checkpoint and pauseTimestamp for delta calculation
    // The pauseTimestamp will be used in collectIncremental to catch all changes during pause
    // No need to clear checkpoint - it will be used in next incremental sync

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
            timezone: pipeline.scheduleTimezone || 'UTC' 
          },
        );
        nextScheduledRunAt = result.nextRunAt;
        this.logger.log(`Pipeline ${pipelineId} rescheduled on resume: next run at ${nextScheduledRunAt?.toISOString()}`);
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

    this.logger.log(
      `Pipeline ${pipelineId} resumed. Checkpoint preserved with pauseTimestamp: ${checkpoint?.pauseTimestamp || 'none'}. Next incremental sync will use min(pauseTimestamp, lastSyncValue) for delta calculation.`,
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

    // Log applied mappings for visibility
    const mappedFields = this.transformerService.getMappedFieldsList(columnMappings);
    // Get destination type from data source
    const destDataSource = await this.dataSourceRepository.findById(destinationSchema.dataSourceId);
    const destType = destDataSource?.sourceType || 'unknown';
    this.logger.log(
      `Dry run: Mapping applied to ${sourceSchema.sourceType}→${destType}: ${mappedFields.length} fields`,
    );

    const transformedSample = await this.transformerService.transform(
      sourceData.rows,
      columnMappings,
      transformations,
    );

    // Log sample transformed data
    if (transformedSample.length > 0) {
      this.logger.log(`Dry run sample transformed data: ${JSON.stringify(transformedSample[0], null, 2)}`);
    }

    return {
      wouldWrite: transformedSample.length,
      sourceRowCount: sourceData.totalRows,
      sampleRows: sourceData.rows,
      transformedSample,
      errors: [],
      appliedMappings: mappedFields,
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
    const sourceSchemaInfo: SchemaInfo = {
      columns: [],
      primaryKeys: [],
      isRelational: this.transformerService.isRelationalType(sourceDataSource.sourceType),
      sourceType: sourceDataSource.sourceType,
      entityName: sourceSchema.sourceTable || undefined,
    };

    const destSchemaInfo: SchemaInfo = {
      columns: [],
      primaryKeys: [],
      isRelational: this.transformerService.isRelationalType(destDataSource.sourceType),
      sourceType: destDataSource.sourceType,
      entityName: destinationSchema.destinationTable || undefined,
    };

    this.logger.log(
      `Bidirectional pipeline: ${sourceDataSource.sourceType} (${sourceSchemaInfo.isRelational ? 'SQL' : 'NoSQL'}) → ` +
      `${destDataSource.sourceType} (${destSchemaInfo.isRelational ? 'SQL' : 'NoSQL'})`
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

      // Get column mappings
      const columnMappings = (destinationSchema.columnMappings as ColumnMapping[]) || [];

      // Validate mappings for bidirectional transform
      const validation = this.transformerService.validateBidirectional(
        columnMappings,
        sourceSchemaInfo,
        destSchemaInfo,
      );

      if (!validation.valid) {
        throw new BadRequestException(`Mapping validation failed: ${validation.errors.join(', ')}`);
      }

      if (validation.warnings?.length) {
        this.logger.warn(`Mapping warnings: ${validation.warnings.join(', ')}`);
      }

      // Collect all data (for simplicity, batching can be added later)
      console.log(`\n📦 Collecting data from ${sourceSchemaInfo.sourceType}...`);
      
      const sourceData = await this.collectorService.collect({
        sourceSchema,
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

      // Transform using bidirectional method
      console.log(`\n🔄 Transforming data (${sourceSchemaInfo.isRelational ? 'SQL' : 'NoSQL'} → ${destSchemaInfo.isRelational ? 'SQL' : 'NoSQL'})...`);
      
      const transformedData = await this.transformerService.transformBidirectional(
        sourceData.rows,
        columnMappings,
        sourceSchemaInfo,
        destSchemaInfo,
      );

      // Log transformation results
      for (const [entity, rows] of Object.entries(transformedData)) {
        console.log(`   📋 Entity '${entity}': ${rows.length} rows`);
      }

      // Emit to destination using multi-entity method
      console.log(`\n📤 Writing to ${destSchemaInfo.sourceType}...`);
      
      const writeResults = await this.emitterService.emitMultiEntity({
        destinationSchema,
        organizationId: pipeline.organizationId,
        userId,
        entityData: transformedData,
        writeMode: (destinationSchema.writeMode as 'append' | 'upsert' | 'replace') || 'append',
        upsertKeys: options?.upsertKeys,
      });

      // Calculate totals
      for (const [entity, result] of Object.entries(writeResults)) {
        totalRowsWritten += result.rowsWritten;
        console.log(`   ✓ ${entity}: ${result.rowsWritten} rows written`);
      }

      const duration = Date.now() - startTime;
      console.log(`\n✅ Pipeline completed: ${totalRowsWritten.toLocaleString()} rows written in ${(duration / 1000).toFixed(1)}s\n`);

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
}
