/**
 * PgBoss Job Handler Service
 * Registers and handles all PgBoss jobs for pipeline operations
 *
 * Job Types:
 * - full-sync: Complete data sync from source to destination
 * - incremental-sync: Sync only new/changed records (CDC)
 * - delta-check: Poll source for changes and enqueue incremental if needed
 * - status-update: Pub/sub for real-time UI updates
 *
 * ROOT FIX: Centralized job handling with proper error recovery and retry logic
 */

import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { PipelineService } from './pipeline.service';
import { PipelineRepository } from '../repositories/pipeline.repository';
import { CollectorService } from './collector.service';
import { PipelineUpdatesGateway } from '../gateways/pipeline-updates.gateway';
import {
  PgBossService,
  JOB_NAMES,
  FullSyncJobData,
  IncrementalSyncJobData,
  DeltaCheckJobData,
  StatusUpdateEventData,
} from './pgboss.service';
import type { Job } from 'pg-boss';

@Injectable()
export class PgBossJobHandlerService implements OnModuleInit {
  private readonly logger = new Logger(PgBossJobHandlerService.name);

  constructor(
    private readonly pgBossService: PgBossService,
    private readonly pipelineService: PipelineService,
    private readonly pipelineRepository: PipelineRepository,
    private readonly collectorService: CollectorService,
    private readonly pipelineUpdatesGateway: PipelineUpdatesGateway,
  ) {}

  /**
   * Register all job handlers on module init
   */
  async onModuleInit(): Promise<void> {
    this.logger.log('Registering PgBoss job handlers...');

    // Wait a bit for PgBoss to fully initialize
    await new Promise((resolve) => setTimeout(resolve, 2000));

    if (!this.pgBossService.isReady()) {
      this.logger.warn('PgBoss not ready, job handlers will be registered when ready');
      // Retry registration after delay
      setTimeout(() => this.registerHandlers(), 5000);
      return;
    }

    await this.registerHandlers();
  }

  /**
   * Register all handlers
   */
  private async registerHandlers(): Promise<void> {
    try {
      // Register job workers
      await this.pgBossService.registerWorker<FullSyncJobData>(
        JOB_NAMES.FULL_SYNC,
        this.handleFullSync.bind(this),
      );

      await this.pgBossService.registerWorker<IncrementalSyncJobData>(
        JOB_NAMES.INCREMENTAL_SYNC,
        this.handleIncrementalSync.bind(this),
      );

      await this.pgBossService.registerWorker<DeltaCheckJobData>(
        JOB_NAMES.DELTA_CHECK,
        this.handleDeltaCheck.bind(this),
      );

      // Register pub/sub subscriber for status updates
      await this.pgBossService.registerSubscriber<StatusUpdateEventData>(
        JOB_NAMES.STATUS_UPDATE,
        this.handleStatusUpdate.bind(this),
      );

      // Setup global polling cron job
      await this.pgBossService.setupGlobalPollingCron();

      // Register handler for global polling
      await this.pgBossService.registerWorker(
        'global-pipeline-polling',
        this.handleGlobalPolling.bind(this),
      );

      this.logger.log('✅ All PgBoss job handlers registered successfully');
    } catch (error) {
      this.logger.error(`Failed to register job handlers: ${error}`);
    }
  }

  // ============================================================================
  // JOB HANDLERS
  // ============================================================================

  /**
   * Handle full sync job
   * Performs complete data sync from source to destination
   */
  private async handleFullSync(job: Job<FullSyncJobData>): Promise<void> {
    const { pipelineId, organizationId, userId, triggerType, batchSize } = job.data;

    this.logger.log(`[FULL-SYNC] Starting job ${job.id} for pipeline ${pipelineId}`);
    this.logger.log(`[FULL-SYNC] Trigger: ${triggerType}, Batch Size: ${batchSize || 500}`);

    try {
      // Verify pipeline exists and is not already running
      const pipeline = await this.pipelineRepository.findById(pipelineId);
      if (!pipeline) {
        throw new Error(`Pipeline ${pipelineId} not found`);
      }

      if (pipeline.status === 'running') {
        this.logger.warn(`[FULL-SYNC] Pipeline ${pipelineId} is already running, skipping`);
        return;
      }

      // Run the pipeline
      await this.pipelineService.runPipeline(pipelineId, userId || 'system', triggerType, {
        batchSize: batchSize || 500,
      });

      this.logger.log(`[FULL-SYNC] Completed job ${job.id} for pipeline ${pipelineId}`);

      // Publish status update
      await this.pgBossService.publishStatusUpdate({
        pipelineId,
        organizationId,
        status: 'completed',
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error);
      this.logger.error(`[FULL-SYNC] Job ${job.id} failed: ${errorMsg}`);

      // Publish failure status
      await this.pgBossService.publishStatusUpdate({
        pipelineId,
        organizationId,
        status: 'failed',
        error: errorMsg,
        timestamp: new Date().toISOString(),
      });

      throw error; // Re-throw for PgBoss retry
    }
  }

  /**
   * Handle incremental sync job
   * Syncs only new/changed records since last checkpoint
   *
   * ROOT FIX: This is where CDC actually happens with proper checkpoint handling
   */
  private async handleIncrementalSync(job: Job<IncrementalSyncJobData>): Promise<void> {
    const { pipelineId, organizationId, userId, triggerType, checkpoint, batchSize } = job.data;

    this.logger.log(`[INCREMENTAL-SYNC] Starting job ${job.id} for pipeline ${pipelineId}`);
    if (checkpoint.walPosition || checkpoint.lsn) {
      this.logger.log(`[INCREMENTAL-SYNC] WAL CDC checkpoint: LSN ${checkpoint.walPosition || checkpoint.lsn}`);
    } else if (checkpoint.watermarkField && checkpoint.lastValue) {
      this.logger.log(`[INCREMENTAL-SYNC] Column-based checkpoint: ${checkpoint.watermarkField} > ${checkpoint.lastValue}`);
    }

    try {
      // Verify pipeline exists
      const pipeline = await this.pipelineRepository.findById(pipelineId);
      if (!pipeline) {
        throw new Error(`Pipeline ${pipelineId} not found`);
      }

      // Check if pipeline is in a valid state for incremental sync
      if (!['listing', 'idle', 'completed'].includes(pipeline.status || '')) {
        this.logger.warn(
          `[INCREMENTAL-SYNC] Pipeline ${pipelineId} is in ${pipeline.status} status, skipping`,
        );
        return;
      }

      // Run incremental sync
      // The pipeline service will handle the actual incremental logic
      await this.pipelineService.runPipeline(
        pipelineId,
        userId || pipeline.createdBy || 'system',
        triggerType === 'polling' ? 'polling' : triggerType === 'resume' ? 'manual' : 'scheduled',
        {
          batchSize: batchSize || 500,
        },
      );

      this.logger.log(`[INCREMENTAL-SYNC] Completed job ${job.id} for pipeline ${pipelineId}`);

      // Publish status update
      await this.pgBossService.publishStatusUpdate({
        pipelineId,
        organizationId,
        status: 'listing', // Back to listing for next poll
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error);
      this.logger.error(`[INCREMENTAL-SYNC] Job ${job.id} failed: ${errorMsg}`);

      // Publish failure status
      await this.pgBossService.publishStatusUpdate({
        pipelineId,
        organizationId,
        status: 'failed',
        error: errorMsg,
        timestamp: new Date().toISOString(),
      });

      throw error;
    }
  }

  /**
   * Handle delta check job
   * Polls source database for new records and enqueues incremental sync if found
   *
   * ROOT FIX: Automated polling that triggers incremental syncs
   */
  private async handleDeltaCheck(job: Job<DeltaCheckJobData>): Promise<void> {
    const { pipelineId, organizationId } = job.data;

    this.logger.debug(`[DELTA-CHECK] Checking pipeline ${pipelineId} for changes`);

    try {
      // Get pipeline with source schema
      const pipeline = await this.pipelineRepository.findById(pipelineId);
      if (!pipeline) {
        this.logger.warn(`[DELTA-CHECK] Pipeline ${pipelineId} not found`);
        return;
      }

      // USER REQUIREMENT: Only check pipelines in LISTING status (CDC mode)
      // No need to check syncMode or incrementalColumn - status-based detection only
      if (pipeline.status !== 'listing') {
        this.logger.debug(
          `[DELTA-CHECK] Pipeline ${pipelineId} not eligible (status: ${pipeline.status}, expected: listing)`,
        );
        return;
      }

      // ROOT FIX: Use WAL-based CDC - check LSN position, not column values
      const checkpoint = pipeline.checkpoint as any;
      const lastLSN = checkpoint?.walPosition || checkpoint?.lsn;
      const slotName = checkpoint?.slotName;

      if (!lastLSN || !slotName) {
        this.logger.debug(
          `[DELTA-CHECK] Pipeline ${pipelineId} missing WAL config (LSN: ${lastLSN}, slot: ${slotName})`,
        );
        return;
      }

      // Check for WAL changes using replication slot LSN position
      const hasChanges = await this.checkForWALChanges(
        pipelineId,
        lastLSN,
        slotName,
      );

      if (hasChanges) {
        this.logger.log(`[DELTA-CHECK] Changes detected for pipeline ${pipelineId}, enqueuing incremental sync`);

        // Enqueue incremental sync job with WAL checkpoint
        await this.pgBossService.enqueueIncrementalSync({
          pipelineId,
          organizationId,
          userId: pipeline.createdBy || 'system',
          triggerType: 'polling',
          checkpoint: {
            walPosition: lastLSN,
            lsn: lastLSN,
            slotName,
            publicationName: checkpoint?.publicationName,
          } as any, // Type assertion for WAL checkpoint
          batchSize: 500,
        });
      } else {
        this.logger.debug(`[DELTA-CHECK] No changes detected for pipeline ${pipelineId}`);
      }
    } catch (error) {
      this.logger.error(`[DELTA-CHECK] Error checking pipeline ${pipelineId}: ${error}`);
      // Don't re-throw - delta checks should fail silently and retry on next interval
    }
  }

  /**
   * Handle global polling cron job
   * Checks ALL active pipelines for changes every minute
   *
   * ROOT FIX: This replaces pg_cron's pipeline_polling_function
   */
  private async handleGlobalPolling(job: Job<{ type: string }>): Promise<void> {
    this.logger.debug('[GLOBAL-POLL] Running global pipeline polling check');

    try {
      // Find all pipelines in LISTING status with incremental mode
      const activePipelines = await this.pipelineRepository.findActivePipelinesForPolling();

      if (activePipelines.length === 0) {
        this.logger.debug('[GLOBAL-POLL] No active pipelines to poll');
        return;
      }

      this.logger.log(`[GLOBAL-POLL] Found ${activePipelines.length} pipeline(s) to check`);

      // Enqueue delta check for each pipeline
      for (const pipeline of activePipelines) {
        await this.pgBossService.enqueueDeltaCheck({
          pipelineId: pipeline.id,
          organizationId: pipeline.organizationId,
        });
      }

      this.logger.debug(`[GLOBAL-POLL] Enqueued ${activePipelines.length} delta check jobs`);
    } catch (error) {
      this.logger.error(`[GLOBAL-POLL] Error during global polling: ${error}`);
    }
  }

  /**
   * Handle status update pub/sub event
   * Forwards status updates to WebSocket clients
   */
  private async handleStatusUpdate(data: StatusUpdateEventData): Promise<void> {
    this.logger.debug(`[STATUS-UPDATE] Forwarding update for pipeline ${data.pipelineId}`);

    // Forward to WebSocket gateway - emit to 'update' to match frontend listener
    // Also emit to organization room for dashboard views
    this.pipelineUpdatesGateway.server?.to(`pipeline_${data.pipelineId}`).emit('update', {
      type: 'pipeline',
      pipeline_id: data.pipelineId,
      organization_id: data.organizationId,
      status: data.status,
      total_rows_processed: data.rowsProcessed,
      new_rows_count: data.newRowsCount,
      error: data.error,
      updated_at: data.timestamp,
    });

    // Also emit to organization room
    if (data.organizationId) {
      this.pipelineUpdatesGateway.server?.to(`org_${data.organizationId}`).emit('pipeline_update', {
        pipeline_id: data.pipelineId,
        status: data.status,
        total_rows_processed: data.rowsProcessed,
        new_rows_count: data.newRowsCount,
        updated_at: data.timestamp,
      });
    }
  }

  // ============================================================================
  // HELPER METHODS
  // ============================================================================

  /**
   * Check if source database has new WAL changes since last LSN
   * ROOT FIX: Uses WAL position (LSN) to detect changes - NO COLUMN CHECKS
   */
  private async checkForWALChanges(
    pipelineId: string,
    lastLSN: string,
    slotName: string,
  ): Promise<boolean> {
    try {
      // Get the full pipeline with source schema
      const pipelineWithSchema = await this.pipelineRepository.findByIdForCDC(pipelineId);
      if (!pipelineWithSchema || !pipelineWithSchema.sourceSchema) {
        return false;
      }

      // For PostgreSQL, use the handler to check WAL slot
      // We'll use a simple approach: try to collect 1 row with WAL CDC
      // If it returns data, there are changes
      const result = await this.collectorService.collectIncremental({
        sourceSchema: pipelineWithSchema.sourceSchema,
        organizationId: pipelineWithSchema.organizationId,
        userId: pipelineWithSchema.createdBy || 'system',
        checkpoint: {
          walPosition: lastLSN,
          lsn: lastLSN,
          slotName,
        } as any, // Type assertion for WAL checkpoint
        limit: 1,
        offset: 0,
      });

      return result.rows.length > 0;
    } catch (error) {
      this.logger.warn(`[DELTA-CHECK] Error checking WAL changes: ${error}`);
      return false;
    }
  }

}
