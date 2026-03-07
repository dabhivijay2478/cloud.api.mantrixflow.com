/**
 * Scheduled Pipeline Worker Service
 * Handles scheduled pipeline execution using pgmq + pg_cron
 *
 * Uses pgmq for reliable scheduling with:
 * - Message queuing for scheduled runs (pipeline_jobs queue)
 * - Automatic retry on failure
 * - Priority queues for urgent vs routine runs
 *
 * Guide: Each pipeline can have its own schedule, configured via:
 * - scheduleType: 'minutes' | 'hourly' | 'daily' | 'weekly' | 'monthly' | 'custom_cron'
 * - scheduleValue: Interval or time value
 * - scheduleTimezone: Timezone for the schedule
 */

import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { PipelineRepository } from '../repositories/pipeline.repository';
import { PipelineSchedulerService } from './pipeline-scheduler.service';
import { PgmqQueueService } from '../../queue';

// Default batch size for scheduled runs
const DEFAULT_BATCH_SIZE = 500;

// Polling interval for due pipelines (in milliseconds)
const POLL_INTERVAL_MS = 60000; // Every 60 seconds

@Injectable()
export class ScheduledPipelineWorkerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ScheduledPipelineWorkerService.name);
  private pollInterval: NodeJS.Timeout | null = null;
  private isProcessing = false;

  constructor(
    private readonly pipelineRepository: PipelineRepository,
    readonly _schedulerService: PipelineSchedulerService,
    private readonly pipelineQueueService: PgmqQueueService,
  ) {}

  /**
   * Start the worker on module initialization
   */
  async onModuleInit(): Promise<void> {
    this.logger.log('════════════════════════════════════════════════════════');
    this.logger.log('Starting Scheduled Pipeline Worker...');
    this.logger.log(`   Poll Interval: ${POLL_INTERVAL_MS / 1000} seconds`);
    this.logger.log(`   Default Batch Size: ${DEFAULT_BATCH_SIZE} records`);
    this.logger.log('════════════════════════════════════════════════════════');
    this.logger.log('Using pgmq + pg_cron for job management:');
    this.logger.log('   - Message queuing for scheduled runs (pipeline_jobs)');
    this.logger.log('   - Automatic retries via pgmq requeue with backoff');
    this.logger.log('   - CDC poll cycle every 5 min via pg_cron');
    this.logger.log('════════════════════════════════════════════════════════');

    // Wait for pgmq queue to be ready
    let retries = 0;
    while (!this.pipelineQueueService.isReady() && retries < 10) {
      await new Promise((resolve) => setTimeout(resolve, 1000));
      retries++;
    }

    if (!this.pipelineQueueService.isReady()) {
      this.logger.warn('pgmq not ready, will retry');
      setTimeout(() => this.onModuleInit(), 5000);
      return;
    }

    // Start polling for due pipelines
    this.startPolling();

    this.logger.log('Scheduled Pipeline Worker started');
  }

  /**
   * Stop the worker on module destruction
   */
  onModuleDestroy(): void {
    this.stopPolling();
    this.logger.log('Scheduled Pipeline Worker stopped');
  }

  /**
   * Start polling for due pipelines
   */
  private startPolling(): void {
    // Run immediately on startup
    void this.pollForDuePipelines();

    // Then start the interval
    this.pollInterval = setInterval(() => {
      void this.pollForDuePipelines();
    }, POLL_INTERVAL_MS);

    this.logger.log(`Started polling for due pipelines (interval: ${POLL_INTERVAL_MS}ms)`);
  }

  /**
   * Stop polling
   */
  private stopPolling(): void {
    if (this.pollInterval) {
      clearInterval(this.pollInterval);
      this.pollInterval = null;
    }
  }

  /**
   * Recover stuck pipelines (running for > 1 hour)
   * ROOT FIX: Automatically reset pipelines that are stuck in 'running' status
   */
  private async recoverStuckPipelines(): Promise<void> {
    try {
      // Find pipelines stuck in 'running' status for more than 1 hour
      const stuckPipelines = await this.pipelineRepository.findStuckPipelines();

      if (stuckPipelines.length > 0) {
        this.logger.warn(
          `[SCHEDULER] Found ${stuckPipelines.length} stuck pipeline(s), recovering...`,
        );

        for (const pipeline of stuckPipelines) {
          try {
            // Reset to idle or listing based on incremental mode
            const targetStatus = pipeline.incrementalColumn ? 'listing' : 'idle';

            await this.pipelineRepository.update(pipeline.id, {
              status: targetStatus,
              lastError: `Pipeline was stuck in 'running' status and has been automatically recovered`,
            });

            this.logger.log(
              `[SCHEDULER] Recovered stuck pipeline ${pipeline.name} (${pipeline.id}) - reset to ${targetStatus}`,
            );
          } catch (error) {
            this.logger.error(
              `[SCHEDULER] Failed to recover stuck pipeline ${pipeline.id}: ${error}`,
            );
          }
        }
      }
    } catch (error) {
      this.logger.error(`[SCHEDULER] Error recovering stuck pipelines: ${error}`);
    }
  }

  /**
   * Poll for pipelines that are due to run and enqueue them
   */
  private async pollForDuePipelines(): Promise<void> {
    if (this.isProcessing) {
      this.logger.debug('[SCHEDULER] Skipping poll - already processing');
      return;
    }

    this.isProcessing = true;

    try {
      const now = new Date();
      this.logger.debug(`[SCHEDULER] Polling for due pipelines at ${now.toISOString()}`);

      // ROOT FIX: Recover stuck pipelines (running for > 1 hour)
      await this.recoverStuckPipelines();

      // Find all pipelines that are due
      const duePipelines = await this.pipelineRepository.findDuePipelines(now);

      if (duePipelines.length === 0) {
        this.logger.debug('[SCHEDULER] No pipelines due to run');
        return;
      }

      this.logger.log(`[SCHEDULER] Found ${duePipelines.length} pipeline(s) due to run`);

      // Enqueue each pipeline as a pgmq job
      for (const pipeline of duePipelines) {
        if (!this.pipelineQueueService.isReady()) {
          this.logger.warn('[SCHEDULER] pgmq not ready, cannot enqueue job');
          continue;
        }

        const run = await this.pipelineRepository.createRun({
          pipelineId: pipeline.id,
          organizationId: pipeline.organizationId,
          status: 'pending',
          jobState: 'queued',
          triggerType: 'scheduled',
          triggeredBy: pipeline.createdBy || undefined,
          startedAt: new Date(),
        });
        await this.pipelineQueueService.enqueueFullSync({
          pipelineId: pipeline.id,
          runId: run.id,
          organizationId: pipeline.organizationId,
          userId: pipeline.createdBy || 'system',
          triggerType: 'scheduled',
          batchSize: DEFAULT_BATCH_SIZE,
        });

        this.logger.log(
          `[SCHEDULER] Enqueued scheduled run for pipeline ${pipeline.name} (${pipeline.id})`,
        );
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);

      if (errorMessage.includes('column') && errorMessage.includes('does not exist')) {
        this.logger.error(
          `[SCHEDULER] Database column missing. Run migrations:\n  cd apps/api && bun run db:migrate`,
        );
      } else {
        this.logger.error(`[SCHEDULER] Error during polling: ${errorMessage}`);
      }
    } finally {
      this.isProcessing = false;
    }
  }
}
