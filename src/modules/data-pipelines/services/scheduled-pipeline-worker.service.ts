/**
 * Scheduled Pipeline Worker Service
 * Handles scheduled pipeline execution using RabbitMQ
 *
 * Uses RabbitMQ for reliable scheduling with:
 * - Message queuing for scheduled runs
 * - Delayed messages for scheduling
 * - Automatic retry on failure
 * - Priority queues for urgent vs routine runs
 *
 * Guide: Each pipeline can have its own schedule, configured via:
 * - scheduleType: 'minutes' | 'hourly' | 'daily' | 'weekly' | 'monthly' | 'custom_cron'
 * - scheduleValue: Interval or time value
 * - scheduleTimezone: Timezone for the schedule
 */

import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { PipelineService } from './pipeline.service';
import { PipelineRepository } from '../repositories/pipeline.repository';
import { ActivityLogService } from '../../activity-logs/activity-log.service';
import { PIPELINE_ACTIONS } from '../../activity-logs/constants/activity-log-types';
import { PipelineSchedulerService } from './pipeline-scheduler.service';
import { RabbitMQService, FullSyncJobData } from '../../queue/rabbitmq.service';

// Default batch size for scheduled runs
const DEFAULT_BATCH_SIZE = 500;

// Polling interval for due pipelines (in milliseconds)
const POLL_INTERVAL_MS = 60000; // Every 60 seconds

interface ScheduledRunJobData {
  pipelineId: string;
  organizationId: string;
  name: string;
  scheduleType: string;
  scheduleValue: string | null;
  scheduleTimezone: string | null;
  createdBy: string | null; // User who created the pipeline - used for scheduled runs
}

@Injectable()
export class ScheduledPipelineWorkerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ScheduledPipelineWorkerService.name);
  private pollInterval: NodeJS.Timeout | null = null;
  private isProcessing = false;

  constructor(
    private readonly pipelineService: PipelineService,
    private readonly pipelineRepository: PipelineRepository,
    private readonly activityLogService: ActivityLogService,
    private readonly schedulerService: PipelineSchedulerService,
    private readonly rabbitmqService: RabbitMQService,
  ) {}

  /**
   * Start the worker on module initialization
   */
  async onModuleInit(): Promise<void> {
    this.logger.log('════════════════════════════════════════════════════════');
    this.logger.log('🔧 Starting Scheduled Pipeline Worker...');
    this.logger.log(`   Poll Interval: ${POLL_INTERVAL_MS / 1000} seconds`);
    this.logger.log(`   Default Batch Size: ${DEFAULT_BATCH_SIZE} records`);
    this.logger.log('════════════════════════════════════════════════════════');
    this.logger.log('📌 Using RabbitMQ for job management:');
    this.logger.log('   - Message queuing for scheduled runs');
    this.logger.log('   - Delayed messages for scheduling');
    this.logger.log('   - Automatic retries');
    this.logger.log('   - Priority queues (high: manual, normal: scheduled)');
    this.logger.log('════════════════════════════════════════════════════════');

    // Wait for RabbitMQ to initialize
    let retries = 0;
    while (!this.rabbitmqService.isReady() && retries < 10) {
      await new Promise((resolve) => setTimeout(resolve, 1000));
      retries++;
    }

    if (!this.rabbitmqService.isReady()) {
      this.logger.warn('RabbitMQ not ready, will retry');
      setTimeout(() => this.onModuleInit(), 5000);
      return;
    }

    // Start polling for due pipelines
    this.startPolling();

    this.logger.log('✅ Scheduled Pipeline Worker started');
  }

  /**
   * Stop the worker on module destruction
   */
  onModuleDestroy(): void {
    this.stopPolling();
    this.logger.log('Scheduled Pipeline Worker stopped');
  }

  /**
   * Handle a scheduled pipeline run (called directly from polling)
   */
  private async handleScheduledRun(data: ScheduledRunJobData): Promise<void> {
    const { pipelineId, organizationId, name, scheduleType, scheduleValue, scheduleTimezone } = data;
    const startTime = Date.now();

    this.logger.log('════════════════════════════════════════════════════════');
    this.logger.log('🚀 SCHEDULED PIPELINE RUN TRIGGERED');
    this.logger.log(`   Pipeline: ${name} (${pipelineId})`);
    this.logger.log(`   Organization: ${organizationId}`);
    this.logger.log(`   Schedule: ${scheduleType} / ${scheduleValue}`);
    this.logger.log('════════════════════════════════════════════════════════');

    try {
      // Log the scheduled run start
      await this.activityLogService.logPipelineAction(
        organizationId,
        null,
        PIPELINE_ACTIONS.SCHEDULED_RUN_STARTED,
        pipelineId,
        name,
        {
          scheduleType,
          scheduleValue,
          scheduledAt: new Date().toISOString(),
        },
      );

      // Execute the pipeline using the pipeline creator's user ID
      const userId = data.createdBy || 'system';
      const run = await this.pipelineService.runPipeline(
        pipelineId,
        userId,
        'scheduled',
        { batchSize: DEFAULT_BATCH_SIZE },
      );

      // Calculate and update next run time
      const nextRunAt = this.calculateNextRun(scheduleType, scheduleValue);
      await this.pipelineRepository.update(pipelineId, {
        lastScheduledRunAt: new Date(),
        nextScheduledRunAt: nextRunAt,
      });

      const duration = Date.now() - startTime;
      this.logger.log('────────────────────────────────────────────────────────');
      this.logger.log('✅ SCHEDULED RUN COMPLETED');
      this.logger.log(`   Pipeline: ${name} (${pipelineId})`);
      this.logger.log(`   Run ID: ${run.id}`);
      this.logger.log(`   Duration: ${duration}ms`);
      this.logger.log(`   Next Run: ${nextRunAt?.toISOString() || 'Not scheduled'}`);
      this.logger.log('────────────────────────────────────────────────────────');

      // Log success
      await this.activityLogService.logPipelineAction(
        organizationId,
        null,
        PIPELINE_ACTIONS.SCHEDULED_RUN_COMPLETED,
        pipelineId,
        name,
        {
          runId: run.id,
          duration,
          status: 'success',
          nextScheduledRunAt: nextRunAt?.toISOString(),
        },
      );
    } catch (error) {
      const duration = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : String(error);

      this.logger.error('────────────────────────────────────────────────────────');
      this.logger.error('❌ SCHEDULED RUN FAILED');
      this.logger.error(`   Pipeline: ${name} (${pipelineId})`);
      this.logger.error(`   Duration: ${duration}ms`);
      this.logger.error(`   Error: ${errorMessage}`);
      this.logger.error('────────────────────────────────────────────────────────');

      // Still update next run time on failure
      try {
        const nextRunAt = this.calculateNextRun(scheduleType, scheduleValue);
        await this.pipelineRepository.update(pipelineId, {
          nextScheduledRunAt: nextRunAt,
        });
      } catch (updateError) {
        this.logger.error(`Failed to update next run time: ${updateError}`);
      }

      // Log failure
      try {
        await this.activityLogService.logPipelineAction(
          organizationId,
          null,
          PIPELINE_ACTIONS.SCHEDULED_RUN_FAILED,
          pipelineId,
          name,
          {
            error: errorMessage,
            duration,
            status: 'failed',
          },
        );
      } catch (logError) {
        this.logger.error(`Failed to log activity: ${logError}`);
      }
    }
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
        this.logger.warn(`[SCHEDULER] Found ${stuckPipelines.length} stuck pipeline(s), recovering...`);
        
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

      // Enqueue each pipeline as a RabbitMQ job
      for (const pipeline of duePipelines) {
        if (!this.rabbitmqService.isReady()) {
          this.logger.warn('[SCHEDULER] RabbitMQ not ready, cannot enqueue job');
          continue;
        }

        // Enqueue the scheduled run as a full sync job
        await this.rabbitmqService.enqueueFullSync({
          pipelineId: pipeline.id,
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

  /**
   * Calculate the next run time based on schedule type and value
   */
  private calculateNextRun(scheduleType: string, scheduleValue: string | null): Date {
    const now = new Date();
    const value = scheduleValue || '';

    switch (scheduleType) {
      case 'minutes': {
        const minutes = parseInt(value, 10) || 15;
        return new Date(now.getTime() + minutes * 60 * 1000);
      }
      case 'hourly': {
        const hours = parseInt(value, 10) || 1;
        return new Date(now.getTime() + hours * 60 * 60 * 1000);
      }
      case 'daily': {
        const [hours, minutes] = (value || '00:00').split(':').map(Number);
        const next = new Date(now);
        next.setDate(next.getDate() + 1);
        next.setHours(hours || 0, minutes || 0, 0, 0);
        return next;
      }
      case 'weekly': {
        return new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000);
      }
      case 'monthly': {
        const next = new Date(now);
        next.setMonth(next.getMonth() + 1);
        return next;
      }
      default:
        // Default to 1 hour
        return new Date(now.getTime() + 60 * 60 * 1000);
    }
  }
}
