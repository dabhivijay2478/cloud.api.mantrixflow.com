/**
 * PostgreSQL Pipeline Queue Service
 * Manages job queue for scheduled pipeline executions
 */

import { InjectQueue } from '@nestjs/bullmq';
import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import type { Queue } from 'bullmq';
import type { PostgresPipelineService } from '../postgres-pipeline.service';
import type { PostgresPipelineRepository } from '../repositories/postgres-pipeline.repository';
import type { PipelineJobData } from './jobs/postgres-pipeline.processor';

@Injectable()
export class PostgresPipelineQueueService {
  private readonly logger = new Logger(PostgresPipelineQueueService.name);

  constructor(
    @InjectQueue('postgres-pipeline')
    private readonly pipelineQueue: Queue<PipelineJobData>,
    private readonly pipelineRepository: PostgresPipelineRepository,
    private readonly pipelineService: PostgresPipelineService,
  ) {}

  /**
   * Add pipeline execution job to queue
   */
  async addPipelineJob(
    pipelineId: string,
    triggeredBy?: string,
    triggerType: 'manual' | 'scheduled' | 'webhook' = 'manual',
    metadata?: Record<string, any>,
  ): Promise<string> {
    const job = await this.pipelineQueue.add(
      'execute-pipeline',
      {
        pipelineId,
        triggeredBy,
        triggerType,
        metadata,
      },
      {
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 5000,
        },
        removeOnComplete: 100, // Keep last 100 completed jobs
        removeOnFail: 500, // Keep last 500 failed jobs
      },
    );

    this.logger.log(`Added pipeline job ${job.id} for pipeline ${pipelineId}`);

    return job.id!;
  }

  /**
   * Schedule recurring pipeline job
   */
  async schedulePipeline(pipelineId: string, frequency: string): Promise<void> {
    const cronPattern = this.frequencyToCron(frequency);

    if (!cronPattern) {
      this.logger.warn(`Invalid frequency ${frequency} for pipeline ${pipelineId}`);
      return;
    }

    await this.pipelineQueue.add(
      'execute-pipeline',
      {
        pipelineId,
        triggerType: 'scheduled',
      },
      {
        repeat: {
          pattern: cronPattern,
        },
        jobId: `scheduled-${pipelineId}`, // Unique ID to prevent duplicates
      },
    );

    this.logger.log(`Scheduled pipeline ${pipelineId} with frequency ${frequency}`);
  }

  /**
   * Remove scheduled pipeline job
   */
  async unschedulePipeline(pipelineId: string): Promise<void> {
    const jobId = `scheduled-${pipelineId}`;
    const job = await this.pipelineQueue.getJob(jobId);

    if (job) {
      await job.remove();
      this.logger.log(`Removed scheduled job for pipeline ${pipelineId}`);
    }
  }

  /**
   * Get job status
   */
  async getJobStatus(jobId: string): Promise<any> {
    const job = await this.pipelineQueue.getJob(jobId);

    if (!job) {
      return null;
    }

    const state = await job.getState();
    const progress = job.progress;

    return {
      id: job.id,
      state,
      progress,
      data: job.data,
      attemptsMade: job.attemptsMade,
      finishedOn: job.finishedOn,
      processedOn: job.processedOn,
    };
  }

  /**
   * Cron job to check and schedule pipelines
   * Runs every 5 minutes
   */
  @Cron(CronExpression.EVERY_5_MINUTES)
  async checkScheduledPipelines(): Promise<void> {
    this.logger.debug('Checking for pipelines that need scheduling...');

    // This would query pipelines that have syncFrequency set
    // and ensure they are scheduled in the queue
    // Implementation depends on your specific requirements
  }

  /**
   * Cron job to check for new records and migrate them in active pipelines
   * Runs every 1 minute to enable near-real-time incremental migration
   *
   * IMPORTANT: This method actually migrates new records, not just checks for existence.
   * It fetches ALL new records and processes them through the full pipeline.
   */
  @Cron('*/1 * * * *') // Every 1 minute
  async checkForNewRecordsAndMigrate(): Promise<void> {
    this.logger.debug('Checking for new records in active pipelines (continuous migration)...');

    try {
      // Find all active pipelines in 'running' or 'listing' state
      // These are pipelines that should be continuously monitored and migrated
      const activePipelines = await this.pipelineRepository.findActiveContinuousPipelines();

      if (activePipelines.length === 0) {
        this.logger.debug('No active continuous pipelines found');
        return;
      }

      this.logger.log(
        `Found ${activePipelines.length} active continuous pipelines to check for new records`,
      );

      // Process each pipeline: check for new records and migrate them
      for (const pipeline of activePipelines) {
        try {
          // Double-check that pipeline is still active (safety check)
          // This prevents processing pipelines that were paused between query and processing
          if (pipeline.status !== 'active') {
            this.logger.debug(
              `Pipeline ${pipeline.id} is not active (status: ${pipeline.status}), skipping`,
            );
            continue;
          }

          // Double-check migration state (safety check)
          if (pipeline.migrationState !== 'running' && pipeline.migrationState !== 'listing') {
            this.logger.debug(
              `Pipeline ${pipeline.id} is not in continuous migration state (state: ${pipeline.migrationState}), skipping`,
            );
            continue;
          }

          // Skip if pipeline doesn't have incremental column configured
          if (!pipeline.incrementalColumn) {
            this.logger.debug(
              `Pipeline ${pipeline.id} does not have incremental column configured, skipping`,
            );
            continue;
          }

          // Check if new records exist (lightweight check)
          // If found, queue migration job which will fetch and migrate ALL new records
          const hasNewRecords = await this.pipelineService.hasNewRecords(pipeline.id);

          if (hasNewRecords) {
            this.logger.log(
              `Pipeline ${pipeline.id}: New records detected, queuing incremental migration job`,
            );

            // Queue the migration job
            // executePipeline will fetch ALL new records (not just check existence)
            // and migrate them through the full pipeline
            await this.addPipelineJob(pipeline.id, 'system', 'scheduled', {
              reason: 'continuous_incremental_migration',
              checkedAt: new Date().toISOString(),
            });

            // Schedule next check in 1 minute (aggressive checking when data is changing)
            const nextCheckIn1Min = new Date(Date.now() + 60 * 1000);
            await this.pipelineRepository.update(pipeline.id, {
              nextSyncAt: nextCheckIn1Min,
            } as any);
          } else {
            this.logger.debug(
              `Pipeline ${pipeline.id}: No new records found, scheduling next check in 5 minutes`,
            );

            // No new records found - schedule next check in 5 minutes to save resources
            // This reduces unnecessary database queries when data is not changing
            const nextCheckIn5Min = new Date(Date.now() + 5 * 60 * 1000);
            await this.pipelineRepository.update(pipeline.id, {
              nextSyncAt: nextCheckIn5Min,
            } as any);
          }
        } catch (error) {
          this.logger.error(
            `Error processing pipeline ${pipeline.id} in continuous migration: ${error.message}`,
            error.stack,
          );
          // Continue with other pipelines even if one fails
        }
      }
    } catch (error) {
      this.logger.error(
        `Error in checkForNewRecordsAndMigrate cron job: ${error.message}`,
        error.stack,
      );
    }
  }

  /**
   * Convert frequency string to cron pattern
   */
  private frequencyToCron(frequency: string): string | null {
    const frequencyMap: Record<string, string> = {
      '5min': '*/5 * * * *',
      '15min': '*/15 * * * *',
      '30min': '*/30 * * * *',
      '1hour': '0 * * * *',
      '6hours': '0 */6 * * *',
      '12hours': '0 */12 * * *',
      '24hours': '0 0 * * *',
      daily: '0 0 * * *',
      weekly: '0 0 * * 0',
      monthly: '0 0 1 * *',
    };

    return frequencyMap[frequency] || null;
  }

  /**
   * Get queue metrics
   */
  async getQueueMetrics(): Promise<{
    waiting: number;
    active: number;
    completed: number;
    failed: number;
    delayed: number;
  }> {
    const [waiting, active, completed, failed, delayed] = await Promise.all([
      this.pipelineQueue.getWaitingCount(),
      this.pipelineQueue.getActiveCount(),
      this.pipelineQueue.getCompletedCount(),
      this.pipelineQueue.getFailedCount(),
      this.pipelineQueue.getDelayedCount(),
    ]);

    return {
      waiting,
      active,
      completed,
      failed,
      delayed,
    };
  }

  /**
   * Clean old jobs
   */
  async cleanOldJobs(grace: number = 24 * 60 * 60 * 1000): Promise<void> {
    await this.pipelineQueue.clean(grace, 100, 'completed');
    await this.pipelineQueue.clean(grace, 100, 'failed');

    this.logger.log('Cleaned old jobs from queue');
  }
}
