/**
 * Scheduled Pipeline Worker Service
 * Handles automatic execution of scheduled pipelines using PgBoss
 */

import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { PgBossService } from '../../queue/pgboss.service';
import { PipelineService } from './pipeline.service';
import { PipelineRepository } from '../repositories/pipeline.repository';
import { ActivityLogService } from '../../activity-logs/activity-log.service';
import { PIPELINE_ACTIONS } from '../../activity-logs/constants/activity-log-types';
import { PIPELINE_SCHEDULE_QUEUE } from './pipeline-scheduler.service';
import type { Job } from 'pg-boss';

interface ScheduledPipelineJobData {
  pipelineId: string;
  organizationId: string;
  isScheduled: boolean;
  scheduleType: string;
  scheduleValue?: string;
}

@Injectable()
export class ScheduledPipelineWorkerService implements OnModuleInit {
  private readonly logger = new Logger(ScheduledPipelineWorkerService.name);

  constructor(
    private readonly pgBossService: PgBossService,
    private readonly pipelineService: PipelineService,
    private readonly pipelineRepository: PipelineRepository,
    private readonly activityLogService: ActivityLogService,
  ) {}

  /**
   * Register the worker on module initialization
   */
  async onModuleInit(): Promise<void> {
    // Register worker for all scheduled pipeline jobs
    // The pattern matches all pipelines: pipeline-scheduled-run:*
    await this.registerScheduledPipelineWorker();
  }

  /**
   * Register worker to process scheduled pipeline runs
   */
  private async registerScheduledPipelineWorker(): Promise<void> {
    try {
      // Register a worker that handles all scheduled pipeline jobs
      await this.pgBossService.registerWorker<ScheduledPipelineJobData>(
        `${PIPELINE_SCHEDULE_QUEUE}.*`,
        async (job: Job<{ payload: ScheduledPipelineJobData }>) => {
          await this.handleScheduledPipelineRun(job);
          return { success: true };
        },
        {
          teamSize: 5, // Process up to 5 concurrent scheduled runs
        },
      );

      this.logger.log('Scheduled pipeline worker registered successfully');
    } catch (error) {
      this.logger.error(`Failed to register scheduled pipeline worker: ${error}`);
    }
  }

  /**
   * Handle a scheduled pipeline run
   */
  private async handleScheduledPipelineRun(
    job: Job<{ payload: ScheduledPipelineJobData }>,
  ): Promise<void> {
    const { pipelineId, organizationId, scheduleType, scheduleValue } = job.data.payload;
    const startTime = Date.now();

    this.logger.log(`[JOB] ════════════════════════════════════════════════════════`);
    this.logger.log(`[JOB] 🚀 SCHEDULED PIPELINE RUN TRIGGERED`);
    this.logger.log(`[JOB]    Job ID: ${job.id}`);
    this.logger.log(`[JOB]    Pipeline ID: ${pipelineId}`);
    this.logger.log(`[JOB]    Organization: ${organizationId}`);
    this.logger.log(`[JOB]    Schedule Type: ${scheduleType}`);
    this.logger.log(`[JOB]    Schedule Value: ${scheduleValue}`);
    this.logger.log(`[JOB]    Triggered At: ${new Date().toISOString()}`);
    this.logger.log(`[JOB] ════════════════════════════════════════════════════════`);

    try {
      // Fetch current pipeline state
      const pipeline = await this.pipelineRepository.findById(pipelineId, organizationId);

      if (!pipeline) {
        this.logger.warn(`[JOB] ⚠️ Pipeline ${pipelineId} not found, skipping scheduled run`);
        return;
      }

      // Check if pipeline is still configured for scheduled runs
      if (!pipeline.scheduleType || pipeline.scheduleType === 'none') {
        this.logger.log(`Pipeline ${pipelineId} no longer scheduled, skipping`);
        return;
      }

      // Check if pipeline is paused or in a failed state
      if (pipeline.status === 'paused') {
        this.logger.log(`Pipeline ${pipelineId} is paused, skipping scheduled run`);
        return;
      }

      if (pipeline.status === 'running' || pipeline.status === 'initializing') {
        this.logger.log(`Pipeline ${pipelineId} is already running, skipping scheduled run`);
        return;
      }

      // Log the scheduled run start
      await this.activityLogService.logPipelineAction(
        organizationId,
        'system', // System-triggered run
        PIPELINE_ACTIONS.SCHEDULED_RUN_STARTED,
        pipelineId,
        pipeline.name,
        {
          scheduleType: pipeline.scheduleType,
          scheduleValue: pipeline.scheduleValue,
          scheduledAt: new Date().toISOString(),
        },
      );

      // Execute the pipeline (use 'scheduled' trigger type)
      // Note: Using 'system' as userId for scheduled runs
      const run = await this.pipelineService.runPipeline(
        pipelineId,
        'system', // System user for scheduled runs
        'scheduled',
        { batchSize: 1000 }, // Default batch size for scheduled runs
      );

      // Update last scheduled run timestamp
      await this.pipelineRepository.update(pipelineId, {
        lastScheduledRunAt: new Date(),
      });

      const duration = Date.now() - startTime;
      this.logger.log(`[JOB] ────────────────────────────────────────────────────────`);
      this.logger.log(`[JOB] ✅ SCHEDULED RUN COMPLETED SUCCESSFULLY`);
      this.logger.log(`[JOB]    Pipeline: ${pipeline.name} (${pipelineId})`);
      this.logger.log(`[JOB]    Run ID: ${run.id}`);
      this.logger.log(`[JOB]    Duration: ${duration}ms`);
      this.logger.log(`[JOB] ────────────────────────────────────────────────────────`);

      // Log success
      await this.activityLogService.logPipelineAction(
        organizationId,
        'system',
        PIPELINE_ACTIONS.SCHEDULED_RUN_COMPLETED,
        pipelineId,
        pipeline.name,
        {
          runId: run.id,
          duration,
          status: 'success',
        },
      );
    } catch (error) {
      const duration = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : String(error);

      this.logger.error(`[JOB] ────────────────────────────────────────────────────────`);
      this.logger.error(`[JOB] ❌ SCHEDULED RUN FAILED`);
      this.logger.error(`[JOB]    Pipeline ID: ${pipelineId}`);
      this.logger.error(`[JOB]    Duration: ${duration}ms`);
      this.logger.error(`[JOB]    Error: ${errorMessage}`);
      this.logger.error(`[JOB] ────────────────────────────────────────────────────────`,
        error instanceof Error ? error.stack : undefined,
      );

      // Log failure
      try {
        await this.activityLogService.logPipelineAction(
          organizationId,
          'system',
          PIPELINE_ACTIONS.SCHEDULED_RUN_FAILED,
          pipelineId,
          'Unknown', // Name might not be available if fetch failed
          {
            error: errorMessage,
            duration,
            status: 'failed',
          },
        );
      } catch (logError) {
        this.logger.error(`Failed to log scheduled run failure: ${logError}`);
      }

      // Re-throw to mark job as failed (PgBoss will handle retries)
      throw error;
    }
  }
}
