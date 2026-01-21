/**
 * Scheduled Pipeline Worker Service
 * Polls the database for pipelines that are due to run based on their schedule
 * 
 * This uses a polling approach instead of PgBoss cron because:
 * - Each pipeline has its own schedule configuration stored in the database
 * - We need to support different schedules per pipeline
 * - PgBoss schedule() ties schedule name to queue name (can't have multiple)
 */

import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { PipelineService } from './pipeline.service';
import { PipelineRepository } from '../repositories/pipeline.repository';
import { ActivityLogService } from '../../activity-logs/activity-log.service';
import { PIPELINE_ACTIONS } from '../../activity-logs/constants/activity-log-types';
import { PipelineSchedulerService } from './pipeline-scheduler.service';

// Poll interval in milliseconds (every 60 seconds)
const POLL_INTERVAL_MS = 60000;

interface DuePipeline {
  id: string;
  organizationId: string;
  name: string;
  scheduleType: string;
  scheduleValue: string | null;
  scheduleTimezone: string | null;
  nextScheduledRunAt: Date | null;
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
  ) {}

  /**
   * Start the polling loop on module initialization
   */
  async onModuleInit(): Promise<void> {
    this.logger.log(`[SCHEDULER] ════════════════════════════════════════════════════════`);
    this.logger.log(`[SCHEDULER] 🔧 Starting scheduled pipeline worker...`);
    this.logger.log(`[SCHEDULER]    Poll Interval: ${POLL_INTERVAL_MS / 1000} seconds`);
    
    // Start the polling loop
    this.startPolling();
    
    this.logger.log(`[SCHEDULER] ✅ Scheduled pipeline worker started`);
    this.logger.log(`[SCHEDULER] ════════════════════════════════════════════════════════`);
  }

  /**
   * Stop the polling loop on module destruction
   */
  onModuleDestroy(): void {
    this.stopPolling();
  }

  /**
   * Start the polling loop
   */
  private startPolling(): void {
    // Run immediately on startup
    void this.pollForDuePipelines();

    // Then start the interval
    this.pollInterval = setInterval(() => {
      void this.pollForDuePipelines();
    }, POLL_INTERVAL_MS);
  }

  /**
   * Stop the polling loop
   */
  private stopPolling(): void {
    if (this.pollInterval) {
      clearInterval(this.pollInterval);
      this.pollInterval = null;
      this.logger.log(`[SCHEDULER] Polling stopped`);
    }
  }

  /**
   * Poll for pipelines that are due to run
   */
  private async pollForDuePipelines(): Promise<void> {
    // Prevent concurrent polling
    if (this.isProcessing) {
      this.logger.debug(`[SCHEDULER] Skipping poll - already processing`);
      return;
    }

    this.isProcessing = true;

    try {
      const now = new Date();
      this.logger.debug(`[SCHEDULER] Polling for due pipelines at ${now.toISOString()}`);

      // Find all pipelines that are due to run
      const duePipelines = await this.pipelineRepository.findDuePipelines();

      if (duePipelines.length === 0) {
        this.logger.debug(`[SCHEDULER] No pipelines due to run`);
        return;
      }

      this.logger.log(`[SCHEDULER] 📋 Found ${duePipelines.length} pipeline(s) due to run`);

      // Process each due pipeline
      for (const pipeline of duePipelines) {
        await this.runScheduledPipeline(pipeline as DuePipeline);
      }
    } catch (error) {
      // Extract detailed error information
      const errorMessage = error instanceof Error ? error.message : String(error);
      const errorStack = error instanceof Error ? error.stack : undefined;
      
      // Check for common database errors and provide actionable fixes
      if (errorMessage.includes('relation') && errorMessage.includes('does not exist')) {
        this.logger.error(
          `[SCHEDULER] Database table does not exist. Please run migrations:\n` +
          `  cd apps/api && bun run db:migrate\n` +
          `Error: ${errorMessage}`,
        );
      } else if (errorMessage.includes('column') && errorMessage.includes('does not exist')) {
        const columnMatch = errorMessage.match(/column "([^"]+)" does not exist/);
        const columnName = columnMatch ? columnMatch[1] : 'unknown';
        this.logger.error(
          `[SCHEDULER] ════════════════════════════════════════════════════════\n` +
          `❌ Database column "${columnName}" does not exist\n` +
          `This usually means migrations haven't been run.\n` +
          `\nTo fix this, run:\n` +
          `  cd apps/api\n` +
          `  bun run db:migrate\n` +
          `\nThis will apply all pending migrations including:\n` +
          `  - 0016_pipeline_incremental_sync_fixes.sql (adds pause_timestamp and other columns)\n` +
          `  - 0017_add_polling_trigger_type.sql (adds polling to trigger_type enum)\n` +
          `\nOriginal error: ${errorMessage}\n` +
          `════════════════════════════════════════════════════════\n`,
        );
      } else {
        this.logger.error(`[SCHEDULER] Error during polling: ${errorMessage}`, errorStack);
      }
    } finally {
      this.isProcessing = false;
    }
  }

  /**
   * Run a scheduled pipeline
   */
  private async runScheduledPipeline(pipeline: DuePipeline): Promise<void> {
    const { id: pipelineId, organizationId, name, scheduleType, scheduleValue, scheduleTimezone } = pipeline;
    const startTime = Date.now();

    this.logger.log(`[JOB] ════════════════════════════════════════════════════════`);
    this.logger.log(`[JOB] 🚀 SCHEDULED PIPELINE RUN TRIGGERED`);
    this.logger.log(`[JOB]    Pipeline: ${name} (${pipelineId})`);
    this.logger.log(`[JOB]    Organization: ${organizationId}`);
    this.logger.log(`[JOB]    Schedule Type: ${scheduleType}`);
    this.logger.log(`[JOB]    Schedule Value: ${scheduleValue}`);
    this.logger.log(`[JOB]    Triggered At: ${new Date().toISOString()}`);
    this.logger.log(`[JOB] ════════════════════════════════════════════════════════`);

    try {
      // Log the scheduled run start
      await this.activityLogService.logPipelineAction(
        organizationId,
        null, // System action - no user ID
        PIPELINE_ACTIONS.SCHEDULED_RUN_STARTED,
        pipelineId,
        name,
        {
          scheduleType,
          scheduleValue,
          scheduledAt: new Date().toISOString(),
        },
      );

      // Execute the pipeline
      const run = await this.pipelineService.runPipeline(
        pipelineId,
        '', // System-triggered run - empty userId
        'scheduled',
        { batchSize: 1000 },
      );

      // Calculate next run time and update the pipeline
      const cronExpression = this.schedulerService.generateCronExpression(
        scheduleType as any, 
        scheduleValue || undefined
      );
      
      let nextScheduledRunAt: Date | undefined;
      if (cronExpression) {
        // Simple next run calculation - add interval
        nextScheduledRunAt = this.calculateNextRun(scheduleType, scheduleValue);
      }

      // Update timestamps
      await this.pipelineRepository.update(pipelineId, {
        lastScheduledRunAt: new Date(),
        nextScheduledRunAt: nextScheduledRunAt,
      });

      const duration = Date.now() - startTime;
      this.logger.log(`[JOB] ────────────────────────────────────────────────────────`);
      this.logger.log(`[JOB] ✅ SCHEDULED RUN COMPLETED SUCCESSFULLY`);
      this.logger.log(`[JOB]    Pipeline: ${name} (${pipelineId})`);
      this.logger.log(`[JOB]    Run ID: ${run.id}`);
      this.logger.log(`[JOB]    Duration: ${duration}ms`);
      this.logger.log(`[JOB]    Next Run: ${nextScheduledRunAt?.toISOString() || 'Not calculated'}`);
      this.logger.log(`[JOB] ────────────────────────────────────────────────────────`);

      // Log success
      await this.activityLogService.logPipelineAction(
        organizationId,
        null, // System action - no user ID
        PIPELINE_ACTIONS.SCHEDULED_RUN_COMPLETED,
        pipelineId,
        name,
        {
          runId: run.id,
          duration,
          status: 'success',
          nextScheduledRunAt: nextScheduledRunAt?.toISOString(),
        },
      );
    } catch (error) {
      const duration = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : String(error);

      this.logger.error(`[JOB] ────────────────────────────────────────────────────────`);
      this.logger.error(`[JOB] ❌ SCHEDULED RUN FAILED`);
      this.logger.error(`[JOB]    Pipeline: ${name} (${pipelineId})`);
      this.logger.error(`[JOB]    Duration: ${duration}ms`);
      this.logger.error(`[JOB]    Error: ${errorMessage}`);
      this.logger.error(`[JOB] ────────────────────────────────────────────────────────`);

      // Still update next run time even on failure
      try {
        const nextScheduledRunAt = this.calculateNextRun(scheduleType, scheduleValue);
        await this.pipelineRepository.update(pipelineId, {
          nextScheduledRunAt,
        });
      } catch (updateError) {
        this.logger.error(`[JOB] Failed to update next run time: ${updateError}`);
      }

      // Log failure
      try {
        await this.activityLogService.logPipelineAction(
          organizationId,
          null, // System action - no user ID
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
        this.logger.error(`[JOB] Failed to log scheduled run failure: ${logError}`);
      }
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
        // Next day at the same time
        const [hours, minutes] = (value || '00:00').split(':').map(Number);
        const next = new Date(now);
        next.setDate(next.getDate() + 1);
        next.setHours(hours || 0, minutes || 0, 0, 0);
        return next;
      }
      case 'weekly': {
        // Next week same day/time
        return new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000);
      }
      case 'monthly': {
        // Next month same day
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
