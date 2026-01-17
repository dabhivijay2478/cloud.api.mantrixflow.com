/**
 * PgBoss vs NestJS @Cron Guide
 *
 * This file provides guidance on when to use each approach
 * and examples of both.
 */

import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { PgBossService } from '../pgboss.service';
import { QUEUE_NAMES } from '../pgboss.constants';

/**
 * WHEN TO USE EACH APPROACH:
 *
 * ========================================
 * USE PgBoss Scheduling WHEN:
 * ========================================
 * 1. You need DISTRIBUTED scheduling (only one instance runs the job)
 * 2. Jobs need to survive application restarts
 * 3. Jobs need retry logic with persistence
 * 4. You need job history and audit trail
 * 5. Jobs might take a long time and need to be tracked
 * 6. You need to dynamically add/remove schedules at runtime
 * 7. Job data needs to be stored with the schedule
 *
 * Examples:
 * - Database maintenance jobs
 * - Report generation
 * - Data pipeline execution
 * - Batch processing
 * - External API syncs
 *
 * ========================================
 * USE NestJS @Cron WHEN:
 * ========================================
 * 1. Simple, fast tasks that don't need persistence
 * 2. In-memory operations only
 * 3. Every instance should run the task (cache warming, etc.)
 * 4. Tasks that are quick and don't need tracking
 * 5. Development/debugging convenience
 *
 * Examples:
 * - In-memory cache refresh
 * - Metrics collection
 * - Health checks
 * - Log rotation
 * - Connection pool cleanup
 */

@Injectable()
export class SchedulerExamples {
  private readonly logger = new Logger(SchedulerExamples.name);

  constructor(private readonly pgBossService: PgBossService) {}

  // ==========================================
  // NESTJS @CRON EXAMPLES
  // Use for simple, non-distributed tasks
  // ==========================================

  /**
   * Example: Cache cleanup every minute
   * Runs on ALL application instances
   */
  @Cron(CronExpression.EVERY_MINUTE)
  async cleanupLocalCache(): Promise<void> {
    this.logger.debug('Cleaning up local cache...');
    // In-memory cache cleanup
    // This runs on every instance - suitable for local caches
  }

  /**
   * Example: Collect metrics every 30 seconds
   */
  @Cron(CronExpression.EVERY_30_SECONDS)
  async collectMetrics(): Promise<void> {
    this.logger.debug('Collecting application metrics...');
    // Collect metrics for this instance
    // Each instance reports its own metrics
  }

  /**
   * Example: Health check every 5 minutes
   */
  @Cron('*/5 * * * *')
  async healthCheck(): Promise<void> {
    this.logger.debug('Running health check...');
    // Check database connections, external services, etc.
    // Each instance verifies its own connections
  }

  // ==========================================
  // PGBOSS SCHEDULING EXAMPLES
  // Use for distributed, persistent tasks
  // ==========================================

  /**
   * Example: Setup distributed cron jobs
   * Call this once during application startup
   */
  async setupDistributedJobs(): Promise<void> {
    // Daily report generation - only ONE instance runs this
    await this.pgBossService.schedule(QUEUE_NAMES.DATA_EXPORT, {
      cron: '0 0 * * *', // Every day at midnight
      timezone: 'UTC',
      data: {
        type: 'daily-report',
        format: 'pdf',
      },
    });

    // Hourly data sync - distributed, exactly once
    await this.pgBossService.schedule(QUEUE_NAMES.DATA_SYNC, {
      cron: '0 * * * *', // Every hour
      timezone: 'UTC',
      data: {
        syncType: 'incremental',
      },
    });

    // Weekly cleanup - runs once across all instances
    await this.pgBossService.schedule(QUEUE_NAMES.CLEANUP, {
      cron: '0 3 * * 0', // Every Sunday at 3 AM
      timezone: 'UTC',
      data: {
        cleanupType: 'archive',
        retentionDays: 30,
      },
    });

    this.logger.log('Distributed cron jobs scheduled with PgBoss');
  }

  /**
   * Example: Dynamic schedule creation
   * Use when schedules need to be created at runtime
   */
  async createDynamicSchedule(
    scheduleId: string,
    cronExpression: string,
    jobData: Record<string, unknown>,
  ): Promise<void> {
    const scheduleName = `dynamic:${scheduleId}`;

    // Create the schedule
    await this.pgBossService.schedule(scheduleName, {
      cron: cronExpression,
      timezone: 'UTC',
      data: jobData,
    });

    // Register a worker for this schedule
    await this.pgBossService.work(scheduleName, async () => {
      this.logger.log(`Executing dynamic schedule: ${scheduleId}`);
      // Process the job
      return { success: true };
    });
  }

  /**
   * Example: Remove a dynamic schedule
   */
  async removeDynamicSchedule(scheduleId: string): Promise<void> {
    await this.pgBossService.unschedule(`dynamic:${scheduleId}`);
    await this.pgBossService.offWork(`dynamic:${scheduleId}`);
  }

  // ==========================================
  // HYBRID APPROACH
  // Combine both for maximum flexibility
  // ==========================================

  /**
   * Example: NestJS cron that delegates to PgBoss
   * Quick trigger with persistent execution
   */
  @Cron(CronExpression.EVERY_HOUR)
  async checkAndTriggerPipelines(): Promise<void> {
    this.logger.debug('Checking for pipelines to run...');

    // Quick check (runs on every instance, but only triggers jobs)
    const pipelinesNeedingRun: string[] = []; // Query your DB

    for (const pipelineId of pipelinesNeedingRun) {
      // Delegate actual execution to PgBoss (distributed, persistent)
      await this.pgBossService.sendSingleton(
        QUEUE_NAMES.PIPELINE_EXECUTION,
        { pipelineId },
        `pipeline:${pipelineId}`,
        { singletonSeconds: 3600 }, // Prevent duplicate runs for 1 hour
      );
    }
  }
}

/**
 * CRON EXPRESSION REFERENCE
 *
 * Cron Expression Format:
 * ┌───────────── minute (0 - 59)
 * │ ┌───────────── hour (0 - 23)
 * │ │ ┌───────────── day of month (1 - 31)
 * │ │ │ ┌───────────── month (1 - 12)
 * │ │ │ │ ┌───────────── day of week (0 - 6) (Sunday = 0)
 * │ │ │ │ │
 * * * * * *
 *
 * Common Expressions:
 * - '0 * * * *'      - Every hour at minute 0
 * - 'star/5 * * * *' - Every 5 minutes (replace 'star' with asterisk)
 * - '0 0 * * *'      - Every day at midnight
 * - '0 9 * * 1-5'    - Every weekday at 9 AM
 * - '0 0 1 * *'      - First day of every month at midnight
 * - '0 3 * * 0'      - Every Sunday at 3 AM
 */
