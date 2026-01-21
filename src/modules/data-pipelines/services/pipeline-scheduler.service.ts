/**
 * Pipeline Scheduler Service
 * Handles scheduling and unscheduling of automated pipeline runs
 * 
 * Note: This service stores schedule config in the database.
 * The ScheduledPipelineWorkerService polls for due pipelines and executes them.
 * We no longer use PgBoss - scheduling is handled via database polling.
 */

import { Injectable, Logger, BadRequestException } from '@nestjs/common';
import { ScheduleType } from '../dto/create-pipeline.dto';
import { ActivityLogService } from '../../activity-logs/activity-log.service';
import { PIPELINE_ACTIONS } from '../../activity-logs/constants/activity-log-types';

export const PIPELINE_SCHEDULE_QUEUE = 'pipeline_scheduled_run';

export interface ScheduleConfig {
  scheduleType: ScheduleType;
  scheduleValue?: string;
  timezone?: string;
}

export interface ScheduleInfo {
  isScheduled: boolean;
  scheduleType: ScheduleType;
  scheduleValue?: string;
  timezone: string;
  cronExpression?: string;
  humanReadable: string;
  nextRunAt?: Date;
  lastRunAt?: Date;
}

@Injectable()
export class PipelineSchedulerService {
  private readonly logger = new Logger(PipelineSchedulerService.name);

  constructor(
    private readonly activityLogService: ActivityLogService,
  ) {}

  /**
   * Schedule a pipeline for automatic runs
   */
  async schedulePipeline(
    pipelineId: string,
    organizationId: string,
    config: ScheduleConfig,
  ): Promise<{ cronExpression: string; nextRunAt: Date }> {
    const { scheduleType, scheduleValue, timezone = 'UTC' } = config;

    // Validate and generate cron expression
    const cronExpression = this.generateCronExpression(scheduleType, scheduleValue);

    if (!cronExpression) {
      throw new BadRequestException('Invalid schedule configuration');
    }

    // We DON'T use PgBoss schedule directly for per-pipeline schedules
    // Instead, we store the schedule config in the database and
    // the ScheduledPipelineWorkerService polls for due pipelines
    
    // Calculate next run time
    const nextRunAt = this.calculateNextRunTime(cronExpression, timezone);
    const humanReadable = this.getHumanReadableSchedule(scheduleType, scheduleValue, timezone);

    // Log to console with details
    this.logger.log(`[SCHEDULE] ✅ Pipeline schedule configured:`);
    this.logger.log(`[SCHEDULE]    Pipeline ID: ${pipelineId}`);
    this.logger.log(`[SCHEDULE]    Organization: ${organizationId}`);
    this.logger.log(`[SCHEDULE]    Schedule Type: ${scheduleType}`);
    this.logger.log(`[SCHEDULE]    Schedule Value: ${scheduleValue}`);
    this.logger.log(`[SCHEDULE]    Cron: ${cronExpression}`);
    this.logger.log(`[SCHEDULE]    Timezone: ${timezone}`);
    this.logger.log(`[SCHEDULE]    Human Readable: ${humanReadable}`);
    this.logger.log(`[SCHEDULE]    Next Run: ${nextRunAt.toISOString()}`);

    // Log to activity log
    try {
      await this.activityLogService.logPipelineAction(
        organizationId,
        null, // System action - no user ID
        PIPELINE_ACTIONS.SCHEDULE_UPDATED,
        pipelineId,
        `Pipeline ${pipelineId}`,
        {
          scheduleType,
          scheduleValue,
          timezone,
          cronExpression,
          humanReadable,
          nextRunAt: nextRunAt.toISOString(),
        },
      );
    } catch (error) {
      this.logger.warn(`[SCHEDULE] Failed to log activity: ${error}`);
    }

    return { cronExpression, nextRunAt };
  }

  /**
   * Unschedule a pipeline
   * With database-based scheduling, this just logs the action
   * The actual schedule is removed by setting scheduleType to 'none' in the pipeline
   */
  async unschedulePipeline(pipelineId: string, organizationId?: string): Promise<void> {
    this.logger.log(`[SCHEDULE] ❌ Pipeline ${pipelineId} schedule removed`);

    // Log to activity log if organizationId provided
    if (organizationId) {
      try {
        await this.activityLogService.logPipelineAction(
          organizationId,
          null, // System action - no user ID
          PIPELINE_ACTIONS.SCHEDULE_REMOVED,
          pipelineId,
          `Pipeline ${pipelineId}`,
          { action: 'unscheduled' },
        );
      } catch (error) {
        this.logger.warn(`[SCHEDULE] Failed to log unschedule activity: ${error}`);
      }
    }
  }

  /**
   * Generate cron expression from schedule configuration
   */
  generateCronExpression(
    scheduleType: ScheduleType,
    scheduleValue?: string,
  ): string | null {
    switch (scheduleType) {
      case ScheduleType.NONE:
        return null;

      case ScheduleType.MINUTES: {
        // scheduleValue is the interval in minutes (e.g., "15" for every 15 minutes)
        const minutes = parseInt(scheduleValue || '30', 10);
        if (isNaN(minutes) || minutes < 1 || minutes > 59) {
          throw new BadRequestException(
            'Invalid minutes value. Must be between 1 and 59.',
          );
        }
        return `*/${minutes} * * * *`;
      }

      case ScheduleType.HOURLY: {
        // scheduleValue is the interval in hours (e.g., "2" for every 2 hours)
        // or specific minute (e.g., "30" for XX:30)
        const value = parseInt(scheduleValue || '1', 10);
        if (isNaN(value) || value < 1 || value > 23) {
          throw new BadRequestException(
            'Invalid hourly value. Must be between 1 and 23.',
          );
        }
        return `0 */${value} * * *`;
      }

      case ScheduleType.DAILY: {
        // scheduleValue is time in HH:MM format (e.g., "09:30")
        const time = this.parseTimeString(scheduleValue || '00:00');
        return `${time.minute} ${time.hour} * * *`;
      }

      case ScheduleType.WEEKLY: {
        // scheduleValue format: "DAY:HH:MM" (e.g., "1:09:30" for Monday at 09:30)
        // DAY: 0=Sunday, 1=Monday, ..., 6=Saturday
        const [dayStr, timeStr] = (scheduleValue || '1:00:00').split(':');
        const day = parseInt(dayStr, 10);
        const time = this.parseTimeString(
          timeStr ? `${timeStr}:${scheduleValue?.split(':')[2] || '00'}` : '00:00',
        );
        if (isNaN(day) || day < 0 || day > 6) {
          throw new BadRequestException(
            'Invalid weekly day value. Must be 0-6 (Sunday-Saturday).',
          );
        }
        return `${time.minute} ${time.hour} * * ${day}`;
      }

      case ScheduleType.MONTHLY: {
        // scheduleValue format: "DAY:HH:MM" (e.g., "1:09:30" for 1st of month at 09:30)
        const parts = (scheduleValue || '1:00:00').split(':');
        const dayOfMonth = parseInt(parts[0], 10);
        const time = this.parseTimeString(
          parts.length > 1 ? `${parts[1]}:${parts[2] || '00'}` : '00:00',
        );
        if (isNaN(dayOfMonth) || dayOfMonth < 1 || dayOfMonth > 31) {
          throw new BadRequestException(
            'Invalid monthly day value. Must be 1-31.',
          );
        }
        return `${time.minute} ${time.hour} ${dayOfMonth} * *`;
      }

      case ScheduleType.CUSTOM_CRON: {
        // scheduleValue is a full cron expression
        if (!scheduleValue || !this.isValidCron(scheduleValue)) {
          throw new BadRequestException(
            'Invalid cron expression. Must be a valid 5-part cron string.',
          );
        }
        return scheduleValue;
      }

      default:
        return null;
    }
  }

  /**
   * Parse time string in HH:MM format
   */
  private parseTimeString(timeStr: string): { hour: number; minute: number } {
    const [hourStr, minuteStr] = timeStr.split(':');
    const hour = parseInt(hourStr || '0', 10);
    const minute = parseInt(minuteStr || '0', 10);

    if (isNaN(hour) || hour < 0 || hour > 23) {
      throw new BadRequestException('Invalid hour. Must be 0-23.');
    }
    if (isNaN(minute) || minute < 0 || minute > 59) {
      throw new BadRequestException('Invalid minute. Must be 0-59.');
    }

    return { hour, minute };
  }

  /**
   * Validate cron expression (basic validation)
   */
  private isValidCron(cron: string): boolean {
    const parts = cron.trim().split(/\s+/);
    // Standard cron has 5 parts: minute hour day month weekday
    return parts.length === 5;
  }

  /**
   * Calculate next run time from cron expression
   */
  private calculateNextRunTime(cronExpression: string, _timezone: string): Date {
    // Simple calculation - for more accurate results, use a cron parser library
    // This is a basic implementation that returns approximate next run
    const now = new Date();
    const parts = cronExpression.split(' ');
    
    // Parse minute and hour if specified
    const minutePart = parts[0];
    const hourPart = parts[1];
    
    let nextRun = new Date(now);
    
    // Handle */N patterns for minutes
    if (minutePart.startsWith('*/')) {
      const interval = parseInt(minutePart.substring(2), 10);
      const nextMinute = Math.ceil((now.getMinutes() + 1) / interval) * interval;
      nextRun.setMinutes(nextMinute % 60);
      if (nextMinute >= 60) {
        nextRun.setHours(nextRun.getHours() + 1);
      }
      nextRun.setSeconds(0);
      nextRun.setMilliseconds(0);
    } else if (minutePart !== '*') {
      // Specific minute
      nextRun.setMinutes(parseInt(minutePart, 10));
      nextRun.setSeconds(0);
      nextRun.setMilliseconds(0);
    }
    
    // Handle */N patterns for hours
    if (hourPart.startsWith('*/')) {
      const interval = parseInt(hourPart.substring(2), 10);
      const nextHour = Math.ceil((now.getHours() + 1) / interval) * interval;
      if (nextHour >= 24) {
        nextRun.setDate(nextRun.getDate() + 1);
        nextRun.setHours(0);
      } else {
        nextRun.setHours(nextHour);
      }
    } else if (hourPart !== '*') {
      // Specific hour
      const hour = parseInt(hourPart, 10);
      if (hour < now.getHours() || (hour === now.getHours() && parseInt(minutePart, 10) <= now.getMinutes())) {
        nextRun.setDate(nextRun.getDate() + 1);
      }
      nextRun.setHours(hour);
    }
    
    return nextRun;
  }

  /**
   * Get human-readable schedule description
   */
  getHumanReadableSchedule(
    scheduleType: ScheduleType,
    scheduleValue?: string,
    timezone: string = 'UTC',
  ): string {
    switch (scheduleType) {
      case ScheduleType.NONE:
        return 'Manual (no automatic schedule)';

      case ScheduleType.MINUTES: {
        const minutes = parseInt(scheduleValue || '30', 10);
        return `Every ${minutes} minute${minutes === 1 ? '' : 's'}`;
      }

      case ScheduleType.HOURLY: {
        const hours = parseInt(scheduleValue || '1', 10);
        return hours === 1 ? 'Every hour' : `Every ${hours} hours`;
      }

      case ScheduleType.DAILY: {
        const time = scheduleValue || '00:00';
        return `Daily at ${time} (${timezone})`;
      }

      case ScheduleType.WEEKLY: {
        const parts = (scheduleValue || '1:00:00').split(':');
        const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
        const day = parseInt(parts[0], 10);
        const time = parts.length > 1 ? `${parts[1]}:${parts[2] || '00'}` : '00:00';
        return `Every ${dayNames[day] || 'Monday'} at ${time} (${timezone})`;
      }

      case ScheduleType.MONTHLY: {
        const parts = (scheduleValue || '1:00:00').split(':');
        const dayOfMonth = parseInt(parts[0], 10);
        const time = parts.length > 1 ? `${parts[1]}:${parts[2] || '00'}` : '00:00';
        const suffix = this.getOrdinalSuffix(dayOfMonth);
        return `Monthly on the ${dayOfMonth}${suffix} at ${time} (${timezone})`;
      }

      case ScheduleType.CUSTOM_CRON:
        return `Custom: ${scheduleValue || 'Not set'}`;

      default:
        return 'Unknown schedule';
    }
  }

  /**
   * Get ordinal suffix for a number (1st, 2nd, 3rd, etc.)
   */
  private getOrdinalSuffix(n: number): string {
    const s = ['th', 'st', 'nd', 'rd'];
    const v = n % 100;
    return s[(v - 20) % 10] || s[v] || s[0];
  }

  /**
   * Get schedule info for a pipeline
   */
  getScheduleInfo(
    scheduleType: ScheduleType,
    scheduleValue?: string,
    timezone: string = 'UTC',
    lastScheduledRunAt?: Date,
    nextScheduledRunAt?: Date,
  ): ScheduleInfo {
    const isScheduled = scheduleType !== ScheduleType.NONE;
    const cronExpression = isScheduled
      ? this.generateCronExpression(scheduleType, scheduleValue) || undefined
      : undefined;

    return {
      isScheduled,
      scheduleType,
      scheduleValue,
      timezone,
      cronExpression,
      humanReadable: this.getHumanReadableSchedule(scheduleType, scheduleValue, timezone),
      nextRunAt: nextScheduledRunAt,
      lastRunAt: lastScheduledRunAt,
    };
  }
}
