/**
 * Activity Log Service
 * Centralized service for logging all organization-scoped activities
 *
 * This service provides a single point of entry for all activity logging.
 * Controllers and services should call this service to log activities,
 * rather than directly accessing the repository.
 *
 * All logs are immutable and organization-scoped.
 */

import { Injectable, Logger } from '@nestjs/common';
import type { ActivityLog } from '../../database/schemas/activity-logs';
import type { ActivityLogActionType, ActivityLogEntityType } from './constants/activity-log-types';
import { ActivityLogRepository } from './repositories/activity-log.repository';

export interface LogActivityParams {
  organizationId: string;
  userId?: string | null; // Nullable for system actions
  actionType: ActivityLogActionType;
  entityType: ActivityLogEntityType;
  entityId?: string | null;
  message: string;
  metadata?: Record<string, unknown> | null;
}

@Injectable()
export class ActivityLogService {
  private readonly logger = new Logger(ActivityLogService.name);

  constructor(private readonly activityLogRepository: ActivityLogRepository) {}

  /**
   * Log an activity
   * This is the main method for logging activities throughout the application.
   *
   * @param params Activity log parameters
   * @returns The created activity log entry
   */
  async logActivity(params: LogActivityParams): Promise<ActivityLog> {
    try {
      const log = await this.activityLogRepository.create({
        organizationId: params.organizationId,
        userId: params.userId || null,
        actionType: params.actionType,
        entityType: params.entityType,
        entityId: params.entityId || null,
        message: params.message,
        metadata: params.metadata || null,
      });

      this.logger.debug(
        `Activity logged: ${params.actionType} for ${params.entityType} in org ${params.organizationId}`,
      );

      return log;
    } catch (error) {
      // Log error but don't throw - activity logging should not break business logic
      this.logger.error(
        `Failed to log activity: ${params.actionType}`,
        error instanceof Error ? error.stack : String(error),
      );
      // Return a minimal log entry to prevent breaking the calling code
      // In production, you might want to use a fallback logging mechanism
      throw error; // Re-throw for now to ensure we catch any schema issues during development
    }
  }

  /**
   * Get activity logs with filters and pagination
   */
  async getActivityLogs(
    organizationId: string,
    filters?: {
      actionType?: string;
      entityType?: string;
      entityId?: string;
      userId?: string;
      startDate?: Date;
      endDate?: Date;
    },
    pagination?: {
      limit?: number;
      cursor?: string;
    },
  ): Promise<ActivityLog[]> {
    return this.activityLogRepository.findMany(
      {
        organizationId,
        ...filters,
      },
      pagination,
    );
  }

  /**
   * Count activity logs with filters
   */
  async countActivityLogs(
    organizationId: string,
    filters?: {
      actionType?: string;
      entityType?: string;
      entityId?: string;
      userId?: string;
      startDate?: Date;
      endDate?: Date;
    },
  ): Promise<number> {
    return this.activityLogRepository.count({
      organizationId,
      ...filters,
    });
  }
}
