/**
 * Activity Log Repository
 * Database operations for activity logs
 */

import { Inject, Injectable } from '@nestjs/common';
import { and, desc, eq, gte, lte, or, sql } from 'drizzle-orm';
import type { DrizzleDatabase } from '../../../database/drizzle/database';
import {
  type ActivityLog,
  type NewActivityLog,
  activityLogs,
} from '../../../database/schemas/activity-logs';

export interface ActivityLogFilters {
  organizationId: string;
  actionType?: string;
  entityType?: string;
  entityId?: string;
  userId?: string;
  startDate?: Date;
  endDate?: Date;
}

export interface ActivityLogPagination {
  limit?: number;
  cursor?: string; // ID of the last log from previous page
}

@Injectable()
export class ActivityLogRepository {
  constructor(@Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase) {}

  /**
   * Create a new activity log entry
   * Logs are immutable - no updates or deletes allowed
   */
  async create(data: NewActivityLog): Promise<ActivityLog> {
    const [log] = await this.db.insert(activityLogs).values(data).returning();
    return log;
  }

  /**
   * Find activity logs with filters and pagination
   */
  async findMany(
    filters: ActivityLogFilters,
    pagination?: ActivityLogPagination,
  ): Promise<ActivityLog[]> {
    const conditions = [eq(activityLogs.organizationId, filters.organizationId)];

    if (filters.actionType) {
      conditions.push(eq(activityLogs.actionType, filters.actionType));
    }

    if (filters.entityType) {
      conditions.push(eq(activityLogs.entityType, filters.entityType));
    }

    if (filters.entityId) {
      conditions.push(eq(activityLogs.entityId, filters.entityId));
    }

    if (filters.userId) {
      conditions.push(eq(activityLogs.userId, filters.userId));
    }

    if (filters.startDate) {
      conditions.push(gte(activityLogs.createdAt, filters.startDate));
    }

    if (filters.endDate) {
      conditions.push(lte(activityLogs.createdAt, filters.endDate));
    }

    // Cursor-based pagination
    if (pagination?.cursor) {
      // Get the timestamp of the cursor log for efficient pagination
      const [cursorLog] = await this.db
        .select({ createdAt: activityLogs.createdAt })
        .from(activityLogs)
        .where(eq(activityLogs.id, pagination.cursor))
        .limit(1);

      if (cursorLog && pagination.cursor) {
        const dateCondition = sql`${activityLogs.createdAt} < ${cursorLog.createdAt}`;
        const idCondition = and(
          eq(activityLogs.createdAt, cursorLog.createdAt),
          sql`${activityLogs.id} < ${pagination.cursor}`,
        );
        if (idCondition) {
          conditions.push(or(dateCondition, idCondition)!);
        } else {
          conditions.push(dateCondition);
        }
      }
    }

    const limit = pagination?.limit || 50;

    return this.db
      .select()
      .from(activityLogs)
      .where(and(...conditions))
      .orderBy(desc(activityLogs.createdAt), desc(activityLogs.id))
      .limit(limit);
  }

  /**
   * Count activity logs with filters
   */
  async count(filters: ActivityLogFilters): Promise<number> {
    const conditions = [eq(activityLogs.organizationId, filters.organizationId)];

    if (filters.actionType) {
      conditions.push(eq(activityLogs.actionType, filters.actionType));
    }

    if (filters.entityType) {
      conditions.push(eq(activityLogs.entityType, filters.entityType));
    }

    if (filters.entityId) {
      conditions.push(eq(activityLogs.entityId, filters.entityId));
    }

    if (filters.userId) {
      conditions.push(eq(activityLogs.userId, filters.userId));
    }

    if (filters.startDate) {
      conditions.push(gte(activityLogs.createdAt, filters.startDate));
    }

    if (filters.endDate) {
      conditions.push(lte(activityLogs.createdAt, filters.endDate));
    }

    const [result] = await this.db
      .select({ count: sql<number>`count(*)` })
      .from(activityLogs)
      .where(and(...conditions));

    return result?.count || 0;
  }

  /**
   * Find activity log by ID
   */
  async findById(id: string): Promise<ActivityLog | null> {
    const [log] = await this.db
      .select()
      .from(activityLogs)
      .where(eq(activityLogs.id, id))
      .limit(1);
    return log || null;
  }
}
