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
import type { ActivityLogCursor } from '../utils/cursor.util';

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
  cursor?: ActivityLogCursor; // Decoded cursor with createdAt (ISO string) and id
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
    // Cursor is already decoded and contains createdAt (ISO string) and id
    // We use these values directly - no database lookup needed
    if (pagination?.cursor) {
      const cursor = pagination.cursor;

      // Validate cursor has required fields
      if (!cursor.createdAt || !cursor.id) {
        throw new Error('Cursor must have both createdAt and id');
      }

      // Cursor createdAt is already validated as ISO string in decodeCursor()
      // Just ensure it's a string (should always be true after decoding)
      const cursorDateStr: string =
        typeof cursor.createdAt === 'string'
          ? cursor.createdAt
          : new Date(cursor.createdAt).toISOString();

      // Escape single quotes to prevent SQL injection
      const escapedDateStr = cursorDateStr.replace(/'/g, "''");
      const escapedCursorId = cursor.id.replace(/'/g, "''");

      // Build pagination condition using string timestamps
      // This ensures postgres-js receives string parameters, not Date objects
      // Condition: (created_at < cursor.createdAt) OR (created_at = cursor.createdAt AND id < cursor.id)
      const dateCondition = sql.raw(
        `"activity_logs"."created_at" < '${escapedDateStr}'::timestamp`,
      );
      const idCondition = sql.raw(
        `"activity_logs"."created_at" = '${escapedDateStr}'::timestamp AND "activity_logs"."id" < '${escapedCursorId}'`,
      );

      conditions.push(or(dateCondition, idCondition)!);
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
    const [log] = await this.db.select().from(activityLogs).where(eq(activityLogs.id, id)).limit(1);
    return log || null;
  }
}
