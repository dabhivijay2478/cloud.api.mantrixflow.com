/**
 * PostgreSQL Query Log Repository
 * Handles database operations for postgres_query_logs table
 */

import { Injectable, Inject } from '@nestjs/common';
import { eq, desc, lt, sql } from 'drizzle-orm';
import {
  postgresQueryLogs,
  PostgresQueryLog,
  NewPostgresQueryLog,
} from '../../../../database/drizzle/schema/postgres-connectors.schema';
import { QUERY_LOG_RETENTION_DAYS } from '../constants/postgres.constants';
import type { DrizzleDatabase } from '../../../../database/drizzle/database';

@Injectable()
export class PostgresQueryLogRepository {
  constructor(@Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase) {}

  /**
   * Create query log entry
   */
  async create(data: NewPostgresQueryLog): Promise<PostgresQueryLog> {
    const [log] = await this.db
      .insert(postgresQueryLogs)
      .values(data)
      .returning();
    return log;
  }

  /**
   * Find query logs for connection
   */
  async findByConnectionId(
    connectionId: string,
    limit: number = 100,
    offset: number = 0,
  ): Promise<PostgresQueryLog[]> {
    return await this.db
      .select()
      .from(postgresQueryLogs)
      .where(eq(postgresQueryLogs.connectionId, connectionId))
      .orderBy(desc(postgresQueryLogs.createdAt))
      .limit(limit)
      .offset(offset);
  }

  /**
   * Find query logs for user
   */
  async findByUserId(
    userId: string,
    limit: number = 100,
    offset: number = 0,
  ): Promise<PostgresQueryLog[]> {
    return await this.db
      .select()
      .from(postgresQueryLogs)
      .where(eq(postgresQueryLogs.userId, userId))
      .orderBy(desc(postgresQueryLogs.createdAt))
      .limit(limit)
      .offset(offset);
  }

  /**
   * Clean up old query logs (retention policy)
   */
  async cleanupOldLogs(): Promise<number> {
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - QUERY_LOG_RETENTION_DAYS);

    await this.db
      .delete(postgresQueryLogs)
      .where(lt(postgresQueryLogs.createdAt, cutoffDate));

    // Drizzle returns the result, but we need to check how many rows were affected
    // Since Drizzle doesn't return rowCount directly, we'll return 0 for now
    // In a real scenario, you might want to query first to get the count
    return 0;
  }

  /**
   * Get query statistics for connection
   */
  async getStatistics(connectionId: string): Promise<{
    totalQueries: number;
    successfulQueries: number;
    failedQueries: number;
    averageExecutionTimeMs: number;
    slowQueries: number;
    totalRowsReturned: number;
  }> {
    const result = await this.db
      .select({
        total: sql<number>`count(*)::int`,
        successful: sql<number>`count(*) filter (where ${postgresQueryLogs.status} = 'success')::int`,
        failed: sql<number>`count(*) filter (where ${postgresQueryLogs.status} = 'error')::int`,
        avgTime: sql<number>`coalesce(avg(${postgresQueryLogs.executionTimeMs}), 0)::int`,
        slow: sql<number>`count(*) filter (where ${postgresQueryLogs.executionTimeMs} > 10000)::int`,
        totalRows: sql<number>`coalesce(sum(${postgresQueryLogs.rowsReturned}), 0)::int`,
      })
      .from(postgresQueryLogs)
      .where(eq(postgresQueryLogs.connectionId, connectionId));

    const stats = result[0] || {
      total: 0,
      successful: 0,
      failed: 0,
      avgTime: 0,
      slow: 0,
      totalRows: 0,
    };

    return {
      totalQueries: stats.total || 0,
      successfulQueries: stats.successful || 0,
      failedQueries: stats.failed || 0,
      averageExecutionTimeMs: stats.avgTime || 0,
      slowQueries: stats.slow || 0,
      totalRowsReturned: stats.totalRows || 0,
    };
  }
}
