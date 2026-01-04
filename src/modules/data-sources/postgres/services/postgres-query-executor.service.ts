/**
 * PostgreSQL Query Executor Service
 * Executes queries with security, rate limiting, and monitoring
 */

import { Injectable } from '@nestjs/common';
import { QUERY_CONFIG } from '../constants/postgres.constants';
import type { QueryExecutionResult } from '../postgres.types';
import { mapErrorToStandardized } from '../utils/error-mapper.util';
import { sanitizeQuery } from '../utils/query-sanitizer.util';
import type { PostgresConnectionPoolService } from './postgres-connection-pool.service';

/**
 * Rate limit tracking
 */
interface RateLimitEntry {
  queries: number;
  resetAt: Date;
}

@Injectable()
export class PostgresQueryExecutorService {
  private rateLimits = new Map<string, RateLimitEntry>();

  constructor(private readonly connectionPoolService: PostgresConnectionPoolService) {}

  /**
   * Execute query
   */
  async executeQuery(
    connectionId: string,
    userId: string,
    query: string,
    params?: any[],
    timeout?: number,
  ): Promise<QueryExecutionResult> {
    const startTime = Date.now();

    // Sanitize and validate query
    const validation = sanitizeQuery(query);
    if (!validation.isValid) {
      throw new Error(validation.error);
    }

    // Check rate limit
    this.checkRateLimit(connectionId, userId);

    // Execute query
    try {
      const queryTimeout = timeout || QUERY_CONFIG.MAX_QUERY_LENGTH;
      const result = await this.connectionPoolService.executeQuery(
        connectionId,
        query,
        params,
        queryTimeout,
      );

      const executionTimeMs = Date.now() - startTime;

      // Extract column information
      const columns = result.fields.map((field) => ({
        name: field.name,
        dataType: field.dataTypeID ? this.getDataTypeName(field.dataTypeID) : 'unknown',
      }));

      return {
        rows: result.rows,
        rowCount: result.rowCount || 0,
        columns,
        executionTimeMs,
      };
    } catch (error) {
      const executionTimeMs = Date.now() - startTime;
      const standardized = mapErrorToStandardized(error);

      // Log query failure
      this.logQuery(connectionId, userId, query, executionTimeMs, 0, 'error', standardized.message);

      throw error;
    } finally {
      // Log successful query
      // Note: This should be done in a non-blocking way
      // For now, we'll log synchronously but in production use a queue
    }
  }

  /**
   * Execute query with explain plan
   */
  async explainQuery(connectionId: string, query: string, params?: any[]): Promise<any> {
    // Sanitize query
    const validation = sanitizeQuery(query);
    if (!validation.isValid) {
      throw new Error(validation.error);
    }

    // Prepend EXPLAIN
    const explainQuery = `EXPLAIN (FORMAT JSON) ${query}`;

    const result = await this.connectionPoolService.executeQuery(
      connectionId,
      explainQuery,
      params,
    );
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    return result.rows[0]?.['QUERY PLAN'] || result.rows[0];
  }

  /**
   * Check rate limit
   */
  private checkRateLimit(connectionId: string, userId: string): void {
    const key = `${connectionId}:${userId}`;
    const now = new Date();
    let entry = this.rateLimits.get(key);

    // Reset if expired
    if (!entry || entry.resetAt < now) {
      entry = {
        queries: 0,
        resetAt: new Date(now.getTime() + 3600000), // 1 hour
      };
      this.rateLimits.set(key, entry);
    }

    // Check limit
    if (entry.queries >= QUERY_CONFIG.RATE_LIMIT_QUERIES_PER_HOUR) {
      throw new Error('Rate limit exceeded. Maximum 100 queries per hour.');
    }

    // Increment
    entry.queries++;
  }

  /**
   * Log query execution
   */
  private logQuery(
    connectionId: string,
    userId: string,
    query: string,
    executionTimeMs: number,
    rowsReturned: number,
    status: 'success' | 'error',
    errorMessage?: string,
  ): void {
    // TODO: Implement actual logging to database
    // This should use the PostgresQueryLogRepository
    console.log({
      connectionId,
      userId,
      query: query.substring(0, 1000), // Truncate for logging
      executionTimeMs,
      rowsReturned,
      status,
      errorMessage,
    });
  }

  /**
   * Get data type name from OID
   */
  private getDataTypeName(oid: number): string {
    // Common PostgreSQL type OIDs
    const typeMap: Record<number, string> = {
      16: 'boolean',
      20: 'bigint',
      21: 'smallint',
      23: 'integer',
      25: 'text',
      700: 'real',
      701: 'double precision',
      1043: 'varchar',
      1082: 'date',
      1114: 'timestamp',
      1184: 'timestamptz',
      1700: 'numeric',
      2950: 'uuid',
      3802: 'jsonb',
    };

    return typeMap[oid] || 'unknown';
  }

  /**
   * Cancel running query
   */
  async cancelQuery(connectionId: string, pid: number): Promise<void> {
    const pool = this.connectionPoolService.getPool(connectionId);
    if (!pool) {
      throw new Error(`Pool not found for connection ${connectionId}`);
    }

    try {
      await pool.query('SELECT pg_cancel_backend($1)', [pid]);
    } catch (error) {
      throw new Error(
        `Failed to cancel query: ${error instanceof Error ? error.message : 'Unknown error'}`,
      );
    }
  }

  /**
   * Get slow queries (for monitoring)
   */
  getSlowQueries(
    _connectionId: string,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _thresholdMs: number = QUERY_CONFIG.SLOW_QUERY_THRESHOLD_MS,
  ): any[] {
    // TODO: Implement slow query tracking
    // This would require maintaining a query log in memory or database
    return [];
  }
}
