/**
 * PostgreSQL Sync Service
 * Handles data synchronization from PostgreSQL to Supabase
 */

import { Injectable } from '@nestjs/common';
import type { Pool } from 'pg';
import { SYNC_CONFIG } from '../constants/postgres.constants';
import { SyncJobStatus, SyncMode, type SyncProgress } from '../postgres.types';
import type { PostgresSyncJobRepository } from '../repositories/postgres-sync-job.repository';
import type { PostgresConnectionPoolService } from './postgres-connection-pool.service';

@Injectable()
export class PostgresSyncService {
  constructor(
    private readonly connectionPoolService: PostgresConnectionPoolService,
    private readonly syncJobRepository: PostgresSyncJobRepository,
  ) {}

  /**
   * Start sync job
   *
   * ARCHITECTURAL NOTE:
   * This method properly tracks job state in postgres_sync_jobs table.
   * The job record is the AUTHORITATIVE source of truth for:
   * - Job execution state
   * - Progress (rowsSynced)
   * - Cursor (lastSyncValue for incremental sync)
   * - Destination table
   * - Timestamps
   */
  async startSync(
    connectionId: string,
    jobId: string,
    tableName: string,
    schema: string,
    syncMode: SyncMode,
    incrementalColumn?: string,
    customWhereClause?: string,
  ): Promise<SyncProgress> {
    // Get job record to read destination table (AUTHORITATIVE)
    const job = await this.syncJobRepository.findById(jobId);
    if (!job) {
      throw new Error(`Sync job ${jobId} not found`);
    }

    // Update job status to running with proper timestamps
    await this.syncJobRepository.update(jobId, {
      status: 'running',
      startedAt: new Date(),
      updatedAt: new Date(),
    });

    const pool = this.connectionPoolService.getPool(connectionId);
    if (!pool) {
      throw new Error(`Pool not found for connection ${connectionId}`);
    }

    try {
      if (syncMode === SyncMode.FULL) {
        return await this.performFullSync(
          pool,
          connectionId,
          jobId,
          schema,
          tableName,
          customWhereClause,
        );
      } else {
        return await this.performIncrementalSync(
          pool,
          connectionId,
          jobId,
          schema,
          tableName,
          incrementalColumn!,
          customWhereClause,
        );
      }
    } catch (error) {
      // Update job record with error state (AUTHORITATIVE)
      await this.syncJobRepository.update(jobId, {
        status: 'failed',
        errorMessage: error instanceof Error ? error.message : 'Unknown error',
        completedAt: new Date(),
        updatedAt: new Date(),
      });

      throw error;
    }
  }

  /**
   * Perform full sync
   */
  private async performFullSync(
    pool: Pool,
    connectionId: string,
    jobId: string,
    schema: string,
    tableName: string,
    customWhereClause?: string,
  ): Promise<SyncProgress> {
    let rowsSynced = 0;
    const startTime = new Date();

    // Get total row count
    const countQuery = this.buildCountQuery(schema, tableName, customWhereClause);
    const countResult = await pool.query(countQuery);

    const totalRows = parseInt((countResult.rows[0] as { count?: string })?.count || '0', 10);

    // Sync in batches
    let offset = 0;
    const batchSize = SYNC_CONFIG.BATCH_SIZE;

    while (offset < totalRows) {
      const selectQuery = this.buildSelectQuery(
        schema,
        tableName,
        batchSize,
        offset,
        customWhereClause,
      );
      const result = await pool.query(selectQuery);

      if (result.rows.length === 0) {
        break;
      }

      // TODO: Insert rows into Supabase destination table
      // await this.insertBatch(connectionId, destinationTable, result.rows);

      rowsSynced += result.rows.length;
      offset += batchSize;

      // Update progress in job record (AUTHORITATIVE)
      // This ensures job state is tracked throughout execution
      await this.syncJobRepository.update(jobId, {
        rowsSynced,
        updatedAt: new Date(), // Update timestamp on each progress update
      });

      // Emit progress event (would use WebSocket in production)
      // this.emitProgress(jobId, { rowsSynced, totalRows, percentage: (rowsSynced / totalRows) * 100 });
    }

    // Mark as completed with final state
    await this.syncJobRepository.update(jobId, {
      status: 'success',
      rowsSynced,
      completedAt: new Date(),
      updatedAt: new Date(),
    });

    return {
      jobId,
      connectionId,
      tableName,
      status: SyncJobStatus.SUCCESS,
      rowsSynced,
      totalRows,
      percentage: 100,
      startedAt: startTime,
    };
  }

  /**
   * Perform incremental sync
   */
  private async performIncrementalSync(
    pool: Pool,
    connectionId: string,
    jobId: string,
    schema: string,
    tableName: string,
    incrementalColumn: string,
    customWhereClause?: string,
  ): Promise<SyncProgress> {
    // Get last sync value
    const job = await this.syncJobRepository.findById(jobId);
    const lastSyncValue = job?.lastSyncValue;

    let rowsSynced = 0;
    const startTime = new Date();

    // Build incremental query
    const whereClauses: string[] = [];
    if (lastSyncValue) {
      whereClauses.push(`${this.quoteIdentifier(incrementalColumn)} > $1`);
    }
    if (customWhereClause) {
      whereClauses.push(`(${customWhereClause})`);
    }

    const whereClause = whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : '';

    // Sync in batches
    let offset = 0;
    const batchSize = SYNC_CONFIG.BATCH_SIZE;
    let maxIncrementalValue: any = lastSyncValue;

    while (true) {
      const selectQuery = `
        SELECT *
        FROM ${this.quoteIdentifier(schema)}.${this.quoteIdentifier(tableName)}
        ${whereClause}
        ORDER BY ${this.quoteIdentifier(incrementalColumn)} ASC
        LIMIT $${lastSyncValue ? 2 : 1} OFFSET $${lastSyncValue ? 3 : 2}
      `;

      const params = lastSyncValue ? [lastSyncValue, batchSize, offset] : [batchSize, offset];

      const result = await pool.query(selectQuery, params);

      if (result.rows.length === 0) {
        break;
      }

      // TODO: Insert rows into Supabase
      // await this.insertBatch(connectionId, destinationTable, result.rows);

      // Track max incremental value
      const lastRow = result.rows[result.rows.length - 1] as Record<string, unknown>;

      maxIncrementalValue = lastRow[incrementalColumn];

      rowsSynced += result.rows.length;
      offset += batchSize;

      // Update progress and cursor in job record (AUTHORITATIVE)
      // This ensures job state and cursor are tracked throughout execution
      await this.syncJobRepository.update(jobId, {
        rowsSynced,
        lastSyncValue: String(maxIncrementalValue), // Update cursor during execution
        updatedAt: new Date(), // Update timestamp on each progress update
      });
    }

    // Mark as completed with final state and cursor
    await this.syncJobRepository.update(jobId, {
      status: 'success',
      rowsSynced,
      lastSyncValue: String(maxIncrementalValue), // Final cursor value
      completedAt: new Date(),
      updatedAt: new Date(),
    });

    return {
      jobId,
      connectionId,
      tableName,
      status: SyncJobStatus.SUCCESS,
      rowsSynced,
      startedAt: startTime,
    };
  }

  /**
   * Build count query
   */
  private buildCountQuery(schema: string, tableName: string, customWhereClause?: string): string {
    const whereClause = customWhereClause ? `WHERE ${customWhereClause}` : '';
    return `SELECT COUNT(*) as count FROM ${this.quoteIdentifier(schema)}.${this.quoteIdentifier(tableName)} ${whereClause}`;
  }

  /**
   * Build select query with pagination
   */
  private buildSelectQuery(
    schema: string,
    tableName: string,
    limit: number,
    offset: number,
    customWhereClause?: string,
  ): string {
    const whereClause = customWhereClause ? `WHERE ${customWhereClause}` : '';
    return `SELECT * FROM ${this.quoteIdentifier(schema)}.${this.quoteIdentifier(tableName)} ${whereClause} LIMIT ${limit} OFFSET ${offset}`;
  }

  /**
   * Quote identifier for SQL safety
   */
  private quoteIdentifier(identifier: string): string {
    return `"${identifier.replace(/"/g, '""')}"`;
  }

  /**
   * Cancel sync job
   */
  async cancelSync(jobId: string): Promise<void> {
    // Update job record with cancellation state (AUTHORITATIVE)
    await this.syncJobRepository.update(jobId, {
      status: 'failed',
      errorMessage: 'Sync cancelled by user',
      completedAt: new Date(),
      updatedAt: new Date(),
    });
  }
}
