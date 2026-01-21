/**
 * MySQL Source Handler
 * Handles data collection and schema discovery for MySQL databases
 */

import { Logger } from '@nestjs/common';
import * as mysql from 'mysql2/promise';
import { ColumnInfo, DataSourceType } from '../../types/common.types';
import {
  BaseSourceHandler,
  CollectParams,
  CollectResult,
  ConnectionTestResult,
  PipelineSourceSchemaWithConfig,
  SchemaInfo,
} from '../../types/source-handler.types';

export class MySQLHandler extends BaseSourceHandler {
  readonly type = DataSourceType.MYSQL;
  private readonly logger = new Logger(MySQLHandler.name);

  /**
   * Test MySQL connection
   */
  async testConnection(connectionConfig: any): Promise<ConnectionTestResult> {
    let connection: mysql.Connection | null = null;

    try {
      connection = await mysql.createConnection(this.getConnectionOptions(connectionConfig));
      const [rows] = await connection.query('SELECT VERSION() as version');
      const version = (rows as any[])[0]?.version || 'Unknown';

      return {
        success: true,
        message: 'Connection successful',
        details: {
          version,
          serverInfo: { version },
        },
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return {
        success: false,
        message: `Connection failed: ${message}`,
      };
    } finally {
      if (connection) {
        await connection.end();
      }
    }
  }

  /**
   * Discover MySQL schema
   */
  async discoverSchema(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
  ): Promise<SchemaInfo> {
    let connection: mysql.Connection | null = null;

    try {
      connection = await mysql.createConnection(this.getConnectionOptions(connectionConfig));
      const tableName = sourceSchema.config.table || sourceSchema.config.tableName || sourceSchema.sourceTable;
      const database = connectionConfig.database;

      this.logger.log(`Discovering schema for ${database}.${tableName}`);

      // Get columns
      const [columnsRows] = await connection.query(
        `
        SELECT 
          COLUMN_NAME,
          DATA_TYPE,
          IS_NULLABLE,
          COLUMN_DEFAULT,
          CHARACTER_MAXIMUM_LENGTH,
          NUMERIC_PRECISION,
          NUMERIC_SCALE,
          COLUMN_KEY
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
      `,
        [database, tableName],
      );

      // Get estimated row count
      const [countRows] = await connection.query(
        `
        SELECT TABLE_ROWS as estimate
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
      `,
        [database, tableName],
      );

      const columns: ColumnInfo[] = (columnsRows as any[]).map(row => ({
        name: row.COLUMN_NAME,
        dataType: this.normalizeDataType(row.DATA_TYPE),
        nullable: row.IS_NULLABLE === 'YES',
        defaultValue: row.COLUMN_DEFAULT,
        maxLength: row.CHARACTER_MAXIMUM_LENGTH,
        precision: row.NUMERIC_PRECISION,
        scale: row.NUMERIC_SCALE,
        isPrimaryKey: row.COLUMN_KEY === 'PRI',
      }));

      const primaryKeys = columns.filter(c => c.isPrimaryKey).map(c => c.name);
      const estimatedRowCount = (countRows as any[])[0]?.estimate;

      this.logger.log(`Found ${columns.length} columns, ${primaryKeys.length} primary keys`);

      return {
        columns,
        primaryKeys,
        estimatedRowCount: Number(estimatedRowCount),
        isRelational: true,
        sourceType: 'mysql',
        entityName: tableName || undefined,
      };
    } catch (error) {
      this.logger.error(`Failed to discover schema: ${error}`);
      throw error;
    } finally {
      if (connection) {
        await connection.end();
      }
    }
  }

  /**
   * Collect data from MySQL
   */
  async collect(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    params: CollectParams,
  ): Promise<CollectResult> {
    let connection: mysql.Connection | null = null;

    try {
      connection = await mysql.createConnection(this.getConnectionOptions(connectionConfig));
      const tableName = sourceSchema.config.table || sourceSchema.config.tableName || sourceSchema.sourceTable;

      let query = `SELECT * FROM \`${tableName}\``;
      const queryParams: any[] = [];

      // Add incremental filter if provided
      if (params.incrementalColumn && params.lastSyncValue) {
        query += ` WHERE \`${params.incrementalColumn}\` > ?`;
        queryParams.push(params.lastSyncValue);
      }

      // Add ordering for consistent pagination
      query += ` ORDER BY 1`;

      // Add pagination
      query += ` LIMIT ? OFFSET ?`;
      queryParams.push(params.limit, params.offset);

      this.logger.log(`Executing query: ${query}`);

      const [rows] = await connection.query(query, queryParams);

      // Get total count
      let totalRows: number | undefined;
      try {
        const countQuery = params.incrementalColumn && params.lastSyncValue
          ? `SELECT COUNT(*) as count FROM \`${tableName}\` WHERE \`${params.incrementalColumn}\` > ?`
          : `SELECT COUNT(*) as count FROM \`${tableName}\``;
        const countParams = params.incrementalColumn && params.lastSyncValue 
          ? [params.lastSyncValue] 
          : [];
        const [countResult] = await connection.query(countQuery, countParams);
        totalRows = (countResult as any[])[0]?.count;
      } catch {
        // Ignore count errors
      }

      const resultRows = rows as any[];
      const hasMore = resultRows.length === params.limit;
      const nextCursor = hasMore ? String(params.offset + params.limit) : undefined;

      return {
        rows: resultRows,
        totalRows,
        nextCursor,
        hasMore,
      };
    } catch (error) {
      this.logger.error(`Failed to collect data: ${error}`);
      throw error;
    } finally {
      if (connection) {
        await connection.end();
      }
    }
  }

  /**
   * Collect incremental data (new/changed records since checkpoint)
   * Implements strict incremental filtering: WHERE watermarkField > lastValue
   * 
   * Root Fix: This ensures only new/changed records are collected, preventing re-writing all data
   */
  async collectIncremental(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    checkpoint: { watermarkField: string; lastValue: string | number; pauseTimestamp?: string },
    params: Omit<CollectParams, 'incrementalColumn' | 'lastSyncValue'>,
  ): Promise<CollectResult> {
    let connection: mysql.Connection | null = null;

    try {
      connection = await mysql.createConnection(this.getConnectionOptions(connectionConfig));
      const tableName = sourceSchema.config.table || sourceSchema.config.tableName || sourceSchema.sourceTable;

      // Determine the effective last value (consider pause timestamp)
      let effectiveLastValue: string | number = checkpoint.lastValue;
      if (checkpoint.pauseTimestamp) {
        const pauseDate = new Date(checkpoint.pauseTimestamp);
        const lastValueDate = typeof checkpoint.lastValue === 'string' || typeof checkpoint.lastValue === 'number'
          ? new Date(checkpoint.lastValue)
          : new Date();
        
        effectiveLastValue = pauseDate < lastValueDate 
          ? checkpoint.pauseTimestamp 
          : (typeof checkpoint.lastValue === 'string' ? checkpoint.lastValue : String(checkpoint.lastValue));
      }

      // Build strict incremental query: WHERE watermarkField > lastValue
      let query = `SELECT * FROM \`${tableName}\``;
      const queryParams: any[] = [];

      // Strict incremental filter - only records newer than checkpoint
      query += ` WHERE \`${checkpoint.watermarkField}\` > ?`;
      queryParams.push(effectiveLastValue);

      // Order by watermark field for consistent pagination
      query += ` ORDER BY \`${checkpoint.watermarkField}\` ASC`;

      // Add pagination
      query += ` LIMIT ? OFFSET ?`;
      queryParams.push(params.limit, params.offset);

      this.logger.log(`Incremental query: ${query} with params: ${JSON.stringify(queryParams)}`);

      const [rows] = await connection.query(query, queryParams);

      // Get total count for incremental records
      let totalRows: number | undefined;
      try {
        const countQuery = `SELECT COUNT(*) as count FROM \`${tableName}\` WHERE \`${checkpoint.watermarkField}\` > ?`;
        const [countResult] = await connection.query(countQuery, [effectiveLastValue]);
        totalRows = (countResult as any[])[0]?.count;
      } catch {
        // Ignore count errors
      }

      const resultRows = rows as any[];
      const hasMore = resultRows.length === params.limit;
      const nextCursor = hasMore ? String(params.offset + params.limit) : undefined;

      this.logger.log(
        `Incremental sync: Found ${resultRows.length} new records (total available: ${totalRows || 'unknown'})`,
      );

      return {
        rows: resultRows,
        totalRows,
        nextCursor,
        hasMore,
      };
    } catch (error) {
      this.logger.error(`Failed to collect incremental data: ${error}`);
      throw error;
    } finally {
      if (connection) {
        await connection.end();
      }
    }
  }

  /**
   * Stream data using async generator for large datasets
   */
  async *collectStream(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    params: CollectParams,
  ): AsyncIterable<any[]> {
    const batchSize = params.batchSize || 1000;
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const result = await this.collect(sourceSchema, connectionConfig, {
        ...params,
        limit: batchSize,
        offset,
      });

      if (result.rows.length > 0) {
        yield result.rows;
        offset += result.rows.length;
      }

      hasMore = result.hasMore || false;
    }
  }

  /**
   * Get MySQL connection options
   */
  private getConnectionOptions(connectionConfig: any): mysql.ConnectionOptions {
    return {
      host: connectionConfig.host,
      port: connectionConfig.port || 3306,
      user: connectionConfig.username || connectionConfig.user,
      password: connectionConfig.password,
      database: connectionConfig.database,
      ssl: connectionConfig.ssl ? { rejectUnauthorized: false } : undefined,
      connectTimeout: connectionConfig.connectionTimeout || 10000,
    };
  }
}
