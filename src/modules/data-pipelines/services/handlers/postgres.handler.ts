/**
 * PostgreSQL Source Handler
 * Handles data collection and schema discovery for PostgreSQL databases
 */

import { Logger } from '@nestjs/common';
import { Pool } from 'pg';
import { ColumnInfo, DataSourceType } from '../../types/common.types';
import {
  BaseSourceHandler,
  CollectParams,
  CollectResult,
  ConnectionTestResult,
  PipelineSourceSchemaWithConfig,
  SchemaInfo,
} from '../../types/source-handler.types';

export class PostgresHandler extends BaseSourceHandler {
  readonly type = DataSourceType.POSTGRES;
  private readonly logger = new Logger(PostgresHandler.name);

  /**
   * Test PostgreSQL connection
   */
  async testConnection(connectionConfig: any): Promise<ConnectionTestResult> {
    const pool = this.createPool(connectionConfig);

    try {
      const client = await pool.connect();
      try {
        const result = await client.query('SELECT version()');
        const version = result.rows[0]?.version || 'Unknown';

        return {
          success: true,
          message: 'Connection successful',
          details: {
            version,
            serverInfo: { version },
          },
        };
      } finally {
        client.release();
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return {
        success: false,
        message: `Connection failed: ${message}`,
      };
    } finally {
      await pool.end();
    }
  }

  /**
   * Discover PostgreSQL schema
   */
  async discoverSchema(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
  ): Promise<SchemaInfo> {
    const pool = this.createPool(connectionConfig);

    try {
      const client = await pool.connect();
      try {
        const tableName = sourceSchema.config.table || sourceSchema.config.tableName || sourceSchema.sourceTable;
        const schemaName = sourceSchema.config.schema || sourceSchema.sourceSchema || 'public';

        this.logger.log(`Discovering schema for ${schemaName}.${tableName}`);

        // Get columns
        const columnsResult = await client.query(
          `
          SELECT 
            c.column_name,
            c.data_type,
            c.is_nullable,
            c.column_default,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale
          FROM information_schema.columns c
          WHERE c.table_schema = $1 AND c.table_name = $2
          ORDER BY c.ordinal_position
        `,
          [schemaName, tableName],
        );

        // Get primary keys
        const pkResult = await client.query(
          `
          SELECT kcu.column_name
          FROM information_schema.table_constraints tc
          JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
          WHERE tc.table_schema = $1 
            AND tc.table_name = $2 
            AND tc.constraint_type = 'PRIMARY KEY'
        `,
          [schemaName, tableName],
        );

        // Get estimated row count
        const countResult = await client.query(
          `
          SELECT reltuples::bigint as estimate
          FROM pg_class
          WHERE relname = $1
        `,
          [tableName],
        );

        const columns: ColumnInfo[] = columnsResult.rows.map(row => ({
          name: row.column_name,
          dataType: this.normalizeDataType(row.data_type),
          nullable: row.is_nullable === 'YES',
          defaultValue: row.column_default,
          maxLength: row.character_maximum_length,
          precision: row.numeric_precision,
          scale: row.numeric_scale,
          isPrimaryKey: pkResult.rows.some(pk => pk.column_name === row.column_name),
        }));

        const primaryKeys = pkResult.rows.map(row => row.column_name);
        const estimatedRowCount = countResult.rows[0]?.estimate || undefined;

        this.logger.log(`Found ${columns.length} columns, ${primaryKeys.length} primary keys`);

        return {
          columns,
          primaryKeys,
          estimatedRowCount: Number(estimatedRowCount),
          isRelational: true,
          sourceType: 'postgres',
          entityName: tableName || undefined,
        };
      } finally {
        client.release();
      }
    } catch (error) {
      this.logger.error(`Failed to discover schema: ${error}`);
      throw error;
    } finally {
      await pool.end();
    }
  }

  /**
   * Collect data from PostgreSQL
   */
  async collect(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    params: CollectParams,
  ): Promise<CollectResult> {
    const pool = this.createPool(connectionConfig);

    try {
      const client = await pool.connect();
      try {
        const tableName = sourceSchema.config.table || sourceSchema.config.tableName || sourceSchema.sourceTable;
        const schemaName = sourceSchema.config.schema || sourceSchema.sourceSchema || 'public';
        const fullTableName = `"${schemaName}"."${tableName}"`;

        let query = `SELECT * FROM ${fullTableName}`;
        const queryParams: any[] = [];
        let paramIndex = 1;

        // Add incremental filter if provided
        if (params.incrementalColumn && params.lastSyncValue) {
          query += ` WHERE "${params.incrementalColumn}" > $${paramIndex}`;
          queryParams.push(params.lastSyncValue);
          paramIndex++;
        }

        // Add ordering for consistent pagination
        query += ` ORDER BY 1`; // Order by first column

        // Add pagination
        query += ` LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`;
        queryParams.push(params.limit, params.offset);

        this.logger.log(`Executing query: ${query} with params: ${JSON.stringify(queryParams)}`);

        const result = await client.query(query, queryParams);

        // Get total count
        let totalRows: number | undefined;
        try {
          const countQuery = params.incrementalColumn && params.lastSyncValue
            ? `SELECT COUNT(*) FROM ${fullTableName} WHERE "${params.incrementalColumn}" > $1`
            : `SELECT COUNT(*) FROM ${fullTableName}`;
          const countParams = params.incrementalColumn && params.lastSyncValue 
            ? [params.lastSyncValue] 
            : [];
          const countResult = await client.query(countQuery, countParams);
          totalRows = parseInt(countResult.rows[0].count, 10);
        } catch {
          // Ignore count errors
        }

        const hasMore = result.rows.length === params.limit;
        const nextCursor = hasMore ? String(params.offset + params.limit) : undefined;

        return {
          rows: result.rows,
          totalRows,
          nextCursor,
          hasMore,
        };
      } finally {
        client.release();
      }
    } catch (error) {
      this.logger.error(`Failed to collect data: ${error}`);
      throw error;
    } finally {
      await pool.end();
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
    const pool = this.createPool(connectionConfig);
    const batchSize = params.batchSize || 1000;

    try {
      const client = await pool.connect();
      try {
        const tableName = sourceSchema.config.table || sourceSchema.config.tableName || sourceSchema.sourceTable;
        const schemaName = sourceSchema.config.schema || sourceSchema.sourceSchema || 'public';
        const fullTableName = `"${schemaName}"."${tableName}"`;

        let offset = 0;
        let hasMore = true;

        while (hasMore) {
          const query = `SELECT * FROM ${fullTableName} LIMIT ${batchSize} OFFSET ${offset}`;
          const result = await client.query(query);

          if (result.rows.length > 0) {
            yield result.rows;
            offset += result.rows.length;
          }

          hasMore = result.rows.length === batchSize;
        }
      } finally {
        client.release();
      }
    } finally {
      await pool.end();
    }
  }

  /**
   * Create a PostgreSQL connection pool
   */
  private createPool(connectionConfig: any): Pool {
    return new Pool({
      host: connectionConfig.host,
      port: connectionConfig.port || 5432,
      user: connectionConfig.username || connectionConfig.user,
      password: connectionConfig.password,
      database: connectionConfig.database,
      ssl: connectionConfig.ssl ? { rejectUnauthorized: false } : undefined,
      connectionTimeoutMillis: connectionConfig.connectionTimeout || 10000,
      max: connectionConfig.poolSize || 5,
    });
  }
}
