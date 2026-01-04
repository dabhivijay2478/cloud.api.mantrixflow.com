/**
 * PostgreSQL Schema Discovery Service
 * Discovers databases, schemas, tables, columns, and relationships
 */

import { Injectable } from '@nestjs/common';
import { Pool } from 'pg';
import {
  SchemaDiscoveryResult,
  DatabaseInfo,
  SchemaInfo,
  TableInfo,
  ColumnInfo,
  IndexInfo,
} from '../postgres.types';
import { PostgresConnectionPoolService } from './postgres-connection-pool.service';
import {
  mapPostgresTypeToTypeScript,
  isArrayType,
  isJsonbType,
} from '../utils/postgres-type-mapper.util';
import { SCHEMA_DISCOVERY } from '../constants/postgres.constants';

@Injectable()
export class PostgresSchemaDiscoveryService {
  constructor(
    private readonly connectionPoolService: PostgresConnectionPoolService,
  ) {}

  /**
   * Discover complete schema
   */
  async discoverSchema(
    connectionId: string,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _forceRefresh = false,
  ): Promise<SchemaDiscoveryResult> {
    const pool = this.connectionPoolService.getPool(connectionId);
    if (!pool) {
      throw new Error(`Pool not found for connection ${connectionId}`);
    }

    try {
      // Discover databases
      const databases = await this.discoverDatabases(pool);

      // Discover schemas
      const schemas = await this.discoverSchemas(pool);

      // Discover tables from all schemas
      const tables = await this.discoverAllTables(pool);

      return {
        databases,
        schemas,
        tables,
        cached: false,
        cachedAt: new Date(),
      };
    } catch (error) {
      throw new Error(
        `Schema discovery failed: ${error instanceof Error ? error.message : 'Unknown error'}`,
      );
    }
  }

  /**
   * Discover databases
   */
  async discoverDatabases(pool: Pool): Promise<DatabaseInfo[]> {
    const query = `
      SELECT 
        datname as name,
        pg_size_pretty(pg_database_size(datname)) as size,
        pg_encoding_to_char(encoding) as encoding,
        datcollate as collation
      FROM pg_database
      WHERE datistemplate = false
      ORDER BY datname;
    `;

    try {
      const result = await pool.query(query);

      return result.rows.map((row: Record<string, unknown>) => ({
        name: row.name as string,
        size: String(row.size as number),
        encoding: row.encoding as string,
        collation: row.collation as string,
      }));
    } catch (error) {
      // If permission denied, return empty array
      if (error instanceof Error && error.message.includes('permission')) {
        return [];
      }
      throw error;
    }
  }

  /**
   * Discover schemas
   */
  async discoverSchemas(pool: Pool): Promise<SchemaInfo[]> {
    const query = `
      SELECT 
        nspname as name,
        nspowner::regrole::text as owner
      FROM pg_namespace
      WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND nspname NOT LIKE 'pg_temp_%'
        AND nspname NOT LIKE 'pg_toast_temp_%'
      ORDER BY nspname;
    `;

    try {
      const result = await pool.query(query);

      return result.rows.map((row: Record<string, unknown>) => ({
        name: row.name as string,

        owner: row.owner as string,
      }));
    } catch (error) {
      if (error instanceof Error && error.message.includes('permission')) {
        return [];
      }
      throw error;
    }
  }

  /**
   * Discover all tables from all schemas
   */
  async discoverAllTables(pool: Pool): Promise<TableInfo[]> {
    const tablesQuery = `
      SELECT 
        t.table_schema as schema,
        t.table_name as name,
        CASE 
          WHEN t.table_type = 'VIEW' THEN true
          ELSE false
        END as is_view,
        CASE 
          WHEN t.table_type = 'MATERIALIZED VIEW' THEN true
          ELSE false
        END as is_materialized_view,
        CASE 
          WHEN pt.oid IS NOT NULL THEN true
          ELSE false
        END as is_partitioned,
        pt.relname as parent_table
      FROM information_schema.tables t
      LEFT JOIN pg_class pc ON pc.relname = t.table_name
      LEFT JOIN pg_namespace pn ON pn.oid = pc.relnamespace AND pn.nspname = t.table_schema
      LEFT JOIN pg_inherits pi ON pi.inhrelid = pc.oid
      LEFT JOIN pg_class pt ON pt.oid = pi.inhparent
      WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND t.table_schema NOT LIKE 'pg_temp_%'
        AND t.table_schema NOT LIKE 'pg_toast_temp_%'
        AND t.table_type IN ('BASE TABLE', 'VIEW', 'MATERIALIZED VIEW')
      ORDER BY t.table_schema, t.table_name
      LIMIT $1;
    `;

    try {
      const result = await pool.query(tablesQuery, [
        SCHEMA_DISCOVERY.MAX_TABLES,
      ]);
      const tables: TableInfo[] = [];

      for (const row of result.rows) {
        const tableInfo = await this.discoverTableDetails(
          pool,
          // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
          row.schema as string,
          // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
          row.name as string,
          {
            // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
            isView: row.is_view as boolean,
            // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
            isMaterializedView: row.is_materialized_view as boolean,
            // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
            isPartitioned: row.is_partitioned as boolean,
            // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
            parentTable: row.parent_table as string | undefined,
          },
        );
        tables.push(tableInfo);
      }

      return tables;
    } catch (error) {
      if (error instanceof Error && error.message.includes('permission')) {
        return [];
      }
      throw error;
    }
  }

  /**
   * Discover all tables with metadata from a specific schema
   */
  async discoverTables(
    pool: Pool,
    schema: string = 'public',
  ): Promise<TableInfo[]> {
    const tablesQuery = `
      SELECT 
        t.table_schema as schema,
        t.table_name as name,
        CASE 
          WHEN t.table_type = 'VIEW' THEN true
          ELSE false
        END as is_view,
        CASE 
          WHEN t.table_type = 'MATERIALIZED VIEW' THEN true
          ELSE false
        END as is_materialized_view,
        CASE 
          WHEN pt.oid IS NOT NULL THEN true
          ELSE false
        END as is_partitioned,
        pt.relname as parent_table
      FROM information_schema.tables t
      LEFT JOIN pg_class pc ON pc.relname = t.table_name
      LEFT JOIN pg_namespace pn ON pn.oid = pc.relnamespace AND pn.nspname = t.table_schema
      LEFT JOIN pg_inherits pi ON pi.inhrelid = pc.oid
      LEFT JOIN pg_class pt ON pt.oid = pi.inhparent
      WHERE t.table_schema = $1
        AND t.table_type IN ('BASE TABLE', 'VIEW', 'MATERIALIZED VIEW')
      ORDER BY t.table_name
      LIMIT $2;
    `;

    try {
      const result = await pool.query(tablesQuery, [
        schema,
        SCHEMA_DISCOVERY.MAX_TABLES,
      ]);
      const tables: TableInfo[] = [];

      for (const row of result.rows) {
        const tableInfo = await this.discoverTableDetails(
          pool,
          // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
          row.schema as string,
          // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
          row.name as string,
          {
            // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
            isView: row.is_view as boolean,
            // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
            isMaterializedView: row.is_materialized_view as boolean,
            // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
            isPartitioned: row.is_partitioned as boolean,
            // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
            parentTable: row.parent_table as string | undefined,
          },
        );
        tables.push(tableInfo);
      }

      return tables;
    } catch (error) {
      if (error instanceof Error && error.message.includes('permission')) {
        return [];
      }
      throw error;
    }
  }

  /**
   * Check if a schema exists
   */
  async schemaExists(pool: Pool, schemaName: string): Promise<boolean> {
    const query = `
      SELECT COUNT(*) as count
      FROM pg_namespace
      WHERE nspname = $1
        AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND nspname NOT LIKE 'pg_temp_%'
        AND nspname NOT LIKE 'pg_toast_temp_%';
    `;

    try {
      const result = await pool.query(query, [schemaName]);

      return parseInt((result.rows[0] as { count?: string })?.count || '0') > 0;
    } catch {
      return false;
    }
  }

  /**
   * Discover detailed table information
   */
  private async discoverTableDetails(
    pool: Pool,
    schema: string,
    tableName: string,
    metadata: {
      isView: boolean;
      isMaterializedView: boolean;
      isPartitioned: boolean;
      parentTable?: string;
    },
  ): Promise<TableInfo> {
    // Get columns
    const columns = await this.discoverColumns(pool, schema, tableName);

    // Get primary keys
    const primaryKeys = await this.discoverPrimaryKeys(pool, schema, tableName);

    // Get foreign keys
    const foreignKeys = await this.discoverForeignKeys(pool, schema, tableName);

    // Get indexes
    const indexes = await this.discoverIndexes(pool, schema, tableName);

    // Get row count and size
    const { rowCount, size, sizeFormatted } = await this.getTableStats(
      pool,
      schema,
      tableName,
    );

    // Get last updated (if available)
    const lastUpdated = await this.getLastUpdated(
      pool,
      schema,
      tableName,
      columns,
    );

    return {
      name: tableName,
      schema,
      rowCount,
      size,
      sizeFormatted,
      columns,
      primaryKeys,
      foreignKeys,
      indexes,
      isView: metadata.isView,
      isMaterializedView: metadata.isMaterializedView,
      isPartitioned: metadata.isPartitioned,
      parentTable: metadata.parentTable,
      lastUpdated,
    };
  }

  /**
   * Discover columns for a table
   */
  private async discoverColumns(
    pool: Pool,
    schema: string,
    tableName: string,
  ): Promise<ColumnInfo[]> {
    const query = `
      SELECT 
        c.column_name as name,
        c.data_type as data_type,
        c.character_maximum_length as max_length,
        c.numeric_precision as numeric_precision,
        c.numeric_scale as numeric_scale,
        c.is_nullable = 'YES' as is_nullable,
        c.column_default as default_value,
        CASE 
          WHEN t.typcategory = 'A' THEN true
          ELSE false
        END as is_array,
        CASE 
          WHEN c.data_type = 'jsonb' OR c.data_type = 'json' THEN true
          ELSE false
        END as is_jsonb,
        CASE 
          WHEN t.typtype = 'e' THEN true
          ELSE false
        END as is_enum,
        (SELECT array_agg(e.enumlabel ORDER BY e.enumsortorder)
         FROM pg_enum e
         WHERE e.enumtypid = t.oid) as enum_values
      FROM information_schema.columns c
      LEFT JOIN pg_type t ON t.typname = c.udt_name
      LEFT JOIN pg_namespace n ON n.oid = t.typnamespace AND n.nspname = c.table_schema
      WHERE c.table_schema = $1
        AND c.table_name = $2
      ORDER BY c.ordinal_position;
    `;

    try {
      const result = await pool.query(query, [schema, tableName]);
      const columns: ColumnInfo[] = [];

      for (const row of result.rows as Array<Record<string, unknown>>) {
        const isArray =
          isArrayType(row.data_type as string) || (row.is_array as boolean);

        const isJsonb =
          isJsonbType(row.data_type as string) || (row.is_jsonb as boolean);

        const tsType = mapPostgresTypeToTypeScript(
          row.data_type as string,
          isArray,
          isJsonb,
        );

        // Check if column is primary key or foreign key
        const isPrimaryKey = await this.isPrimaryKey(
          pool,
          schema,
          tableName,

          row.name as string,
        );
        const foreignKeyInfo = await this.getForeignKeyInfo(
          pool,
          schema,
          tableName,

          row.name as string,
        );

        columns.push({
          name: row.name as string,

          dataType: row.data_type as string,
          tsType,

          isNullable: row.is_nullable as boolean,
          isPrimaryKey,
          isForeignKey: !!foreignKeyInfo,
          foreignKeyTable: foreignKeyInfo?.referencedTable,
          foreignKeyColumn: foreignKeyInfo?.referencedColumn,

          defaultValue: row.default_value as string | undefined,

          maxLength: row.max_length as number | undefined,

          numericPrecision: row.numeric_precision as number | undefined,

          numericScale: row.numeric_scale as number | undefined,
          isArray,
          isJsonb,

          isEnum: row.is_enum as boolean,

          enumValues: (row.enum_values as string[] | undefined) || undefined,
        });
      }

      return columns;
    } catch {
      return [];
    }
  }

  /**
   * Check if column is primary key
   */
  private async isPrimaryKey(
    pool: Pool,
    schema: string,
    tableName: string,
    columnName: string,
  ): Promise<boolean> {
    const query = `
      SELECT COUNT(*) as count
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu 
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = $1
        AND tc.table_name = $2
        AND kcu.column_name = $3;
    `;

    const result = await pool.query(query, [schema, tableName, columnName]);

    return parseInt((result.rows[0] as { count?: string })?.count || '0') > 0;
  }

  /**
   * Get foreign key information
   */
  private async getForeignKeyInfo(
    pool: Pool,
    schema: string,
    tableName: string,
    columnName: string,
  ): Promise<{ referencedTable: string; referencedColumn: string } | null> {
    const query = `
      SELECT 
        kcu2.table_name as referenced_table,
        kcu2.column_name as referenced_column
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu1
        ON tc.constraint_name = kcu1.constraint_name
        AND tc.table_schema = kcu1.table_schema
      JOIN information_schema.referential_constraints rc
        ON tc.constraint_name = rc.constraint_name
        AND tc.table_schema = rc.constraint_schema
      JOIN information_schema.key_column_usage kcu2
        ON rc.unique_constraint_name = kcu2.constraint_name
        AND rc.unique_constraint_schema = kcu2.table_schema
      WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = $1
        AND tc.table_name = $2
        AND kcu1.column_name = $3;
    `;

    const result = await pool.query(query, [schema, tableName, columnName]);
    if (result.rows.length > 0) {
      const row = result.rows[0] as Record<string, unknown>;
      return {
        referencedTable: row.referenced_table as string,

        referencedColumn: row.referenced_column as string,
      };
    }

    return null;
  }

  /**
   * Discover primary keys
   */
  private async discoverPrimaryKeys(
    pool: Pool,
    schema: string,
    tableName: string,
  ): Promise<string[]> {
    const query = `
      SELECT kcu.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = $1
        AND tc.table_name = $2
      ORDER BY kcu.ordinal_position;
    `;

    try {
      const result = await pool.query(query, [schema, tableName]);

      return result.rows.map(
        (row: Record<string, unknown>) => row.column_name as string,
      );
    } catch {
      return [];
    }
  }

  /**
   * Discover foreign keys
   */
  private async discoverForeignKeys(
    pool: Pool,
    schema: string,
    tableName: string,
  ): Promise<
    Array<{ column: string; referencedTable: string; referencedColumn: string }>
  > {
    const query = `
      SELECT 
        kcu1.column_name as column,
        kcu2.table_name as referenced_table,
        kcu2.column_name as referenced_column
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu1
        ON tc.constraint_name = kcu1.constraint_name
        AND tc.table_schema = kcu1.table_schema
      JOIN information_schema.referential_constraints rc
        ON tc.constraint_name = rc.constraint_name
        AND tc.table_schema = rc.constraint_schema
      JOIN information_schema.key_column_usage kcu2
        ON rc.unique_constraint_name = kcu2.constraint_name
        AND rc.unique_constraint_schema = kcu2.table_schema
      WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = $1
        AND tc.table_name = $2;
    `;

    try {
      const result = await pool.query(query, [schema, tableName]);

      return result.rows.map((row: Record<string, unknown>) => ({
        column: row.column as string,

        referencedTable: row.referenced_table as string,

        referencedColumn: row.referenced_column as string,
      }));
    } catch {
      return [];
    }
  }

  /**
   * Discover indexes
   */
  private async discoverIndexes(
    pool: Pool,
    schema: string,
    tableName: string,
  ): Promise<IndexInfo[]> {
    const query = `
      SELECT 
        i.relname as name,
        array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as columns,
        ix.indisunique as is_unique,
        ix.indisprimary as is_primary
      FROM pg_class t
      JOIN pg_index ix ON t.oid = ix.indrelid
      JOIN pg_class i ON i.oid = ix.indexrelid
      JOIN pg_namespace n ON n.oid = t.relnamespace
      JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
      WHERE n.nspname = $1
        AND t.relname = $2
        AND a.attnum > 0
      GROUP BY i.relname, ix.indisunique, ix.indisprimary
      ORDER BY i.relname;
    `;

    try {
      const result = await pool.query(query, [schema, tableName]);

      return result.rows.map((row: Record<string, unknown>) => ({
        name: row.name as string,

        columns: row.columns as string[],

        isUnique: row.is_unique as boolean,

        isPrimary: row.is_primary as boolean,
      }));
    } catch {
      return [];
    }
  }

  /**
   * Get table statistics (row count and size)
   */
  private async getTableStats(
    pool: Pool,
    schema: string,
    tableName: string,
  ): Promise<{ rowCount: number; size: number; sizeFormatted: string }> {
    const query = `
      SELECT 
        n_live_tup as row_count,
        pg_total_relation_size(quote_ident($1) || '.' || quote_ident($2)) as size_bytes
      FROM pg_stat_user_tables
      WHERE schemaname = $1
        AND relname = $2;
    `;

    try {
      const result = await pool.query(query, [schema, tableName]);
      if (result.rows.length > 0) {
        const row = result.rows[0] as Record<string, unknown>;

        const rowCount = parseInt((row.row_count as string | undefined) || '0');

        const sizeBytes = parseInt(
          (row.size_bytes as string | undefined) || '0',
        );
        const sizeFormatted = this.formatBytes(sizeBytes);

        return { rowCount, size: sizeBytes, sizeFormatted };
      }
    } catch {
      // Fallback: use approximate count
    }

    // Fallback query
    try {
      const countQuery = `SELECT COUNT(*) as count FROM ${this.quoteIdentifier(schema)}.${this.quoteIdentifier(tableName)}`;
      const countResult = await pool.query(countQuery);

      const rowCount = parseInt(
        (countResult.rows[0] as { count?: string })?.count || '0',
      );

      return { rowCount, size: 0, sizeFormatted: '0 B' };
    } catch {
      return { rowCount: 0, size: 0, sizeFormatted: '0 B' };
    }
  }

  /**
   * Get last updated timestamp (if table has updated_at column)
   */
  private async getLastUpdated(
    pool: Pool,
    schema: string,
    tableName: string,
    columns: ColumnInfo[],
  ): Promise<Date | undefined> {
    const updatedAtColumn = columns.find(
      (col) =>
        col.name.toLowerCase().includes('updated_at') ||
        col.name.toLowerCase().includes('modified_at') ||
        (col.dataType.includes('timestamp') &&
          col.name.toLowerCase().includes('at')),
    );

    if (!updatedAtColumn) {
      return undefined;
    }

    try {
      const query = `SELECT MAX(${this.quoteIdentifier(updatedAtColumn.name)}) as last_updated 
                     FROM ${this.quoteIdentifier(schema)}.${this.quoteIdentifier(tableName)}`;
      const result = await pool.query(query);
      const row = result.rows[0] as
        | { last_updated?: string | number | Date }
        | undefined;

      if (row?.last_updated) {
        return new Date(row.last_updated);
      }
    } catch {
      // Ignore errors
    }

    return undefined;
  }

  /**
   * Format bytes to human-readable format
   */
  private formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
  }

  /**
   * Quote identifier for SQL safety
   */
  private quoteIdentifier(identifier: string): string {
    return `"${identifier.replace(/"/g, '""')}"`;
  }
}
