/**
 * Source Schema Types
 * Type definitions for source schema discovery and configuration
 */

import { ColumnInfo } from '../../data-sources/postgres/postgres.types';

/**
 * Source Schema Configuration
 * Defines the source data structure and location
 */
export interface SourceSchemaConfig {
  /** Source type: 'postgres', 'stripe', 'salesforce', etc. */
  sourceType: string;

  /** Source connection ID (for PostgreSQL sources) */
  sourceConnectionId?: string;

  /** Source configuration (for external sources) */
  sourceConfig?: Record<string, any>;

  /** Source schema name (database schema) */
  sourceSchema?: string;

  /** Source table name */
  sourceTable?: string;

  /** Custom SQL query for source (alternative to table) */
  sourceQuery?: string;
}

/**
 * Source Schema Discovery Result
 * Result of discovering schema from a source
 */
export interface SourceSchemaDiscovery {
  /** Source connection ID */
  connectionId: string;

  /** Source schema name */
  schema: string;

  /** Source table name */
  table: string;

  /** Discovered columns from source */
  columns: ColumnInfo[];

  /** Estimated row count */
  estimatedRowCount?: number;

  /** Table size in MB */
  sizeMB?: number;

  /** Primary key columns */
  primaryKeys: string[];

  /** Foreign key relationships */
  foreignKeys: Array<{
    column: string;
    referencedTable: string;
    referencedColumn: string;
  }>;

  /** Discovered at timestamp */
  discoveredAt: Date;
}

/**
 * Source Schema Validation Result
 */
export interface SourceSchemaValidation {
  valid: boolean;
  errors: string[];
  warnings: string[];
  columns: ColumnInfo[];
}

/**
 * Source Data Read Result
 * Result of reading data from source
 */
export interface SourceDataReadResult {
  /** Rows read from source */
  rows: any[];

  /** Total rows available (if known) */
  totalRows?: number;

  /** Columns in the data */
  columns: string[];

  /** Last sync value (for incremental syncs) */
  lastSyncValue?: string;

  /** Read metadata */
  metadata: {
    readTimeMs: number;
    query?: string;
    sourceType: string;
  };
}
