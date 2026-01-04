/**
 * Destination Schema Types
 * Type definitions for destination schema configuration and management
 */

import { ColumnMapping, WriteResult } from '../../data-sources/postgres/postgres.types';

/**
 * Destination Schema Configuration
 * Defines the destination data structure and location
 */
export interface DestinationSchemaConfig {
  /** Destination connection ID (PostgreSQL) */
  destinationConnectionId: string;

  /** Destination schema name (database schema) */
  destinationSchema: string;

  /** Destination table name */
  destinationTable: string;

  /** Whether destination table already exists */
  destinationTableExists: boolean;
}

/**
 * Destination Schema Definition
 * Complete schema definition for destination table
 */
export interface DestinationSchemaDefinition {
  /** Schema name */
  schema: string;

  /** Table name */
  table: string;

  /** Column definitions */
  columns: Array<{
    name: string;
    dataType: string;
    nullable: boolean;
    defaultValue?: string;
    isPrimaryKey?: boolean;
    maxLength?: number;
  }>;

  /** Primary key columns */
  primaryKeys: string[];

  /** Indexes */
  indexes: Array<{
    name: string;
    columns: string[];
    unique?: boolean;
  }>;
}

/**
 * Destination Schema Creation Result
 */
export interface DestinationSchemaCreationResult {
  success: boolean;
  schema: string;
  table: string;
  columnsCreated: number;
  message?: string;
  errors?: string[];
}

/**
 * Destination Schema Validation Result
 */
export interface DestinationSchemaValidation {
  valid: boolean;
  errors: string[];
  warnings: string[];
  missingColumns: string[];
  typeMismatches: Array<{
    column: string;
    expectedType: string;
    actualType: string;
  }>;
  existingColumns: string[];
}

/**
 * Destination Write Configuration
 */
export interface DestinationWriteConfig {
  /** Write mode: append, upsert, or replace */
  writeMode: 'append' | 'upsert' | 'replace';

  /** Columns for upsert key (if writeMode is 'upsert') */
  upsertKey?: string[];

  /** Column mappings from source to destination */
  columnMappings: ColumnMapping[];
}

/**
 * Destination Write Result
 * Extended write result with schema information
 */
export interface DestinationWriteResult extends WriteResult {
  /** Destination schema */
  destinationSchema: string;

  /** Destination table */
  destinationTable: string;

  /** Columns written */
  columnsWritten: string[];

  /** Schema changes applied */
  schemaChanges?: {
    columnsAdded: string[];
    columnsModified: string[];
  };
}

/**
 * Destination Schema Comparison
 * Comparison between source and destination schemas
 */
export interface DestinationSchemaComparison {
  /** Columns that exist in both */
  matchingColumns: Array<{
    sourceColumn: string;
    destinationColumn: string;
    typeCompatible: boolean;
  }>;

  /** Columns only in source */
  sourceOnlyColumns: string[];

  /** Columns only in destination */
  destinationOnlyColumns: string[];

  /** Type mismatches */
  typeMismatches: Array<{
    sourceColumn: string;
    destinationColumn: string;
    sourceType: string;
    destinationType: string;
  }>;

  /** Suggested mappings */
  suggestedMappings: ColumnMapping[];
}
