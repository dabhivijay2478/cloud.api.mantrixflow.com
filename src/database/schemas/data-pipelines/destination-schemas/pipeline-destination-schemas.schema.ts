import {
  pgTable,
  uuid,
  varchar,
  text,
  timestamp,
  jsonb,
  boolean,
} from 'drizzle-orm/pg-core';
import { postgresConnections } from '../../data-sources/connections/postgres-connections.schema';

/**
 * Pipeline Destination Schemas Table
 * Stores destination schema configurations and definitions
 * 
 * This table separates destination configuration from pipeline configuration,
 * allowing for better organization and reuse of destination schemas.
 */
export const pipelineDestinationSchemas = pgTable('pipeline_destination_schemas', {
  id: uuid('id').primaryKey().defaultRandom(),
  orgId: uuid('org_id').notNull(),
  userId: uuid('user_id').notNull(),
  
  // ============================================================================
  // DESTINATION CONNECTION
  // ============================================================================
  /** Destination connection ID (PostgreSQL connection) */
  destinationConnectionId: uuid('destination_connection_id')
    .notNull()
    .references(() => postgresConnections.id, { onDelete: 'cascade' }),
  
  // ============================================================================
  // DESTINATION LOCATION
  // ============================================================================
  /** Destination database schema name (default: 'public') */
  destinationSchema: varchar('destination_schema', { length: 255 }).default(
    'public',
  ),
  
  /** Destination table name */
  destinationTable: varchar('destination_table', { length: 255 }).notNull(),
  
  /** Whether destination table already exists */
  destinationTableExists: boolean('destination_table_exists').default(false),
  
  // ============================================================================
  // SCHEMA DEFINITION
  // ============================================================================
  /** Column definitions for destination table */
  columnDefinitions: jsonb('column_definitions').$type<ColumnDefinition[]>(),
  
  /** Primary key columns */
  primaryKeys: jsonb('primary_keys').$type<string[]>(),
  
  /** Index definitions */
  indexes: jsonb('indexes').$type<IndexDefinition[]>(),
  
  /** Column mappings from source to destination */
  columnMappings: jsonb('column_mappings').$type<ColumnMapping[]>(),
  
  // ============================================================================
  // WRITE CONFIGURATION
  // ============================================================================
  /** Write mode: 'append', 'upsert', 'replace' */
  writeMode: varchar('write_mode', { length: 50 }).default('append'),
  
  /** Columns for upsert key (if writeMode is 'upsert') */
  upsertKey: jsonb('upsert_key').$type<string[]>(),
  
  // ============================================================================
  // VALIDATION & STATUS
  // ============================================================================
  /** Schema validation result */
  validationResult: jsonb('validation_result').$type<DestinationSchemaValidationResult>(),
  
  /** Last time schema was validated */
  lastValidatedAt: timestamp('last_validated_at'),
  
  /** Last time schema was created/updated in database */
  lastSyncedAt: timestamp('last_synced_at'),
  
  // ============================================================================
  // METADATA
  // ============================================================================
  /** Name/description of this destination schema */
  name: varchar('name', { length: 255 }),
  
  /** Whether this schema is active */
  isActive: boolean('is_active').default(true),
  
  /** Timestamps */
  createdAt: timestamp('created_at').notNull().defaultNow(),
  updatedAt: timestamp('updated_at').notNull().defaultNow(),
  deletedAt: timestamp('deleted_at'),
});

/**
 * Column definition for destination table
 */
export interface ColumnDefinition {
  name: string;
  dataType: string;
  nullable: boolean;
  defaultValue?: string;
  isPrimaryKey?: boolean;
  maxLength?: number;
  isUnique?: boolean;
}

/**
 * Index definition
 */
export interface IndexDefinition {
  name: string;
  columns: string[];
  unique?: boolean;
  where?: string; // Partial index condition
}

/**
 * Column mapping from source to destination
 */
export interface ColumnMapping {
  sourceColumn: string;
  destinationColumn: string;
  dataType: string; // PostgreSQL data type
  nullable: boolean;
  defaultValue?: string;
  isPrimaryKey?: boolean;
  maxLength?: number;
}

/**
 * Destination schema validation result
 */
export interface DestinationSchemaValidationResult {
  valid: boolean;
  errors: string[];
  warnings: string[];
  missingColumns?: string[];
  typeMismatches?: Array<{
    column: string;
    expectedType: string;
    actualType: string;
  }>;
  validatedAt: string;
}

/**
 * Type exports for TypeScript
 */
export type PipelineDestinationSchema = typeof pipelineDestinationSchemas.$inferSelect;
export type NewPipelineDestinationSchema = typeof pipelineDestinationSchemas.$inferInsert;

