import { boolean, jsonb, pgTable, text, timestamp, uuid, varchar } from 'drizzle-orm/pg-core';
import { dataSources } from '../../data-sources/data-sources.schema';

/**
 * Pipeline Destination Schemas Table
 * Stores destination schema configurations and definitions
 *
 * This table separates destination configuration from pipeline configuration,
 * allowing for better organization and reuse of destination schemas.
 */
export const pipelineDestinationSchemas = pgTable('pipeline_destination_schemas', {
  id: uuid('id').primaryKey().defaultRandom(),
  organizationId: uuid('organization_id').notNull(),

  // ============================================================================
  // DESTINATION CONNECTION
  // ============================================================================
  /** Data source ID (replaces destination_connection_id) - links to data_sources table */
  dataSourceId: uuid('data_source_id')
    .notNull()
    .references(() => dataSources.id, { onDelete: 'cascade' }),

  // ============================================================================
  // DESTINATION LOCATION
  // ============================================================================
  /** Destination database schema name (default: 'public') */
  destinationSchema: varchar('destination_schema', { length: 255 }).default('public'),

  /** Destination table name */
  destinationTable: varchar('destination_table', { length: 255 }).notNull(),

  /** Whether destination table already exists */
  destinationTableExists: boolean('destination_table_exists').default(false),

  // ============================================================================
  // SCHEMA DEFINITION
  // ============================================================================
  /** Transform type: 'dlt' (data load tool, default) | 'dbt' | 'rules' | 'none' */
  transformType: varchar('transform_type', { length: 50 }).default('dlt'),

  /** dbt model name - only when transformType is 'dbt' */
  dbtModel: varchar('dbt_model', { length: 255 }),

  /** Custom SQL - only when transformType is 'dbt'; ignored for dlt */
  customSql: text('custom_sql'),

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
