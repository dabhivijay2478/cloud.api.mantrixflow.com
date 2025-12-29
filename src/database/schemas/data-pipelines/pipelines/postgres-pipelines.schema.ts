import {
  pgTable,
  uuid,
  varchar,
  integer,
  text,
  timestamp,
  jsonb,
  pgEnum,
} from 'drizzle-orm/pg-core';
import { runStatusEnum } from '../pipeline-runs/postgres-pipeline-runs.schema';
import { pipelineSourceSchemas } from '../source-schemas/pipeline-source-schemas.schema';
import { pipelineDestinationSchemas } from '../destination-schemas/pipeline-destination-schemas.schema';

/**
 * Enum for write mode
 */
export const writeModeEnum = pgEnum('write_mode', [
  'append',
  'upsert',
  'replace',
]);

/**
 * Enum for pipeline status
 */
export const pipelineStatusEnum = pgEnum('pipeline_status', [
  'active',
  'paused',
  'error',
]);

// runStatusEnum is imported from pipeline-runs schema to avoid duplication

/**
 * PostgreSQL Pipelines Table
 * Stores pipeline configurations for data synchronization
 * 
 * Now references separate source and destination schema tables for better organization.
 * 
 * Structure:
 * - Basic Info: id, orgId, userId, name, description
 * - Source Schema Reference: sourceSchemaId (references pipeline_source_schemas)
 * - Destination Schema Reference: destinationSchemaId (references pipeline_destination_schemas)
 * - Transformations: transformations applied during pipeline execution
 * - Sync Configuration: syncMode, incrementalColumn, lastSyncValue, syncFrequency, nextSyncAt
 * - Execution Status: status, lastRunAt, lastRunStatus, lastError
 * - Statistics: totalRowsProcessed, totalRunsSuccessful, totalRunsFailed
 * - Metadata: createdAt, updatedAt, deletedAt
 */
export const postgresPipelines = pgTable('postgres_pipelines', {
  // ============================================================================
  // BASIC INFORMATION
  // ============================================================================
  id: uuid('id').primaryKey().defaultRandom(),
  orgId: uuid('org_id').notNull(),
  userId: uuid('user_id').notNull(),
  name: varchar('name', { length: 255 }).notNull(),
  description: text('description'),

  // ============================================================================
  // SOURCE & DESTINATION SCHEMA REFERENCES
  // References to separate schema tables for better organization
  // ============================================================================
  /** Source type (legacy column - kept for backward compatibility with database) */
  sourceType: varchar('source_type', { length: 100 }),
  
  /** Source schema ID (references pipeline_source_schemas) */
  sourceSchemaId: uuid('source_schema_id')
    .notNull()
    .references(() => pipelineSourceSchemas.id, { onDelete: 'restrict' }),
  
  /** Destination schema ID (references pipeline_destination_schemas) */
  destinationSchemaId: uuid('destination_schema_id')
    .notNull()
    .references(() => pipelineDestinationSchemas.id, { onDelete: 'restrict' }),

  // ============================================================================
  // LEGACY COLUMNS (kept for backward compatibility during migration)
  // These columns exist in the database but are being phased out
  // ============================================================================
  /** Destination connection ID (legacy - kept for backward compatibility, required during migration) */
  destinationConnectionId: uuid('destination_connection_id').notNull(),
  
  /** Destination table name (legacy - kept for backward compatibility, required during migration) */
  destinationTable: varchar('destination_table', { length: 255 }),

  // ============================================================================
  // TRANSFORMATIONS
  // Data transformations to apply during pipeline execution
  // ============================================================================
  /** Data transformations to apply during pipeline execution */
  transformations: jsonb('transformations').$type<Transformation[]>(),

  // ============================================================================
  // SYNC CONFIGURATION
  // ============================================================================
  syncMode: varchar('sync_mode', { length: 50 }).default('full'), // 'full' or 'incremental'
  incrementalColumn: varchar('incremental_column', { length: 255 }),
  lastSyncValue: text('last_sync_value'),
  syncFrequency: varchar('sync_frequency', { length: 50 }).default('manual'), // 'manual', '15min', '1hour', '24hours'
  nextSyncAt: timestamp('next_sync_at'),

  // ============================================================================
  // EXECUTION STATUS
  // ============================================================================
  status: pipelineStatusEnum('status').default('active'),
  lastRunAt: timestamp('last_run_at'),
  lastRunStatus: runStatusEnum('last_run_status'),
  lastError: text('last_error'),

  // ============================================================================
  // STATISTICS
  // ============================================================================
  totalRowsProcessed: integer('total_rows_processed').default(0),
  totalRunsSuccessful: integer('total_runs_successful').default(0),
  totalRunsFailed: integer('total_runs_failed').default(0),

  // ============================================================================
  // METADATA
  // ============================================================================
  createdAt: timestamp('created_at').notNull().defaultNow(),
  updatedAt: timestamp('updated_at').notNull().defaultNow(),
  deletedAt: timestamp('deleted_at'),
});

/**
 * TypeScript Interfaces for JSONB columns
 */

/**
 * Data transformation configuration
 */
export interface Transformation {
  sourceColumn: string;
  transformType: 'rename' | 'cast' | 'concat' | 'split' | 'custom';
  transformConfig: any; // transformation-specific config
  destinationColumn: string;
}

/**
 * Type exports for TypeScript
 */
export type PostgresPipeline = typeof postgresPipelines.$inferSelect;
export type NewPostgresPipeline = typeof postgresPipelines.$inferInsert;

