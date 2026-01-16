import {
  integer,
  jsonb,
  pgEnum,
  pgTable,
  text,
  timestamp,
  uuid,
  varchar,
} from 'drizzle-orm/pg-core';
import { organizations } from '../organizations/organizations.schema';
import { users } from '../users/users.schema';
import { pipelineDestinationSchemas } from './destination-schemas/pipeline-destination-schemas.schema';
import { pipelineSourceSchemas } from './source-schemas/pipeline-source-schemas.schema';

/**
 * Enum for write mode
 */
export const writeModeEnum = pgEnum('write_mode', ['append', 'upsert', 'replace']);

/**
 * Enum for pipeline status
 */
export const pipelineStatusEnum = pgEnum('pipeline_status', ['active', 'paused', 'error']);

/**
 * Enum for run status
 * Shared enum used by both pipelines and pipeline_runs tables
 */
export const runStatusEnum = pgEnum('run_status', [
  'pending',
  'running',
  'success',
  'failed',
  'cancelled',
]);

/**
 * Pipelines Table
 * Stores pipeline configurations for data synchronization
 *
 * Renamed from postgres_pipelines to pipelines for multi-source support.
 *
 * Structure:
 * - Basic Info: id, organizationId, createdBy, name, description
 * - Source Configuration: sourceSchemaId (references pipeline_source_schemas)
 * - Destination Configuration: destinationSchemaId (references pipeline_destination_schemas)
 * - Transformations: columnMappings, transformations
 * - Write Configuration: writeMode, upsertKey
 * - Sync Configuration: syncMode, incrementalColumn, lastSyncValue, syncFrequency, nextSyncAt
 * - Execution Status: status, lastRunAt, lastRunStatus, lastError
 * - Statistics: totalRowsProcessed, totalRunsSuccessful, totalRunsFailed
 * - Metadata: createdAt, updatedAt, deletedAt
 */
export const pipelines = pgTable('pipelines', {
  // ============================================================================
  // BASIC INFORMATION
  // ============================================================================
  id: uuid('id').primaryKey().defaultRandom(),
  organizationId: uuid('organization_id')
    .notNull()
    .references(() => organizations.id, { onDelete: 'cascade' }),
  createdBy: uuid('created_by')
    .notNull()
    .references(() => users.id, { onDelete: 'restrict' }),
  name: varchar('name', { length: 255 }).notNull(),
  description: text('description'),

  // ============================================================================
  // SOURCE & DESTINATION CONFIGURATION
  // ============================================================================
  /** Source schema ID (references pipeline_source_schemas) */
  sourceSchemaId: uuid('source_schema_id')
    .notNull()
    .references(() => pipelineSourceSchemas.id, { onDelete: 'restrict' }),

  /** Destination schema ID (references pipeline_destination_schemas) */
  destinationSchemaId: uuid('destination_schema_id')
    .notNull()
    .references(() => pipelineDestinationSchemas.id, { onDelete: 'restrict' }),

  // ============================================================================
  // TRANSFORMATIONS
  // ============================================================================
  /** Data transformations to apply during pipeline execution */
  transformations: jsonb('transformations').$type<Transformation[]>(),

  // ============================================================================
  // SYNC CONFIGURATION
  // ============================================================================
  syncMode: varchar('sync_mode', { length: 50 }).default('full'), // 'full' or 'incremental'
  incrementalColumn: varchar('incremental_column', { length: 255 }),
  lastSyncValue: text('last_sync_value'),
  syncFrequency: varchar('sync_frequency', { length: 50 }).default('manual'), // 'manual', 'hourly', 'daily', 'weekly'
  nextSyncAt: timestamp('next_sync_at'),

  // ============================================================================
  // EXECUTION STATUS
  // ============================================================================
  status: pipelineStatusEnum('status').default('active'),
  migrationState: varchar('migration_state', { length: 50 }), // 'pending', 'running', 'listing'
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
export type Pipeline = typeof pipelines.$inferSelect;
export type NewPipeline = typeof pipelines.$inferInsert;
