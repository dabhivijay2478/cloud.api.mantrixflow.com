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
import type {
  PollingConfig,
  PipelineCheckpoint,
} from '../../../modules/data-pipelines/types/pipeline-lifecycle.types';

/**
 * Enum for write mode
 */
export const writeModeEnum = pgEnum('write_mode', ['append', 'upsert', 'replace']);

/**
 * Enum for pipeline status (lifecycle states)
 */
export const pipelineStatusEnum = pgEnum('pipeline_status', [
  'idle',
  'initializing',
  'running',
  'listing',
  'listening',
  'paused',
  'failed',
  'completed',
]);

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
 * Enum for schedule type
 */
export const scheduleTypeEnum = pgEnum('schedule_type', [
  'none',
  'minutes',
  'hourly',
  'daily',
  'weekly',
  'monthly',
  'custom_cron',
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
 * - Lifecycle: checkpoint, pollingConfig, lastSyncAt
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
  syncMode: varchar('sync_mode', { length: 50 }).default('full'), // 'full', 'incremental', or 'cdc'
  incrementalColumn: varchar('incremental_column', { length: 255 }),
  lastSyncValue: text('last_sync_value'),
  syncFrequency: varchar('sync_frequency', { length: 50 }).default('manual'), // 'manual', 'hourly', 'daily', 'weekly'
  nextSyncAt: timestamp('next_sync_at'),

  // ============================================================================
  // SCHEDULING CONFIGURATION
  // ============================================================================
  /** Schedule type: none, minutes, hourly, daily, weekly, monthly, custom_cron */
  scheduleType: scheduleTypeEnum('schedule_type').default('none'),

  /** Schedule value: e.g. "15" for minutes, "14:30" for daily, "0 3 * * *" for cron */
  scheduleValue: varchar('schedule_value', { length: 255 }),

  /** Timezone for schedule (e.g. America/New_York, Asia/Kolkata) */
  scheduleTimezone: varchar('schedule_timezone', { length: 50 }).default('UTC'),

  /** Timestamp of last scheduled run */
  lastScheduledRunAt: timestamp('last_scheduled_run_at'),

  /** Calculated next scheduled run time */
  nextScheduledRunAt: timestamp('next_scheduled_run_at'),

  /** Polling interval in seconds (for LISTING mode) */
  pollingIntervalSeconds: integer('polling_interval_seconds').default(300),

  /** Polling configuration (batch size, backoff, etc.) */
  pollingConfig: jsonb('polling_config').$type<PollingConfig>(),

  // ============================================================================
  // EXECUTION STATUS & LIFECYCLE
  // ============================================================================
  /** Current lifecycle status */
  status: pipelineStatusEnum('status').default('idle'),

  /** Migration state for backward compatibility */
  migrationState: varchar('migration_state', { length: 50 }),

  /** Last run timestamp */
  lastRunAt: timestamp('last_run_at'),

  /** Last run status */
  lastRunStatus: runStatusEnum('last_run_status'),

  /** Last error message */
  lastError: text('last_error'),

  /** Checkpoint data for resumable syncs (stores cursor, WAL position, etc.) */
  checkpoint: jsonb('checkpoint').$type<PipelineCheckpoint>(),

  /** Last successful sync timestamp */
  lastSyncAt: timestamp('last_sync_at'),

  /** Timestamp when pipeline was paused (for delta calculation on resume) */
  pauseTimestamp: timestamp('pause_timestamp'),

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
  transformType: 'rename' | 'cast' | 'concat' | 'split' | 'custom' | 'filter' | 'mask' | 'hash';
  transformConfig: any; // transformation-specific config
  destinationColumn: string;
}

/**
 * Type exports for TypeScript
 */
export type Pipeline = typeof pipelines.$inferSelect;
export type NewPipeline = typeof pipelines.$inferInsert;
