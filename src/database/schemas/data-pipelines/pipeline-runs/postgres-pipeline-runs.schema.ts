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
import { postgresPipelines, runStatusEnum } from '../pipelines/postgres-pipelines.schema';

/**
 * Enum for trigger type
 */
export const triggerTypeEnum = pgEnum('trigger_type', ['manual', 'scheduled', 'webhook']);

/**
 * Enum for job state (authoritative state for migration execution)
 */
export const jobStateEnum = pgEnum('job_state', [
  'pending',
  'setup',
  'running',
  'paused',
  'listing',
  'stopped',
  'completed',
  'error',
]);

/**
 * PostgreSQL Pipeline Runs Table
 * Tracks individual pipeline execution runs
 *
 * ARCHITECTURAL NOTE:
 * This table is the AUTHORITATIVE source of truth for:
 * - Resolved destination table (locked during setup phase)
 * - Job execution state
 * - Migration progress and cursor
 *
 * The resolved destination table is determined ONCE during setup
 * and stored here. Migration execution MUST read from this record.
 */
export const postgresPipelineRuns = pgTable('postgres_pipeline_runs', {
  id: uuid('id').primaryKey().defaultRandom(),
  pipelineId: uuid('pipeline_id')
    .notNull()
    .references(() => postgresPipelines.id, { onDelete: 'cascade' }),
  orgId: uuid('org_id').notNull(),

  status: runStatusEnum('status').default('pending'),

  // ============================================================================
  // RESOLVED DESTINATION TABLE (AUTHORITATIVE - locked during setup)
  // ============================================================================
  /** Resolved destination schema name (locked during setup phase) */
  resolvedDestinationSchema: varchar('resolved_destination_schema', { length: 255 }),

  /** Resolved destination table name (locked during setup phase) */
  resolvedDestinationTable: varchar('resolved_destination_table', { length: 255 }),

  /** Whether the destination table was created or already existed */
  destinationTableWasCreated: varchar('destination_table_was_created', { length: 10 }), // 'true' | 'false'

  /** Resolved column mappings (JSONB) - SINGLE source of truth */
  resolvedColumnMappings: jsonb('resolved_column_mappings').$type<any[]>(),

  // ============================================================================
  // JOB STATE (AUTHORITATIVE - drives migration behavior)
  // ============================================================================
  /** Job execution state - authoritative source for migration logic */
  jobState: jobStateEnum('job_state').default('pending'),

  /** Last sync cursor value (for incremental sync) */
  lastSyncCursor: text('last_sync_cursor'),

  /** Last updated timestamp for job state */
  jobStateUpdatedAt: timestamp('job_state_updated_at'),

  // Execution details
  rowsRead: integer('rows_read').default(0),
  rowsWritten: integer('rows_written').default(0),
  rowsSkipped: integer('rows_skipped').default(0),
  rowsFailed: integer('rows_failed').default(0),
  bytesProcessed: integer('bytes_processed').default(0),

  // Timing
  startedAt: timestamp('started_at'),
  completedAt: timestamp('completed_at'),
  durationSeconds: integer('duration_seconds'),

  // Error tracking
  errorMessage: text('error_message'),
  errorCode: varchar('error_code', { length: 50 }),
  errorStack: text('error_stack'),

  // Metadata
  triggerType: triggerTypeEnum('trigger_type').default('manual'),
  triggeredBy: uuid('triggered_by'), // user_id
  runMetadata: jsonb('run_metadata').$type<RunMetadata>(),

  createdAt: timestamp('created_at').notNull().defaultNow(),
  updatedAt: timestamp('updated_at').notNull().defaultNow(),
});

/**
 * Pipeline run metadata
 */
export interface RunMetadata {
  batchSize: number;
  parallelWorkers?: number;
  sourceChecksum?: string;
  destinationChecksum?: string;
  [key: string]: any;
}

/**
 * Resolved destination table information
 * Stored in run record during setup phase - AUTHORITATIVE
 */
export interface ResolvedDestinationTable {
  schema: string;
  table: string;
  wasCreated: boolean;
  columnMappings: any[];
}

/**
 * Type exports for TypeScript
 */
export type PostgresPipelineRun = typeof postgresPipelineRuns.$inferSelect;
export type NewPostgresPipelineRun = typeof postgresPipelineRuns.$inferInsert;
