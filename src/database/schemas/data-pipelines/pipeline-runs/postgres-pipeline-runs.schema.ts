import {
  pgTable,
  uuid,
  integer,
  text,
  timestamp,
  varchar,
  jsonb,
  pgEnum,
} from 'drizzle-orm/pg-core';
import { postgresPipelines } from '../pipelines/postgres-pipelines.schema';

/**
 * Enum for run status
 */
export const runStatusEnum = pgEnum('run_status', [
  'pending',
  'running',
  'success',
  'failed',
  'cancelled',
]);

/**
 * Enum for trigger type
 */
export const triggerTypeEnum = pgEnum('trigger_type', [
  'manual',
  'scheduled',
  'webhook',
]);

/**
 * PostgreSQL Pipeline Runs Table
 * Tracks individual pipeline execution runs
 */
export const postgresPipelineRuns = pgTable('postgres_pipeline_runs', {
  id: uuid('id').primaryKey().defaultRandom(),
  pipelineId: uuid('pipeline_id')
    .notNull()
    .references(() => postgresPipelines.id, { onDelete: 'cascade' }),
  orgId: uuid('org_id').notNull(),

  status: runStatusEnum('status').default('pending'),

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
 * Type exports for TypeScript
 */
export type PostgresPipelineRun = typeof postgresPipelineRuns.$inferSelect;
export type NewPostgresPipelineRun = typeof postgresPipelineRuns.$inferInsert;

