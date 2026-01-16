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
import { pipelines } from './pipelines.schema';

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
 * Enum for job state
 */
export const jobStateEnum = pgEnum('job_state', [
  'pending',
  'queued',
  'running',
  'completed',
  'failed',
]);

/**
 * Enum for trigger type
 */
export const triggerTypeEnum = pgEnum('trigger_type', ['manual', 'scheduled', 'api']);

/**
 * Pipeline Runs Table
 * Tracks individual pipeline execution runs
 * 
 * Renamed from postgres_pipeline_runs to pipeline_runs for multi-source support.
 */
export const pipelineRuns = pgTable('pipeline_runs', {
  id: uuid('id').primaryKey().defaultRandom(),
  pipelineId: uuid('pipeline_id')
    .notNull()
    .references(() => pipelines.id, { onDelete: 'cascade' }),
  organizationId: uuid('organization_id')
    .notNull()
    .references(() => organizations.id, { onDelete: 'cascade' }),

  // Status
  status: runStatusEnum('status').default('pending'),
  jobState: jobStateEnum('job_state').default('pending'),

  // Trigger information
  triggerType: triggerTypeEnum('trigger_type').default('manual'),
  triggeredBy: uuid('triggered_by').references(() => users.id, { onDelete: 'set null' }),

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
 * Type exports for TypeScript
 */
export type PipelineRun = typeof pipelineRuns.$inferSelect;
export type NewPipelineRun = typeof pipelineRuns.$inferInsert;
