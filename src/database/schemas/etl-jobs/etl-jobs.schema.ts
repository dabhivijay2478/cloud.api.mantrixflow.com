/**
 * ETL Jobs Schema
 * Async ETL job records for pgmq + pg_cron queue (NO Redis, NO BullMQ)
 * Status pushed via Supabase Realtime to frontend
 */

import {
  bigint,
  integer,
  jsonb,
  pgEnum,
  pgTable,
  text,
  timestamp,
  uuid,
} from 'drizzle-orm/pg-core';
import { organizations } from '../organizations/organizations.schema';
import { pipelines } from '../data-pipelines/pipelines.schema';
import { dataSourceConnections } from '../data-sources/data-source-connections.schema';

export const etlJobStatusEnum = pgEnum('etl_job_status', [
  'pending',
  'queued',
  'running',
  'completed',
  'failed',
]);

export const etlJobs = pgTable('etl_jobs', {
  id: uuid('id').primaryKey().defaultRandom(),
  pipelineId: uuid('pipeline_id')
    .notNull()
    .references(() => pipelines.id, { onDelete: 'cascade' }),
  orgId: uuid('org_id')
    .notNull()
    .references(() => organizations.id, { onDelete: 'cascade' }),
  status: etlJobStatusEnum('status').notNull().default('pending'),
  syncMode: text('sync_mode'),
  direction: text('direction'),
  sourceConnectionId: uuid('source_connection_id').references(
    () => dataSourceConnections.id,
    { onDelete: 'set null' },
  ),
  destConnectionId: uuid('dest_connection_id').references(
    () => dataSourceConnections.id,
    { onDelete: 'set null' },
  ),
  stateId: text('state_id'),
  meltanoJobId: text('meltano_job_id'),
  pgmqMsgId: bigint('pgmq_msg_id', { mode: 'number' }),
  rowsSynced: integer('rows_synced').default(0),
  bytesProcessed: bigint('bytes_processed', { mode: 'number' }).default(0),
  errorMessage: text('error_message'),
  userMessage: text('user_message'),
  startedAt: timestamp('started_at'),
  completedAt: timestamp('completed_at'),
  createdAt: timestamp('created_at').notNull().defaultNow(),
  payload: jsonb('payload'),
});

export type EtlJob = typeof etlJobs.$inferSelect;
export type NewEtlJob = typeof etlJobs.$inferInsert;
