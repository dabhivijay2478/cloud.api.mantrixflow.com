import {
  pgTable,
  uuid,
  varchar,
  integer,
  text,
  timestamp,
  pgEnum,
} from 'drizzle-orm/pg-core';
import { postgresConnections } from '../connections/postgres-connections.schema';

/**
 * Enum for sync job status
 */
export const syncJobStatusEnum = pgEnum('sync_job_status', [
  'pending',
  'running',
  'success',
  'failed',
]);

/**
 * Enum for sync frequency
 */
export const syncFrequencyEnum = pgEnum('sync_frequency', [
  'manual',
  '15min',
  '1hour',
  '24hours',
]);

/**
 * Enum for sync mode
 */
export const syncModeEnum = pgEnum('sync_mode', ['full', 'incremental']);

/**
 * PostgreSQL Sync Jobs Table
 * Tracks data synchronization jobs from PostgreSQL sources to destinations
 */
export const postgresSyncJobs = pgTable('postgres_sync_jobs', {
  id: uuid('id').primaryKey().defaultRandom(),
  connectionId: uuid('connection_id')
    .notNull()
    .references(() => postgresConnections.id, { onDelete: 'cascade' }),
  tableName: varchar('table_name', { length: 255 }).notNull(),
  syncMode: syncModeEnum('sync_mode').notNull(),
  incrementalColumn: varchar('incremental_column', { length: 255 }),
  lastSyncValue: text('last_sync_value'), // Last timestamp or ID value
  destinationTable: varchar('destination_table', { length: 255 }).notNull(), // Format: raw_postgres_org123_tablename
  status: syncJobStatusEnum('status').notNull().default('pending'),
  rowsSynced: integer('rows_synced').notNull().default(0),
  startedAt: timestamp('started_at'),
  completedAt: timestamp('completed_at'),
  errorMessage: text('error_message'),
  syncFrequency: syncFrequencyEnum('sync_frequency')
    .notNull()
    .default('manual'),
  nextSyncAt: timestamp('next_sync_at'),
  customWhereClause: text('custom_where_clause'), // Optional SQL WHERE clause
  createdAt: timestamp('created_at').notNull().defaultNow(),
  updatedAt: timestamp('updated_at').notNull().defaultNow(),
});

// Type exports for TypeScript
export type PostgresSyncJob = typeof postgresSyncJobs.$inferSelect;
export type NewPostgresSyncJob = typeof postgresSyncJobs.$inferInsert;

