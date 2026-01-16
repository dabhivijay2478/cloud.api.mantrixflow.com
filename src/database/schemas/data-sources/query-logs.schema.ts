import { integer, pgEnum, pgTable, text, timestamp, uuid } from 'drizzle-orm/pg-core';
import { dataSources } from './data-sources.schema';
import { users } from '../users/users.schema';

/**
 * Query Log Status Enum
 */
export const queryLogStatusEnum = pgEnum('query_log_status', ['success', 'error']);

/**
 * Query Logs Table
 * Audit trail for all queries executed against data sources
 *
 * This table tracks query execution history for debugging, auditing, and performance monitoring.
 * Replaces the old postgres_query_logs table with a generic structure.
 */
export const queryLogs = pgTable('query_logs', {
  id: uuid('id').primaryKey().defaultRandom(),

  // Data source reference (replaces connection_id)
  dataSourceId: uuid('data_source_id')
    .notNull()
    .references(() => dataSources.id, { onDelete: 'cascade' }),

  // User who executed the query
  userId: uuid('user_id')
    .notNull()
    .references(() => users.id, { onDelete: 'restrict' }),

  // Query details
  query: text('query').notNull(),
  executionTimeMs: integer('execution_time_ms').notNull(),
  rowsReturned: integer('rows_returned').notNull().default(0),

  // Status
  status: queryLogStatusEnum('status').notNull(),
  errorMessage: text('error_message'),

  // Timestamp
  createdAt: timestamp('created_at').notNull().defaultNow(),
});

// Type exports for TypeScript
export type QueryLog = typeof queryLogs.$inferSelect;
export type NewQueryLog = typeof queryLogs.$inferInsert;
