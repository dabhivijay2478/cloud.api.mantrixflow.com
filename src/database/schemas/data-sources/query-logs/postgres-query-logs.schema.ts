import {
  pgTable,
  uuid,
  integer,
  text,
  timestamp,
  pgEnum,
} from 'drizzle-orm/pg-core';
import { postgresConnections } from '../connections/postgres-connections.schema';

/**
 * Enum for query log status
 */
export const queryLogStatusEnum = pgEnum('query_log_status', [
  'success',
  'error',
]);

/**
 * PostgreSQL Query Logs Table
 * Audit log for all queries executed against PostgreSQL connections
 */
export const postgresQueryLogs = pgTable('postgres_query_logs', {
  id: uuid('id').primaryKey().defaultRandom(),
  connectionId: uuid('connection_id')
    .notNull()
    .references(() => postgresConnections.id, { onDelete: 'cascade' }),
  userId: uuid('user_id').notNull(),
  query: text('query').notNull(),
  executionTimeMs: integer('execution_time_ms').notNull(),
  rowsReturned: integer('rows_returned').notNull().default(0),
  status: queryLogStatusEnum('status').notNull(),
  errorMessage: text('error_message'),
  createdAt: timestamp('created_at').notNull().defaultNow(),
});

// Type exports for TypeScript
export type PostgresQueryLog = typeof postgresQueryLogs.$inferSelect;
export type NewPostgresQueryLog = typeof postgresQueryLogs.$inferInsert;

