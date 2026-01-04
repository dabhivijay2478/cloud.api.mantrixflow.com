import {
  boolean,
  integer,
  jsonb,
  pgEnum,
  pgTable,
  text,
  timestamp,
  uuid,
  varchar,
} from 'drizzle-orm/pg-core';

/**
 * Enum for connection status
 */
export const connectionStatusEnum = pgEnum('connection_status', ['active', 'inactive', 'error']);

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
export const syncFrequencyEnum = pgEnum('sync_frequency', ['manual', '15min', '1hour', '24hours']);

/**
 * Enum for sync mode
 */
export const syncModeEnum = pgEnum('sync_mode', ['full', 'incremental']);

/**
 * Enum for query log status
 */
export const queryLogStatusEnum = pgEnum('query_log_status', ['success', 'error']);

/**
 * PostgreSQL Connections Table
 * Stores encrypted connection credentials and configuration
 */
export const postgresConnections = pgTable('postgres_connections', {
  id: uuid('id').primaryKey().defaultRandom(),
  orgId: uuid('org_id').notNull(),
  userId: uuid('user_id').notNull(),
  name: varchar('name', { length: 255 }).notNull(),
  // Encrypted fields
  host: text('host').notNull(), // Encrypted
  port: integer('port').notNull().default(5432),
  database: text('database').notNull(), // Encrypted
  username: text('username').notNull(), // Encrypted
  password: text('password').notNull(), // Encrypted
  // SSL Configuration
  sslEnabled: boolean('ssl_enabled').notNull().default(false),
  sslCaCert: text('ssl_ca_cert'), // Encrypted, optional
  // SSH Tunnel Configuration
  sshTunnelEnabled: boolean('ssh_tunnel_enabled').notNull().default(false),
  sshHost: text('ssh_host'), // Encrypted, optional
  sshPort: integer('ssh_port'),
  sshUsername: text('ssh_username'), // Encrypted, optional
  sshPrivateKey: text('ssh_private_key'), // Encrypted, optional
  // Connection Pool Configuration
  connectionPoolSize: integer('connection_pool_size').notNull().default(5),
  queryTimeoutSeconds: integer('query_timeout_seconds').notNull().default(60),
  // Status and Metadata
  status: connectionStatusEnum('status').notNull().default('inactive'),
  lastConnectedAt: timestamp('last_connected_at'),
  lastError: text('last_error'),
  // Schema Cache
  schemaCache: jsonb('schema_cache'),
  schemaCachedAt: timestamp('schema_cached_at'),
  // Timestamps
  createdAt: timestamp('created_at').notNull().defaultNow(),
  updatedAt: timestamp('updated_at').notNull().defaultNow(),
});

/**
 * PostgreSQL Sync Jobs Table
 * Tracks data synchronization jobs from PostgreSQL to Supabase
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
  syncFrequency: syncFrequencyEnum('sync_frequency').notNull().default('manual'),
  nextSyncAt: timestamp('next_sync_at'),
  customWhereClause: text('custom_where_clause'), // Optional SQL WHERE clause
  createdAt: timestamp('created_at').notNull().defaultNow(),
  updatedAt: timestamp('updated_at').notNull().defaultNow(),
});

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
export type PostgresConnection = typeof postgresConnections.$inferSelect;
export type NewPostgresConnection = typeof postgresConnections.$inferInsert;
export type PostgresSyncJob = typeof postgresSyncJobs.$inferSelect;
export type NewPostgresSyncJob = typeof postgresSyncJobs.$inferInsert;
export type PostgresQueryLog = typeof postgresQueryLogs.$inferSelect;
export type NewPostgresQueryLog = typeof postgresQueryLogs.$inferInsert;
