import {
  pgTable,
  uuid,
  varchar,
  integer,
  boolean,
  text,
  timestamp,
  jsonb,
  pgEnum,
} from 'drizzle-orm/pg-core';

/**
 * Enum for connection status
 */
export const connectionStatusEnum = pgEnum('connection_status', [
  'active',
  'inactive',
  'error',
]);

/**
 * PostgreSQL Connections Table
 * Stores encrypted connection credentials and configuration for data sources
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

// Type exports for TypeScript
export type PostgresConnection = typeof postgresConnections.$inferSelect;
export type NewPostgresConnection = typeof postgresConnections.$inferInsert;
