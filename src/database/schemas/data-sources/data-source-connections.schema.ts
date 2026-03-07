import { index, jsonb, pgEnum, pgTable, text, timestamp, uuid, varchar } from 'drizzle-orm/pg-core';
import { dataSources } from './data-sources.schema';

/**
 * Connection Status Enum
 */
export const connectionStatusEnum = pgEnum('connection_status', [
  'active',
  'inactive',
  'error',
  'testing',
]);

/**
 * Data Source Connections Table
 * Dynamic connection credentials storage using JSONB
 *
 * This table stores connection configuration for ALL data source types using a flexible JSONB structure.
 * The config field contains type-specific connection details (host, port, credentials, etc.)
 *
 * Key Features:
 * - Universal: Supports any data source type via connection_type and config JSONB
 * - Encrypted: Sensitive fields (passwords, tokens, keys) should be encrypted before storage
 * - Connection testing: Tracks test results and connection status
 *
 * Config JSONB Structure Examples:
 *
 * PostgreSQL (only supported source/destination for ETL):
 * {
 *   "host": "localhost",
 *   "port": 5432,
 *   "database": "mydb",
 *   "username": "user",
 *   "password": "encrypted_pass",
 *   "ssl": {"enabled": true, "ca_cert": "...", "client_cert": "...", "client_key": "..."},
 *   "ssh_tunnel": {"enabled": false, "host": "", "port": 22, "username": "", "private_key": ""},
 *   "pool": {"size": 5, "timeout_seconds": 60}
 * }
 *
 * Amazon S3:
 * {
 *   "bucket": "my-data-bucket",
 *   "region": "us-east-1",
 *   "access_key_id": "AKIA...",
 *   "secret_access_key": "encrypted_secret",
 *   "path_prefix": "data/",
 *   "use_ssl": true
 * }
 *
 * REST API:
 * {
 *   "base_url": "https://api.example.com",
 *   "auth_type": "bearer",
 *   "auth_token": "encrypted_token",
 *   "headers": {"Content-Type": "application/json"},
 *   "rate_limit": {"requests_per_second": 10}
 * }
 *
 * Google BigQuery:
 * {
 *   "project_id": "my-project",
 *   "dataset": "analytics",
 *   "credentials": {
 *     "type": "service_account",
 *     "project_id": "my-project",
 *     "private_key": "encrypted_key",
 *     "client_email": "service@project.iam.gserviceaccount.com"
 *   },
 *   "location": "US"
 * }
 *
 * Snowflake:
 * {
 *   "account": "xy12345.us-east-1",
 *   "username": "analyst",
 *   "password": "encrypted_pass",
 *   "warehouse": "COMPUTE_WH",
 *   "database": "ANALYTICS",
 *   "schema": "PUBLIC",
 *   "role": "ANALYST_ROLE"
 * }
 */
export const dataSourceConnections = pgTable(
  'data_source_connections',
  {
    id: uuid('id').primaryKey().defaultRandom(),

    // Data source reference (1:1 relationship)
    dataSourceId: uuid('data_source_id')
      .notNull()
      .references(() => dataSources.id, { onDelete: 'cascade' }),

    // Connection type: postgres (ETL tap/target), s3, api, bigquery, snowflake, etc.
    connectionType: varchar('connection_type', { length: 100 }).notNull(),

    // Dynamic configuration - stores ALL connection details as JSONB
    // Sensitive fields (passwords, tokens, keys) should be encrypted before storage
    config: jsonb('config').notNull(),

    // Connection status
    status: connectionStatusEnum('status').notNull().default('inactive'),

    // Connection tracking
    lastConnectedAt: timestamp('last_connected_at'),
    lastError: text('last_error'),

    // Connection test results
    testResult: jsonb('test_result'),

    // Singer CDC fields
    collectionMethod: varchar('collection_method', { length: 50 }).default('full_refresh'),
    replicationSlotName: varchar('replication_slot_name', { length: 63 }),
    cdcSlotHealth: jsonb('cdc_slot_health'),
    cdcPrerequisitesStatus: jsonb('cdc_prerequisites_status'),
    publicationName: varchar('publication_name', { length: 255 }),
    schemaEvolutionLog: jsonb('schema_evolution_log'),

    // Timestamps
    createdAt: timestamp('created_at').notNull().defaultNow(),
    updatedAt: timestamp('updated_at').notNull().defaultNow(),
  },
  (table) => ({
    dataSourceIdIdx: index('data_source_connections_data_source_id_idx').on(table.dataSourceId),
    connectionTypeIdx: index('data_source_connections_connection_type_idx').on(
      table.connectionType,
    ),
    statusIdx: index('data_source_connections_status_idx').on(table.status),
  }),
);

// Type exports for TypeScript
export type DataSourceConnection = typeof dataSourceConnections.$inferSelect;
export type NewDataSourceConnection = typeof dataSourceConnections.$inferInsert;

/**
 * TypeScript interfaces for config JSONB structures
 * These are examples - actual config structure depends on connection_type
 */

export interface PostgresConfig {
  host: string;
  port: number;
  database: string;
  username: string;
  password: string; // Should be encrypted
  ssl?: {
    enabled: boolean;
    ca_cert?: string; // Should be encrypted
    client_cert?: string; // Should be encrypted
    client_key?: string; // Should be encrypted
  };
  ssh_tunnel?: {
    enabled: boolean;
    host?: string;
    port?: number;
    username?: string;
    private_key?: string; // Should be encrypted
  };
  pool?: {
    size: number;
    timeout_seconds: number;
  };
}

export interface S3Config {
  bucket: string;
  region: string;
  access_key_id: string; // Should be encrypted
  secret_access_key: string; // Should be encrypted
  path_prefix?: string;
  use_ssl?: boolean;
}

export interface APIConfig {
  base_url: string;
  auth_type: 'bearer' | 'api_key' | 'oauth2' | 'basic';
  auth_token?: string; // Should be encrypted
  api_key?: string; // Should be encrypted
  headers?: Record<string, string>;
  rate_limit?: {
    requests_per_second: number;
  };
}

export interface BigQueryConfig {
  project_id: string;
  dataset: string;
  credentials: {
    type: string;
    project_id: string;
    private_key: string; // Should be encrypted
    client_email: string;
  };
  location?: string;
}

export interface SnowflakeConfig {
  account: string;
  username: string;
  password: string; // Should be encrypted
  warehouse: string;
  database: string;
  schema: string;
  role?: string;
}
