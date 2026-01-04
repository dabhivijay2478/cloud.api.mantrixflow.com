import { boolean, jsonb, pgTable, text, timestamp, uuid, varchar } from 'drizzle-orm/pg-core';
import { postgresConnections } from '../../data-sources/connections/postgres-connections.schema';

/**
 * Pipeline Source Schemas Table
 * Stores source schema configurations and discovered schema information
 *
 * This table separates source configuration from pipeline configuration,
 * allowing for better organization and reuse of source schemas.
 */
export const pipelineSourceSchemas = pgTable('pipeline_source_schemas', {
  id: uuid('id').primaryKey().defaultRandom(),
  orgId: uuid('org_id').notNull(),
  userId: uuid('user_id').notNull(),

  // ============================================================================
  // SOURCE IDENTIFICATION
  // ============================================================================
  /** Source type: 'postgres', 'stripe', 'salesforce', 'google_sheets', etc. */
  sourceType: varchar('source_type', { length: 100 }).notNull(),

  /** Source connection ID (for PostgreSQL sources) */
  sourceConnectionId: uuid('source_connection_id').references(() => postgresConnections.id, {
    onDelete: 'cascade',
  }),

  /** Source configuration (for external sources like Stripe, Salesforce) */
  sourceConfig: jsonb('source_config').$type<SourceConfig>(),

  // ============================================================================
  // SOURCE LOCATION
  // ============================================================================
  /** Source database schema name (e.g., 'public', 'sales', 'analytics') */
  sourceSchema: varchar('source_schema', { length: 255 }),

  /** Source table name */
  sourceTable: varchar('source_table', { length: 255 }),

  /** Custom SQL query for source (alternative to table-based reads) */
  sourceQuery: text('source_query'),

  // ============================================================================
  // DISCOVERED SCHEMA INFORMATION
  // ============================================================================
  /** Discovered columns from source */
  discoveredColumns: jsonb('discovered_columns').$type<DiscoveredColumn[]>(),

  /** Primary key columns */
  primaryKeys: jsonb('primary_keys').$type<string[]>(),

  /** Foreign key relationships */
  foreignKeys: jsonb('foreign_keys').$type<ForeignKey[]>(),

  /** Estimated row count */
  estimatedRowCount: jsonb('estimated_row_count').$type<number>(),

  /** Table size in MB */
  sizeMB: jsonb('size_mb').$type<number>(),

  /** Schema validation result */
  validationResult: jsonb('validation_result').$type<SourceSchemaValidationResult>(),

  // ============================================================================
  // METADATA
  // ============================================================================
  /** Name/description of this source schema */
  name: varchar('name', { length: 255 }),

  /** Whether this schema is active */
  isActive: boolean('is_active').default(true),

  /** Last time schema was discovered/refreshed */
  lastDiscoveredAt: timestamp('last_discovered_at'),

  /** Timestamps */
  createdAt: timestamp('created_at').notNull().defaultNow(),
  updatedAt: timestamp('updated_at').notNull().defaultNow(),
  deletedAt: timestamp('deleted_at'),
});

/**
 * Source configuration for external sources (Stripe, Salesforce, etc.)
 */
export interface SourceConfig {
  apiKey?: string;
  accountId?: string;
  endpoint?: string;
  accessToken?: string;
  refreshToken?: string;
  instanceUrl?: string;
  [key: string]: any;
}

/**
 * Discovered column information
 */
export interface DiscoveredColumn {
  name: string;
  dataType: string;
  nullable: boolean;
  defaultValue?: string;
  maxLength?: number;
  isPrimaryKey?: boolean;
  isForeignKey?: boolean;
}

/**
 * Foreign key relationship
 */
export interface ForeignKey {
  column: string;
  referencedTable: string;
  referencedColumn: string;
  referencedSchema?: string;
}

/**
 * Source schema validation result
 */
export interface SourceSchemaValidationResult {
  valid: boolean;
  errors: string[];
  warnings: string[];
  validatedAt: string;
}

/**
 * Type exports for TypeScript
 */
export type PipelineSourceSchema = typeof pipelineSourceSchemas.$inferSelect;
export type NewPipelineSourceSchema = typeof pipelineSourceSchemas.$inferInsert;
