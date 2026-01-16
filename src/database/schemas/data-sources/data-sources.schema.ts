import {
  boolean,
  index,
  jsonb,
  pgTable,
  text,
  timestamp,
  uuid,
  varchar,
} from 'drizzle-orm/pg-core';
import { organizations } from '../organizations/organizations.schema';
import { users } from '../users/users.schema';

/**
 * Data Sources Table
 * Organization-level data source registry
 *
 * This table stores metadata about data sources (PostgreSQL, MySQL, MongoDB, S3, APIs, etc.)
 * The actual connection credentials are stored in the related data_source_connections table.
 *
 * Key Features:
 * - Organization-centric: Data sources belong to organizations, not users
 * - Multi-source support: Supports any data source type via source_type field
 * - Soft delete: Uses deleted_at for soft deletion
 * - Metadata: Flexible JSONB field for additional configuration
 */
export const dataSources = pgTable(
  'data_sources',
  {
    id: uuid('id').primaryKey().defaultRandom(),

    // Organization reference - data sources belong to organizations
    organizationId: uuid('organization_id')
      .notNull()
      .references(() => organizations.id, { onDelete: 'cascade' }),

    // Basic information
    name: varchar('name', { length: 255 }).notNull(),
    description: text('description'),

    // Source type: postgres, mysql, mongodb, s3, api, bigquery, snowflake, csv, etc.
    sourceType: varchar('source_type', { length: 100 }).notNull(),

    // Status
    isActive: boolean('is_active').notNull().default(true),

    // Metadata - flexible JSONB for additional configuration
    metadata: jsonb('metadata'),

    // Creator reference
    createdBy: uuid('created_by')
      .notNull()
      .references(() => users.id, { onDelete: 'restrict' }),

    // Timestamps
    createdAt: timestamp('created_at').notNull().defaultNow(),
    updatedAt: timestamp('updated_at').notNull().defaultNow(),
    deletedAt: timestamp('deleted_at'), // Soft delete
  },
  (table) => ({
    organizationIdIdx: index('data_sources_organization_id_idx').on(table.organizationId),
    sourceTypeIdx: index('data_sources_source_type_idx').on(table.sourceType),
    // Note: Partial index for is_active with WHERE deleted_at IS NULL is created manually in migration
  }),
);

// Type exports for TypeScript
export type DataSource = typeof dataSources.$inferSelect;
export type NewDataSource = typeof dataSources.$inferInsert;
