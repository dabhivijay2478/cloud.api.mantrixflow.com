import { index, jsonb, pgTable, text, timestamp, uuid } from 'drizzle-orm/pg-core';
import { organizations } from '../organizations/organizations.schema';
import { users } from '../users/users.schema';

/**
 * Activity Logs Table
 * Centralized audit log for all organization-scoped activities
 *
 * This table stores immutable activity logs for:
 * - Organization actions (created, updated, selected)
 * - User & Member actions (invited, role changed, removed)
 * - Data Pipeline actions (created, updated, published, run, paused, resumed, stopped, failed, succeeded)
 * - Migration actions (started, completed, failed, incremental sync, record counts)
 * - Data source & destination actions (connected, updated, deleted, validation failures)
 * - Transformation & mapping actions (field mapping created/updated/deleted, validation)
 *
 * All logs are organization-scoped and immutable (no updates or deletes).
 */
export const activityLogs = pgTable(
  'activity_logs',
  {
    id: uuid('id').primaryKey().defaultRandom(),
    // Organization scope - all logs are scoped to an organization
    organizationId: uuid('organization_id')
      .notNull()
      .references(() => organizations.id, { onDelete: 'cascade' }),
    // User who performed the action (nullable for system actions)
    userId: uuid('user_id').references(() => users.id, { onDelete: 'set null' }),
    // Action type (e.g., 'ORG_CREATED', 'PIPELINE_RUN_STARTED', 'MIGRATION_COMPLETED')
    actionType: text('action_type').notNull(),
    // Entity type (organization | pipeline | migration | datasource | user | mapping)
    entityType: text('entity_type').notNull(),
    // Entity ID (nullable for actions that don't target a specific entity)
    entityId: uuid('entity_id'),
    // Human-readable message describing the action
    message: text('message').notNull(),
    // Additional metadata (JSONB for flexibility)
    metadata: jsonb('metadata'),
    // Timestamp
    createdAt: timestamp('created_at').notNull().defaultNow(),
  },
  (table) => ({
    // Index for efficient queries by organization
    organizationIdIdx: index('activity_logs_organization_id_idx').on(table.organizationId),
    // Index for queries by organization and entity type
    organizationEntityTypeIdx: index('activity_logs_org_entity_type_idx').on(
      table.organizationId,
      table.entityType,
    ),
    // Index for queries by organization and action type
    organizationActionTypeIdx: index('activity_logs_org_action_type_idx').on(
      table.organizationId,
      table.actionType,
    ),
    // Index for queries by user
    userIdIdx: index('activity_logs_user_id_idx').on(table.userId),
    // Index for queries by entity
    entityIdx: index('activity_logs_entity_idx').on(table.entityType, table.entityId),
    // Index for time-based queries (most recent first)
    createdAtIdx: index('activity_logs_created_at_idx').on(table.createdAt),
  }),
);

// Type exports for TypeScript
export type ActivityLog = typeof activityLogs.$inferSelect;
export type NewActivityLog = typeof activityLogs.$inferInsert;
