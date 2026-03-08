import {
  boolean,
  integer,
  jsonb,
  pgTable,
  text,
  timestamp,
  uuid,
  varchar,
} from 'drizzle-orm/pg-core';
import { users } from '../users/users.schema';

/**
 * Organizations Table
 * Stores organization/workspace information
 */
export const organizations = pgTable('organizations', {
  id: uuid('id').primaryKey().defaultRandom(),
  name: varchar('name', { length: 255 }).notNull(),
  slug: varchar('slug', { length: 255 }).notNull().unique(),
  description: text('description'),
  // Owner reference - the user who created this organization (required)
  ownerUserId: uuid('owner_user_id')
    .notNull()
    .references(() => users.id, { onDelete: 'restrict' }),
  // Metadata
  metadata: jsonb('metadata'), // Additional organization data
  // Settings
  settings: jsonb('settings'), // Organization settings
  // Status
  isActive: boolean('is_active').notNull().default(true),
  // Billing
  trialEndsAt: timestamp('trial_ends_at', { withTimezone: true }),
  subscriptionStatus: varchar('subscription_status', { length: 50 }),
  planName: varchar('plan_name', { length: 100 }),
  planRunLimit: integer('plan_run_limit'),
  // Timestamps
  createdAt: timestamp('created_at').notNull().defaultNow(),
  updatedAt: timestamp('updated_at').notNull().defaultNow(),
});

// Type exports for TypeScript
export type Organization = typeof organizations.$inferSelect;
export type NewOrganization = typeof organizations.$inferInsert;
