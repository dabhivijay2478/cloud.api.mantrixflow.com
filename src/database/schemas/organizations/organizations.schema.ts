import { boolean, jsonb, pgTable, text, timestamp, uuid, varchar } from 'drizzle-orm/pg-core';
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
  // Owner reference - the user who created this organization
  ownerUserId: uuid('owner_user_id').references(() => users.id, { onDelete: 'set null' }),
  // Metadata
  metadata: jsonb('metadata'), // Additional organization data
  // Settings
  settings: jsonb('settings'), // Organization settings
  // Status
  isActive: boolean('is_active').notNull().default(true),
  // Billing fields (provider-agnostic)
  billingProvider: varchar('billing_provider', { length: 50 }),
  billingCustomerId: varchar('billing_customer_id', { length: 255 }),
  billingSubscriptionId: varchar('billing_subscription_id', { length: 255 }),
  billingPlanId: varchar('billing_plan_id', { length: 100 }),
  billingStatus: varchar('billing_status', { length: 50 }).default('incomplete'),
  billingCurrentPeriodEnd: timestamp('billing_current_period_end'),
  // Timestamps
  createdAt: timestamp('created_at').notNull().defaultNow(),
  updatedAt: timestamp('updated_at').notNull().defaultNow(),
});

// Type exports for TypeScript
export type Organization = typeof organizations.$inferSelect;
export type NewOrganization = typeof organizations.$inferInsert;
