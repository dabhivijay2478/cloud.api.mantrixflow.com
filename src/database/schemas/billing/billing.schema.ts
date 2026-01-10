import { pgEnum, pgTable, text, timestamp, uuid, varchar } from 'drizzle-orm/pg-core';
import { organizations } from '../organizations/organizations.schema';

/**
 * Billing Status Enum
 */
export const billingStatusEnum = pgEnum('billing_status', [
  'active',
  'trialing',
  'past_due',
  'canceled',
  'unpaid',
  'incomplete',
  'incomplete_expired',
  'paused',
]);

/**
 * Billing Subscriptions Table
 * Stores Stripe billing references for organizations
 * One Stripe Customer per organization
 */
export const billingSubscriptions = pgTable('billing_subscriptions', {
  id: uuid('id').primaryKey().defaultRandom(),
  organizationId: uuid('organization_id')
    .notNull()
    .references(() => organizations.id, { onDelete: 'cascade' })
    .unique(),
  // Stripe references
  stripeCustomerId: varchar('stripe_customer_id', { length: 255 }).notNull().unique(),
  stripeSubscriptionId: varchar('stripe_subscription_id', { length: 255 }),
  // Plan information
  planId: varchar('plan_id', { length: 100 }), // e.g., 'pro', 'enterprise'
  // Billing status (synced from Stripe)
  billingStatus: billingStatusEnum('billing_status').notNull().default('incomplete'),
  // Timestamps
  createdAt: timestamp('created_at').notNull().defaultNow(),
  updatedAt: timestamp('updated_at').notNull().defaultNow(),
});

// Type exports for TypeScript
export type BillingSubscription = typeof billingSubscriptions.$inferSelect;
export type NewBillingSubscription = typeof billingSubscriptions.$inferInsert;
