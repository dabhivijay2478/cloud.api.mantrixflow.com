import { boolean, decimal, jsonb, pgTable, text, timestamp, uuid, varchar } from 'drizzle-orm/pg-core';
import { organizations } from '../organizations/organizations.schema';

/**
 * Subscriptions Table
 * Provider-agnostic subscription records
 * Supports Razorpay, Stripe (future), and other providers
 */
export const subscriptions = pgTable('subscriptions', {
  id: uuid('id').primaryKey().defaultRandom(),
  organizationId: uuid('organization_id')
    .notNull()
    .references(() => organizations.id, { onDelete: 'cascade' }),
  provider: varchar('provider', { length: 50 }).notNull(), // 'razorpay' | 'stripe'
  planId: varchar('plan_id', { length: 100 }).notNull(), // 'free' | 'pro' | 'scale'
  providerSubscriptionId: varchar('provider_subscription_id', { length: 255 })
    .notNull()
    .unique(),
  status: varchar('status', { length: 50 }).notNull().default('incomplete'),
  currentPeriodStart: timestamp('current_period_start'),
  currentPeriodEnd: timestamp('current_period_end'),
  cancelAtPeriodEnd: boolean('cancel_at_period_end').default(false),
  amount: decimal('amount', { precision: 10, scale: 2 }),
  currency: varchar('currency', { length: 10 }).default('INR'),
  metadata: jsonb('metadata'),
  createdAt: timestamp('created_at').notNull().defaultNow(),
  updatedAt: timestamp('updated_at').notNull().defaultNow(),
});

// Type exports for TypeScript
export type Subscription = typeof subscriptions.$inferSelect;
export type NewSubscription = typeof subscriptions.$inferInsert;
