import { jsonb, pgEnum, pgTable, timestamp, uuid, varchar } from 'drizzle-orm/pg-core';
import { users } from '../users/users.schema';

/**
 * Enum for subscription status
 */
export const subscriptionStatusEnum = pgEnum('subscription_status', [
  'active',
  'on_hold',
  'failed',
  'canceled',
  'trialing',
]);

/**
 * Enum for subscription plan
 */
export const subscriptionPlanEnum = pgEnum('subscription_plan', ['free', 'pro', 'scale', 'enterprise']);

/**
 * Dodo Subscriptions Table
 * Tracks user subscriptions for billing via Dodo Payments.
 * Each user (owner) has one active subscription that covers their organization.
 */
export const subscriptions = pgTable('dodo_subscriptions', {
  id: uuid('id').primaryKey().defaultRandom(),
  // User reference - the owner who pays for the subscription
  userId: uuid('user_id')
    .notNull()
    .references(() => users.id, { onDelete: 'cascade' })
    .unique(), // One subscription per user
  // Plan type
  planId: subscriptionPlanEnum('plan_id').notNull(),
  // Subscription status
  status: subscriptionStatusEnum('status').notNull().default('trialing'),
  // Current period start and end
  currentPeriodStart: timestamp('current_period_start'),
  currentPeriodEnd: timestamp('current_period_end'),
  // Trial information
  trialStart: timestamp('trial_start'),
  trialEnd: timestamp('trial_end'),
  // Cancellation
  canceledAt: timestamp('canceled_at'),
  cancelAtPeriodEnd: timestamp('cancel_at_period_end'),
  // Dodo Payments IDs
  dodoSubscriptionId: varchar('dodo_subscription_id', { length: 255 }).unique(),
  dodoCustomerId: varchar('dodo_customer_id', { length: 255 }),
  // Metadata from Dodo
  metadata: jsonb('metadata'),
  // Timestamps
  createdAt: timestamp('created_at').notNull().defaultNow(),
  updatedAt: timestamp('updated_at').notNull().defaultNow(),
});

// Type exports for TypeScript
export type Subscription = typeof subscriptions.$inferSelect;
export type NewSubscription = typeof subscriptions.$inferInsert;
