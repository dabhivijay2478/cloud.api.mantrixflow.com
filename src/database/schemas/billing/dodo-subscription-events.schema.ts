import { pgEnum, pgTable, jsonb, timestamp, uuid, varchar } from 'drizzle-orm/pg-core';
import { subscriptions } from './dodo-subscriptions.schema';

/**
 * Enum for subscription event type
 */
export const subscriptionEventTypeEnum = pgEnum('subscription_event_type', [
  'payment.succeeded',
  'payment.failed',
  'payment.processing',
  'payment.cancelled',
  'subscription.created',
  'subscription.active',
  'subscription.activated',
  'subscription.updated',
  'subscription.on_hold',
  'subscription.renewed',
  'subscription.canceled',
  'subscription.failed',
  'subscription.trial_started',
  'subscription.trial_ended',
]);

/**
 * Dodo Subscription Events Table
 * Tracks all subscription-related events from Dodo Payments webhooks.
 */
export const subscriptionEvents = pgTable('dodo_subscription_events', {
  id: uuid('id').primaryKey().defaultRandom(),
  // Subscription reference
  subscriptionId: uuid('subscription_id')
    .notNull()
    .references(() => subscriptions.id, { onDelete: 'cascade' }),
  // Event type
  eventType: subscriptionEventTypeEnum('event_type').notNull(),
  // Dodo event ID for idempotency
  dodoEventId: varchar('dodo_event_id', { length: 255 }).unique(),
  // Full event payload from Dodo
  payload: jsonb('payload').notNull(),
  // Processing status
  processed: timestamp('processed').defaultNow(),
  // Error information if processing failed
  error: jsonb('error'),
  // Timestamps
  createdAt: timestamp('created_at').notNull().defaultNow(),
});

// Type exports for TypeScript
export type SubscriptionEvent = typeof subscriptionEvents.$inferSelect;
export type NewSubscriptionEvent = typeof subscriptionEvents.$inferInsert;
