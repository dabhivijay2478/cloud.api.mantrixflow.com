import { jsonb, pgTable, text, timestamp, uuid, varchar } from 'drizzle-orm/pg-core';
import { organizations } from '../organizations/organizations.schema';
import { users } from '../users/users.schema';

/**
 * Subscription Events Table
 * Audit log for billing webhook events
 * Stores raw payloads for debugging and compliance
 */
export const subscriptionEvents = pgTable('subscription_events', {
  id: uuid('id').primaryKey().defaultRandom(),
  userId: uuid('user_id').references(() => users.id, {
    onDelete: 'cascade',
  }), // User who owns the subscription (billing is user-scoped)
  organizationId: uuid('organization_id').references(() => organizations.id, {
    onDelete: 'cascade',
  }), // Organization reference (optional)
  provider: varchar('provider', { length: 50 }).notNull(), // 'dodo'
  eventType: varchar('event_type', { length: 100 }).notNull(),
  payload: jsonb('payload').notNull(),
  createdAt: timestamp('created_at').notNull().defaultNow(),
});

// Type exports for TypeScript
export type SubscriptionEvent = typeof subscriptionEvents.$inferSelect;
export type NewSubscriptionEvent = typeof subscriptionEvents.$inferInsert;
