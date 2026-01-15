import { jsonb, pgTable, timestamp, uuid, varchar } from 'drizzle-orm/pg-core';
import { users } from '../users/users.schema';
import { subscriptions } from './dodo-subscriptions.schema';

/**
 * Dodo Customers Table
 * Maps our internal users to Dodo Payments customers and their primary subscription.
 */
export const dodoCustomers = pgTable('dodo_customers', {
  id: uuid('id').primaryKey().defaultRandom(),
  // Our internal user
  userId: uuid('user_id')
    .notNull()
    .references(() => users.id, { onDelete: 'cascade' })
    .unique(), // One Dodo customer per user
  // Dodo Payments customer id
  dodoCustomerId: varchar('dodo_customer_id', { length: 255 }).notNull().unique(),
  // Optional primary subscription in our billing schema linked to this Dodo customer
  subscriptionId: uuid('subscription_id').references(() => subscriptions.id, {
    onDelete: 'set null',
  }),
  // Raw metadata we get from Dodo for this customer
  metadata: jsonb('metadata'),
  // Timestamps
  createdAt: timestamp('created_at').notNull().defaultNow(),
  updatedAt: timestamp('updated_at').notNull().defaultNow(),
});

// Type exports for TypeScript
export type DodoCustomer = typeof dodoCustomers.$inferSelect;
export type NewDodoCustomer = typeof dodoCustomers.$inferInsert;
