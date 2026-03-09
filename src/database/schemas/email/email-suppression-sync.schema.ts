/**
 * Email Suppression Sync Schema
 * Tracks bounced/unsubscribed addresses from UnoSend webhooks
 */

import { pgTable, timestamp, varchar } from 'drizzle-orm/pg-core';

export const emailSuppressionSync = pgTable('email_suppression_sync', {
  email: varchar('email', { length: 255 }).primaryKey(),
  suppressionReason: varchar('suppression_reason', { length: 50 }), // bounce, unsubscribe, complaint
  suppressedAt: timestamp('suppressed_at', { withTimezone: true }).notNull().defaultNow(),
});

export type EmailSuppressionSync = typeof emailSuppressionSync.$inferSelect;
export type NewEmailSuppressionSync = typeof emailSuppressionSync.$inferInsert;
