/**
 * Email Preferences Schema
 * User opt-out preferences for non-critical emails
 */

import { boolean, pgTable, timestamp, uuid } from 'drizzle-orm/pg-core';
import { users } from '../users/users.schema';

export const emailPreferences = pgTable('email_preferences', {
  userId: uuid('user_id')
    .primaryKey()
    .references(() => users.id, { onDelete: 'cascade' }),
  weeklyDigestEnabled: boolean('weekly_digest_enabled').notNull().default(true),
  pipelineFailureEmails: boolean('pipeline_failure_emails').notNull().default(true),
  marketingEmails: boolean('marketing_emails').notNull().default(true),
  updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
});

export type EmailPreference = typeof emailPreferences.$inferSelect;
export type NewEmailPreference = typeof emailPreferences.$inferInsert;
