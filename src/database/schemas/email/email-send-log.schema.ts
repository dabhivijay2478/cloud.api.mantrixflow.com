/**
 * Email Send Log Schema
 * Tracks sent emails for cooldowns, deduplication, and analytics
 */

import { index, pgTable, timestamp, uuid, varchar } from 'drizzle-orm/pg-core';
import { dataSourceConnections } from '../data-sources/data-source-connections.schema';
import { organizations } from '../organizations/organizations.schema';
import { pipelines } from '../data-pipelines/pipelines.schema';
import { users } from '../users/users.schema';

export const emailSendLog = pgTable(
  'email_send_log',
  {
    id: uuid('id').primaryKey().defaultRandom(),
    orgId: uuid('org_id').references(() => organizations.id, { onDelete: 'cascade' }),
    userId: uuid('user_id').references(() => users.id, { onDelete: 'cascade' }),
    emailType: varchar('email_type', { length: 100 }).notNull(),
    recipientEmail: varchar('recipient_email', { length: 255 }).notNull(),
    pipelineId: uuid('pipeline_id').references(() => pipelines.id, { onDelete: 'cascade' }),
    connectionId: uuid('connection_id').references(() => dataSourceConnections.id, {
      onDelete: 'cascade',
    }),
    sentAt: timestamp('sent_at', { withTimezone: true }).notNull().defaultNow(),
    unosendMessageId: varchar('unosend_message_id', { length: 255 }),
  },
  (table) => ({
    pipelineTypeIdx: index('idx_email_send_log_pipeline_type').on(
      table.pipelineId,
      table.emailType,
    ),
    sentAtIdx: index('idx_email_send_log_sent_at').on(table.sentAt),
  }),
);

export type EmailSendLog = typeof emailSendLog.$inferSelect;
export type NewEmailSendLog = typeof emailSendLog.$inferInsert;
