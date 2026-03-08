/**
 * Email Repository
 * Data access for email_send_log, email_suppression_sync, and email_preferences
 */

import { Inject, Injectable } from '@nestjs/common';
import { and, eq, gt } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import {
  emailPreferences,
  emailSendLog,
  emailSuppressionSync,
  users,
  type NewEmailSendLog,
} from '../../../database/schemas';
import type { EmailPreference } from '../../../database/schemas';

const DEFAULT_PREFERENCES: Pick<
  EmailPreference,
  'weeklyDigestEnabled' | 'pipelineFailureEmails' | 'marketingEmails'
> = {
  weeklyDigestEnabled: true,
  pipelineFailureEmails: true,
  marketingEmails: true,
};

@Injectable()
export class EmailRepository {
  constructor(@Inject('DRIZZLE_DB') private readonly db: NodePgDatabase<any>) {}

  /**
   * Check if email is suppressed (bounce/unsubscribe)
   */
  async isSuppressed(email: string): Promise<boolean> {
    const [row] = await this.db
      .select()
      .from(emailSuppressionSync)
      .where(eq(emailSuppressionSync.email, email.toLowerCase()))
      .limit(1);
    return !!row;
  }

  /**
   * Record suppression from UnoSend webhook
   */
  async addSuppression(email: string, reason: string): Promise<void> {
    await this.db.insert(emailSuppressionSync).values({
      email: email.toLowerCase(),
      suppressionReason: reason,
    }).onConflictDoUpdate({
      target: emailSuppressionSync.email,
      set: {
        suppressionReason: reason,
        suppressedAt: new Date(),
      },
    });
  }

  /**
   * Log a sent email
   */
  async logSend(entry: NewEmailSendLog): Promise<void> {
    await this.db.insert(emailSendLog).values(entry);
  }

  /**
   * Check if pipeline_run_failed was sent in the last hour (cooldown)
   */
  async wasPipelineFailureEmailSentRecently(
    pipelineId: string,
    withinHours: number = 1,
  ): Promise<boolean> {
    const cutoff = new Date(Date.now() - withinHours * 60 * 60 * 1000);
    const [row] = await this.db
      .select()
      .from(emailSendLog)
      .where(
        and(
          eq(emailSendLog.pipelineId, pipelineId),
          eq(emailSendLog.emailType, 'pipeline_run_failed'),
          gt(emailSendLog.sentAt, cutoff),
        ),
      )
      .limit(1);
    return !!row;
  }

  /**
   * Get email preferences for a user. Returns defaults when no row exists.
   */
  async getPreferences(
    userId: string,
  ): Promise<Pick<EmailPreference, 'weeklyDigestEnabled' | 'pipelineFailureEmails' | 'marketingEmails'>> {
    const [row] = await this.db
      .select()
      .from(emailPreferences)
      .where(eq(emailPreferences.userId, userId))
      .limit(1);
    if (!row) return { ...DEFAULT_PREFERENCES };
    return {
      weeklyDigestEnabled: row.weeklyDigestEnabled,
      pipelineFailureEmails: row.pipelineFailureEmails,
      marketingEmails: row.marketingEmails,
    };
  }

  /**
   * Get email preferences by recipient email (joins users table).
   */
  async getPreferencesByEmail(
    email: string,
  ): Promise<Pick<EmailPreference, 'weeklyDigestEnabled' | 'pipelineFailureEmails' | 'marketingEmails'>> {
    const [row] = await this.db
      .select({
        weeklyDigestEnabled: emailPreferences.weeklyDigestEnabled,
        pipelineFailureEmails: emailPreferences.pipelineFailureEmails,
        marketingEmails: emailPreferences.marketingEmails,
      })
      .from(emailPreferences)
      .innerJoin(users, eq(users.id, emailPreferences.userId))
      .where(eq(users.email, email.toLowerCase()))
      .limit(1);
    if (!row) return { ...DEFAULT_PREFERENCES };
    return row;
  }

  /**
   * Upsert email preferences for a user.
   */
  async upsertPreferences(
    userId: string,
    data: {
      weeklyDigestEnabled?: boolean;
      pipelineFailureEmails?: boolean;
      marketingEmails?: boolean;
    },
  ): Promise<void> {
    const setFields: Record<string, unknown> = { updatedAt: new Date() };
    if (data.weeklyDigestEnabled !== undefined) setFields.weeklyDigestEnabled = data.weeklyDigestEnabled;
    if (data.pipelineFailureEmails !== undefined) setFields.pipelineFailureEmails = data.pipelineFailureEmails;
    if (data.marketingEmails !== undefined) setFields.marketingEmails = data.marketingEmails;
    await this.db
      .insert(emailPreferences)
      .values({
        userId,
        weeklyDigestEnabled: data.weeklyDigestEnabled ?? true,
        pipelineFailureEmails: data.pipelineFailureEmails ?? true,
        marketingEmails: data.marketingEmails ?? true,
      })
      .onConflictDoUpdate({
        target: emailPreferences.userId,
        set: setFields as Partial<typeof emailPreferences.$inferInsert>,
      });
  }
}
