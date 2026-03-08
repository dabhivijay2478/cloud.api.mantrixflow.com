/**
 * Email Repository
 * Data access for email_send_log and email_suppression_sync
 */

import { Inject, Injectable } from '@nestjs/common';
import { and, eq, gt } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import {
  emailSendLog,
  emailSuppressionSync,
  type NewEmailSendLog,
} from '../../../database/schemas';

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

}
