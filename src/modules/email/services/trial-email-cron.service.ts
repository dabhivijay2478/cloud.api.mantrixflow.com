/**
 * Trial Email Cron Service
 * Daily cron for trial reminder and expiry emails
 */

import { Inject, Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Cron } from '@nestjs/schedule';
import { and, eq, isNotNull, sql } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import {
  dataSources,
  organizations,
  pipelines,
} from '../../../database/schemas';
import { OrganizationMemberRepository } from '../../organizations/repositories/organization-member.repository';
import { UserRepository } from '../../users/repositories/user.repository';
import { EmailService } from '../email.service';

@Injectable()
export class TrialEmailCronService {
  private readonly logger = new Logger(TrialEmailCronService.name);

  constructor(
    @Inject('DRIZZLE_DB') private readonly db: NodePgDatabase<any>,
    private readonly emailService: EmailService,
    private readonly configService: ConfigService,
    private readonly organizationMemberRepository: OrganizationMemberRepository,
    private readonly userRepository: UserRepository,
  ) {}

  /**
   * Daily cron: trial_ends_7_days, trial_ends_1_day, trial_expired
   */
  @Cron('0 10 * * *', { timeZone: 'UTC' }) // 10:00 UTC daily
  async handleTrialEmails(): Promise<void> {
    const now = new Date();

    const trialOrgs = await this.db
      .select()
      .from(organizations)
      .where(
        and(
          eq(organizations.subscriptionStatus, 'trial'),
          isNotNull(organizations.trialEndsAt),
        ),
      );

    for (const org of trialOrgs) {
      const trialEndsAt = org.trialEndsAt;
      if (!trialEndsAt) continue;

      const trialEndDate = new Date(trialEndsAt);
      const daysUntilExpiry = Math.ceil(
        (trialEndDate.getTime() - now.getTime()) / (24 * 60 * 60 * 1000),
      );

      const frontendUrl = this.configService.get<string>('FRONTEND_URL') ?? '';
      const upgradeUrl = `${frontendUrl}/pricing`;

      if (daysUntilExpiry === 7) {
        const recipientEmails = await this.getOrgOwnerAndAdminEmails(org.id);
        if (recipientEmails.length > 0) {
          try {
            const [pipelineCount, connectionCount] = await Promise.all([
              this.getPipelineCount(org.id),
              this.getConnectionCount(org.id),
            ]);
            await this.emailService.sendTrialEnds7Days({
              recipientEmails,
              orgName: org.name,
              trialEndDate: trialEndDate.toISOString().split('T')[0],
              pipelineCount,
              connectionCount,
              rowsSyncedTotal: 0,
              upgradeUrl,
              orgId: org.id,
            });
          } catch (err) {
            this.logger.warn(`trial_ends_7_days failed for org ${org.id}: ${err}`);
          }
        }
      } else if (daysUntilExpiry === 1) {
        const owner = await this.userRepository.findById(org.ownerUserId);
        if (owner?.email) {
          try {
            await this.emailService.sendTrialEnds1Day({
              recipientEmail: owner.email,
              orgName: org.name,
              trialEndDate: trialEndDate.toISOString().split('T')[0],
              upgradeUrl,
              orgId: org.id,
              userId: org.ownerUserId,
            });
          } catch (err) {
            this.logger.warn(`trial_ends_1_day failed for org ${org.id}: ${err}`);
          }
        }
      } else if (daysUntilExpiry <= 0) {
        await this.handleTrialExpired(org);
      }
    }
  }

  private async handleTrialExpired(org: (typeof organizations.$inferSelect)): Promise<void> {
    const [activePipelineCount] = await this.db
      .select({ count: sql<number>`count(*)::int` })
      .from(pipelines)
      .where(
        and(
          eq(pipelines.organizationId, org.id),
          sql`${pipelines.status} != 'paused'`,
          sql`${pipelines.deletedAt} IS NULL`,
        ),
      );

    const toPause = activePipelineCount?.count ?? 0;
    if (toPause > 0) {
      await this.db
        .update(pipelines)
        .set({ status: 'paused', updatedAt: new Date() })
        .where(eq(pipelines.organizationId, org.id));
    }

    await this.db
      .update(organizations)
      .set({
        subscriptionStatus: 'expired',
        updatedAt: new Date(),
      })
      .where(eq(organizations.id, org.id));

    const recipientEmails = await this.getOrgOwnerAndAdminEmails(org.id);
    const frontendUrl = this.configService.get<string>('FRONTEND_URL') ?? '';
    const upgradeUrl = `${frontendUrl}/pricing`;

    if (recipientEmails.length > 0) {
      try {
        await this.emailService.sendTrialExpired({
          recipientEmails,
          orgName: org.name,
          pausedPipelineCount: toPause,
          upgradeUrl,
          orgId: org.id,
        });
      } catch (err) {
        this.logger.warn(`trial_expired email failed for org ${org.id}: ${err}`);
      }
    }
  }

  private async getOrgOwnerAndAdminEmails(organizationId: string): Promise<string[]> {
    const emails = new Set<string>();
    const members = await this.organizationMemberRepository.findByOrganizationId(organizationId);
    for (const m of members) {
      if ((m.role === 'OWNER' || m.role === 'ADMIN') && m.email) {
        emails.add(m.email);
      }
    }
    return Array.from(emails);
  }

  private async getPipelineCount(organizationId: string): Promise<number> {
    const [r] = await this.db
      .select({ count: sql<number>`count(*)::int` })
      .from(pipelines)
      .where(
        and(eq(pipelines.organizationId, organizationId), sql`${pipelines.deletedAt} IS NULL`),
      );
    return r?.count ?? 0;
  }

  private async getConnectionCount(organizationId: string): Promise<number> {
    const [r] = await this.db
      .select({ count: sql<number>`count(*)::int` })
      .from(dataSources)
      .where(eq(dataSources.organizationId, organizationId));
    return r?.count ?? 0;
  }
}
