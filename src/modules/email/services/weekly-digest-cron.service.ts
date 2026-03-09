/**
 * Weekly Digest Cron Service
 * Sends weekly pipeline health summary to org owners and admins
 */

import { Inject, Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Cron } from '@nestjs/schedule';
import { and, desc, eq, gte, sql } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import {
  organizations,
  pipelineRuns,
  pipelines,
} from '../../../database/schemas';
import { OrganizationMemberRepository } from '../../organizations/repositories/organization-member.repository';
import { EmailService } from '../email.service';

@Injectable()
export class WeeklyDigestCronService {
  private readonly logger = new Logger(WeeklyDigestCronService.name);

  constructor(
    @Inject('DRIZZLE_DB') private readonly db: NodePgDatabase<any>,
    private readonly emailService: EmailService,
    private readonly configService: ConfigService,
    private readonly organizationMemberRepository: OrganizationMemberRepository,
  ) {}

  /**
   * Every Monday 9AM UTC
   */
  @Cron('0 9 * * 1', { timeZone: 'UTC' })
  async handleWeeklyDigest(): Promise<void> {
    const weekStart = new Date();
    weekStart.setDate(weekStart.getDate() - 7);
    weekStart.setHours(0, 0, 0, 0);

    const orgs = await this.db.select().from(organizations).where(eq(organizations.isActive, true));

    for (const org of orgs) {
      const stats = await this.getOrgPipelineStats(org.id, weekStart);
      if (stats.totalRuns === 0) continue;
      // Skip trial orgs with no pipelines
      if (org.subscriptionStatus === 'trial') {
        const pipelineCount = await this.getPipelineCount(org.id);
        if (pipelineCount === 0) continue;
      }

      const recipientEmails = await this.getOrgOwnerAndAdminEmails(org.id);
      if (recipientEmails.length === 0) continue;

      const frontendUrl = this.configService.get<string>('FRONTEND_URL') ?? '';
      const analyticsUrl = `${frontendUrl}/workspace`;

      try {
        await this.emailService.sendWeeklyDigest({
          recipientEmails,
          orgName: org.name,
          weekStartDate: weekStart.toISOString().split('T')[0],
          totalRuns: stats.totalRuns,
          successRate: stats.successRate,
          failedRuns: stats.failedRuns,
          rowsSynced: stats.rowsSynced,
          topPipelineName: stats.topPipelineName ?? 'N/A',
          analyticsUrl,
          orgId: org.id,
        });
      } catch (err) {
        this.logger.warn(`weekly_digest failed for org ${org.id}: ${err}`);
      }
    }
  }

  private async getOrgPipelineStats(
    organizationId: string,
    since: Date,
  ): Promise<{
    totalRuns: number;
    successRate: number;
    failedRuns: number;
    rowsSynced: number;
    topPipelineName: string | null;
  }> {
    const runs = await this.db
      .select({
        runId: pipelineRuns.id,
        status: pipelineRuns.status,
        rowsWritten: pipelineRuns.rowsWritten,
        pipelineId: pipelineRuns.pipelineId,
      })
      .from(pipelineRuns)
      .where(
        and(
          eq(pipelineRuns.organizationId, organizationId),
          gte(pipelineRuns.createdAt, since),
        ),
      )
      .orderBy(desc(pipelineRuns.createdAt));

    const totalRuns = runs.length;
    const successRuns = runs.filter((r) => r.status === 'success').length;
    const failedRuns = runs.filter((r) => r.status === 'failed').length;
    const successRate = totalRuns > 0 ? Math.round((successRuns / totalRuns) * 100) : 0;
    const rowsSynced = runs.reduce((sum, r) => sum + (r.rowsWritten ?? 0), 0);

    let topPipelineName: string | null = null;
    if (runs.length > 0) {
      const pipelineIdCounts = new Map<string, number>();
      for (const r of runs) {
        if (r.pipelineId) {
          pipelineIdCounts.set(r.pipelineId, (pipelineIdCounts.get(r.pipelineId) ?? 0) + 1);
        }
      }
      const [topPipelineId] = [...pipelineIdCounts.entries()].sort((a, b) => b[1] - a[1])[0] ?? [];
      if (topPipelineId) {
        const [p] = await this.db
          .select({ name: pipelines.name })
          .from(pipelines)
          .where(eq(pipelines.id, topPipelineId))
          .limit(1);
        topPipelineName = p?.name ?? null;
      }
    }

    return {
      totalRuns,
      successRate,
      failedRuns,
      rowsSynced,
      topPipelineName,
    };
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
}
