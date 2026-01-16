/**
 * Dashboard Service
 * Aggregates data from multiple sources for dashboard overview
 */

import { Inject, Injectable } from '@nestjs/common';
import { ActivityLogService } from '../activity-logs/activity-log.service';
import { PostgresPipelineRepository } from '../data-pipelines/repositories/postgres-pipeline.repository';
import { OrganizationRepository } from '../organizations/repositories/organization.repository';
import { OrganizationMemberRepository } from '../organizations/repositories/organization-member.repository';
import type { DashboardOverviewDto } from './dto/dashboard-response.dto';
import { eq, and, desc, inArray } from 'drizzle-orm';
import { pipelineRuns } from '../../database/schemas';
import type { DrizzleDatabase } from '../../database/drizzle/database';

@Injectable()
export class DashboardService {
  constructor(
    private readonly organizationRepository: OrganizationRepository,
    private readonly memberRepository: OrganizationMemberRepository,
    private readonly pipelineRepository: PostgresPipelineRepository,
    private readonly activityLogService: ActivityLogService,
    @Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase,
  ) {}

  /**
   * Get dashboard overview for an organization
   */
  async getDashboardOverview(organizationId: string): Promise<DashboardOverviewDto> {
    // Get organization
    const organization = await this.organizationRepository.findById(organizationId);
    if (!organization) {
      throw new Error(`Organization with ID "${organizationId}" not found`);
    }

    // Get member count
    const members = await this.memberRepository.findByOrganizationId(organizationId);
    const activeMembers = members.filter(
      (m) => m.status === 'active' || m.status === 'accepted',
    );
    const memberCount = activeMembers.length;

    // Get pipeline statistics
    const pipelines = await this.pipelineRepository.findByOrg(organizationId);
    const totalPipelines = pipelines.length;
    const activePipelines = pipelines.filter((p) => p.status === 'active' && !p.deletedAt).length;
    const pausedPipelines = pipelines.filter((p) => p.status === 'paused' && !p.deletedAt).length;

    // Get pipeline runs for status breakdown
    const pipelineIds = pipelines.map((p) => p.id);
    const pipelineRunsByStatus = {
      running: 0,
      completed: 0,
      failed: 0,
      pending: 0,
    };

    if (pipelineIds.length > 0) {
      // Get recent runs to determine status breakdown
      const recentRuns = await this.db
        .select({
          status: pipelineRuns.jobState,
        })
        .from(pipelineRuns)
        .where(
          and(
            inArray(pipelineRuns.pipelineId, pipelineIds),
            eq(pipelineRuns.organizationId, organizationId),
          ),
        )
        .orderBy(desc(pipelineRuns.createdAt))
        .limit(100);

      // Count by status
      recentRuns.forEach((run) => {
        const status = run.status?.toLowerCase() || 'pending';
        if (status === 'running' || status === 'processing') {
          pipelineRunsByStatus.running++;
        } else if (status === 'completed' || status === 'success') {
          pipelineRunsByStatus.completed++;
        } else if (status === 'failed' || status === 'error') {
          pipelineRunsByStatus.failed++;
        } else {
          pipelineRunsByStatus.pending++;
        }
      });
    }

    // Count failed pipelines (pipelines with recent failed runs)
    const failedPipelines = 0; // Simplified - could be enhanced

    // Get recent migrations (pipeline runs)
    const recentMigrations = await this.getRecentMigrations(organizationId, 10);

    // Get recent activity logs
    const recentActivityResult = await this.activityLogService.getActivityLogs(
      organizationId,
      {},
      { limit: 10 },
    );
    const recentActivity = recentActivityResult.logs.map((log) => ({
      id: log.id,
      actionType: log.actionType,
      entityType: log.entityType,
      message: log.message,
      createdAt: log.createdAt,
      userId: log.userId,
    }));

    return {
      organization: {
        id: organization.id,
        name: organization.name,
        memberCount,
        createdAt: organization.createdAt,
      },
      pipelines: {
        total: totalPipelines,
        active: activePipelines,
        paused: pausedPipelines,
        failed: failedPipelines,
        byStatus: pipelineRunsByStatus,
      },
      recentMigrations,
      recentActivity,
    };
  }

  /**
   * Get recent pipeline runs (migrations)
   */
  private async getRecentMigrations(
    organizationId: string,
    limit: number = 10,
  ): Promise<DashboardOverviewDto['recentMigrations']> {
    // Get all pipelines for this organization
    const pipelines = await this.pipelineRepository.findByOrg(organizationId);
    const pipelineIds = pipelines.map((p) => p.id);
    const pipelineMap = new Map(pipelines.map((p) => [p.id, p]));

    if (pipelineIds.length === 0) {
      return [];
    }

    // Get recent runs
    const runs = await this.db
      .select()
      .from(pipelineRuns)
      .where(
        and(
          inArray(pipelineRuns.pipelineId, pipelineIds),
          eq(pipelineRuns.organizationId, organizationId),
        ),
      )
      .orderBy(desc(pipelineRuns.createdAt))
      .limit(limit);

    return runs.map((run) => {
      const pipeline = pipelineMap.get(run.pipelineId);
      return {
        id: run.id,
        pipelineId: run.pipelineId,
        pipelineName: pipeline?.name || 'Unknown Pipeline',
        status: run.jobState || 'pending',
        startedAt: run.startedAt,
        completedAt: run.completedAt,
        rowsProcessed: run.rowsWritten || run.rowsRead || null,
      };
    });
  }
}
