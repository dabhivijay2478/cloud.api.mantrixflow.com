/**
 * Dashboard Response DTO
 * Response structure for dashboard overview data
 */

export interface DashboardOverviewDto {
  organization: {
    id: string;
    name: string;
    memberCount: number;
    createdAt: Date;
  };
  pipelines: {
    total: number;
    active: number;
    paused: number;
    failed: number;
    byStatus: {
      running: number;
      completed: number;
      failed: number;
      pending: number;
    };
  };
  recentMigrations: Array<{
    id: string;
    pipelineId: string;
    pipelineName: string;
    status: string;
    startedAt: Date | null;
    completedAt: Date | null;
    rowsProcessed: number | null;
  }>;
  recentActivity: Array<{
    id: string;
    actionType: string;
    entityType: string;
    message: string;
    createdAt: Date;
    userId: string | null;
  }>;
}
