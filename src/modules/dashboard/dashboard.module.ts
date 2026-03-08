/**
 * Dashboard Module
 * Module for dashboard data aggregation
 */

import { Module } from '@nestjs/common';
import { DashboardController } from './dashboard.controller';
import { DashboardService } from './dashboard.service';
import { OrganizationModule } from '../organizations/organization.module';
import { DataPipelineModule } from '../data-pipelines/data-pipeline.module';
import { ActivityLogModule } from '../activity-logs/activity-log.module';

@Module({
  imports: [OrganizationModule, DataPipelineModule, ActivityLogModule],
  controllers: [DashboardController],
  providers: [DashboardService],
  exports: [DashboardService],
})
export class DashboardModule {}
