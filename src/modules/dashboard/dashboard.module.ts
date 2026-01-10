/**
 * Dashboard Module
 * Module for dashboard data aggregation
 */

import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createDrizzleDatabase } from '../../database/drizzle/database';
import { DashboardController } from './dashboard.controller';
import { DashboardService } from './dashboard.service';
import { OrganizationModule } from '../organizations/organization.module';
import { DataPipelineModule } from '../data-pipelines/data-pipeline.module';
import { ActivityLogModule } from '../activity-logs/activity-log.module';

@Module({
  imports: [OrganizationModule, DataPipelineModule, ActivityLogModule],
  controllers: [DashboardController],
  providers: [
    // Database provider
    {
      provide: 'DRIZZLE_DB',
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => {
        return createDrizzleDatabase(configService);
      },
    },
    DashboardService,
  ],
  exports: [DashboardService],
})
export class DashboardModule {}
