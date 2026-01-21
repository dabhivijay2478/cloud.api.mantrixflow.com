import { Logger, Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { ActivityLogModule } from './modules/activity-logs/activity-log.module';
import { DashboardModule } from './modules/dashboard/dashboard.module';
import { DataPipelineModule } from './modules/data-pipelines/data-pipeline.module';
import { DataSourceModule } from './modules/data-sources/data-source.module';
import { OnboardingModule } from './modules/onboarding/onboarding.module';
import { OrganizationModule } from './modules/organizations/organization.module';
import { SearchModule } from './modules/search/search.module';
import { UserModule } from './modules/users/user.module';
@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: '.env',
      cache: true,
    }),

    // NestJS native scheduler for simple, non-distributed cron jobs
    // Use for quick, in-memory tasks that run on every instance
    ScheduleModule.forRoot(),

    // Note: PgBoss has been removed - we now use PGMQ (Postgres Message Queue)
    // PGMQ is integrated directly via PGMQService in DataPipelineModule

    // Application Modules
    DataSourceModule,
    DataPipelineModule,
    OrganizationModule,
    UserModule,
    OnboardingModule,
    ActivityLogModule,
    DashboardModule,
    SearchModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
