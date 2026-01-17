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
import { PgBossModule } from './modules/queue';

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

    // PgBoss Queue Module - PostgreSQL-based job queue
    // Replaces BullMQ (Redis-based) with no additional infrastructure
    // Features:
    // - Exactly-once job delivery
    // - Distributed cron scheduling (only one instance runs)
    // - Job persistence and retry with exponential backoff
    // - Dead letter queues
    // - Priority queues
    PgBossModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => {
        const databaseUrl = configService.get<string>('DATABASE_URL');
        const logger = new Logger('PgBossModule');

        if (databaseUrl) {
          logger.log('Initializing PgBoss with DATABASE_URL');
          return {
            connectionString: databaseUrl,
            application_name: 'ai-bi-queue',
            schema: 'pgboss', // Separate schema for queue tables
            retryLimit: 3,
            retryDelay: 60, // 60 seconds
            retryBackoff: true, // Exponential backoff
            expireInHours: 24, // Jobs expire after 24 hours
            archiveCompletedAfterSeconds: 3600, // Archive completed jobs after 1 hour
            deleteAfterDays: 7, // Delete archived jobs after 7 days
            monitorStateIntervalSeconds: 30,
            maintenanceIntervalSeconds: 120,
          };
        }

        // Fallback to individual configuration
        logger.log('Initializing PgBoss with individual config');
        return {
          host: configService.get('DB_HOST', 'localhost'),
          port: configService.get<number>('DB_PORT', 5432),
          database: configService.get('DB_NAME', 'postgres'),
          user: configService.get('DB_USER', 'postgres'),
          password: configService.get<string>('DB_PASSWORD'),
          schema: 'pgboss',
          application_name: 'ai-bi-queue',
          retryLimit: 3,
          retryDelay: 60,
          retryBackoff: true,
        };
      },
    }),

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
