/**
 * PostgreSQL Connector Module
 * NestJS module that wires together all PostgreSQL connector components
 */

import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { BullModule } from '@nestjs/bullmq';
import { ScheduleModule } from '@nestjs/schedule';
import { PostgresController } from './postgres.controller';
import { PostgresService } from './postgres.service';
import { PostgresValidator } from './postgres.validator';

// Repositories
import { PostgresConnectionRepository } from './repositories/postgres-connection.repository';
import { PostgresSyncJobRepository } from './repositories/postgres-sync-job.repository';
import { PostgresQueryLogRepository } from './repositories/postgres-query-log.repository';
import { PostgresPipelineRepository } from './repositories/postgres-pipeline.repository';

// Services
import { PostgresConnectionPoolService } from './services/postgres-connection-pool.service';
import { PostgresSchemaDiscoveryService } from './services/postgres-schema-discovery.service';
import { PostgresQueryExecutorService } from './services/postgres-query-executor.service';
import { PostgresSyncService } from './services/postgres-sync.service';
import { PostgresHealthMonitorService } from './services/postgres-health-monitor.service';
import { PostgresDestinationService } from './services/postgres-destination.service';
import { PostgresPipelineService } from './services/postgres-pipeline.service';
import { PostgresSchemaMapperService } from './services/postgres-schema-mapper.service';
import { PostgresPipelineQueueService } from './services/postgres-pipeline-queue.service';

// Jobs
import { PostgresPipelineProcessor } from './jobs/postgres-pipeline.processor';

// Common services
import { EncryptionService } from '../../../common/encryption/encryption.service';

// Database
import { createDrizzleDatabase } from '../../../database/drizzle/database';

@Module({
  imports: [
    // Register BullMQ queue for pipeline jobs
    BullModule.registerQueue({
      name: 'postgres-pipeline',
    }),
    // Enable scheduling for cron jobs
    ScheduleModule.forRoot(),
  ],
  controllers: [PostgresController],
  providers: [
    // Database provider
    {
      provide: 'DRIZZLE_DB',
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => {
        return createDrizzleDatabase(configService);
      },
    },

    // Main service
    PostgresService,

    // Validator
    PostgresValidator,

    // Repositories
    PostgresConnectionRepository,
    PostgresSyncJobRepository,
    PostgresQueryLogRepository,
    PostgresPipelineRepository,

    // Core services
    PostgresConnectionPoolService,
    PostgresSchemaDiscoveryService,
    PostgresQueryExecutorService,
    PostgresSyncService,
    PostgresHealthMonitorService,

    // Pipeline services
    PostgresDestinationService,
    PostgresPipelineService,
    PostgresSchemaMapperService,
    PostgresPipelineQueueService,

    // Jobs
    PostgresPipelineProcessor,

    // Common services
    EncryptionService,
  ],
  exports: [PostgresService, PostgresPipelineService, PostgresPipelineQueueService],
})
export class PostgresModule { }
