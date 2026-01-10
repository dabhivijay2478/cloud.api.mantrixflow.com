/**
 * Data Pipeline Module
 * NestJS module that wires together all data pipeline components
 */

import { BullModule } from '@nestjs/bullmq';
import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
// Database
import { createDrizzleDatabase } from '../../database/drizzle/database';
// Import data-sources module services (for connection access)
import { ActivityLogModule } from '../activity-logs/activity-log.module';
import { PostgresDataSourceModule } from '../data-sources/postgres/postgres-data-source.module';
import { DataPipelineController } from './data-pipeline.controller';
// Services
import { PostgresDestinationService } from './emitters/postgres-destination.service';
import { PostgresPipelineService } from './postgres-pipeline.service';
import { PipelineDestinationSchemaRepository } from './repositories/pipeline-destination-schema.repository';
import { PipelineSourceSchemaRepository } from './repositories/pipeline-source-schema.repository';
// Repositories
import { PostgresPipelineRepository } from './repositories/postgres-pipeline.repository';

// Jobs
import { PostgresPipelineProcessor } from './shared/jobs/postgres-pipeline.processor';
import { PostgresPipelineQueueService } from './shared/postgres-pipeline-queue.service';
import { PostgresSchemaMapperService } from './transformers/postgres-schema-mapper.service';

@Module({
  imports: [
    // Import data-sources module to access connection services
    PostgresDataSourceModule,
    // Import activity log module for logging pipeline activities
    ActivityLogModule,
    // Register BullMQ queue for pipeline jobs
    BullModule.registerQueue({
      name: 'postgres-pipeline',
    }),
    // Enable scheduling for cron jobs
    ScheduleModule.forRoot(),
  ],
  controllers: [DataPipelineController],
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
    PostgresPipelineService,

    // Repositories
    PostgresPipelineRepository,
    PipelineSourceSchemaRepository,
    PipelineDestinationSchemaRepository,

    // Pipeline services
    PostgresDestinationService,
    PostgresSchemaMapperService,
    PostgresPipelineQueueService,

    // Jobs
    PostgresPipelineProcessor,
  ],
  exports: [PostgresPipelineService, PostgresPipelineQueueService, PostgresPipelineRepository],
})
export class DataPipelineModule {}
