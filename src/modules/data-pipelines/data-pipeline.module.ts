/**
 * Data Pipeline Module
 * NestJS module that wires together all data pipeline components
 */

import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { BullModule } from '@nestjs/bullmq';
import { ScheduleModule } from '@nestjs/schedule';
import { DataPipelineController } from './data-pipeline.controller';
import { PostgresPipelineService } from './postgres-pipeline.service';

// Repositories
import { PostgresPipelineRepository } from './repositories/postgres-pipeline.repository';

// Services
import { PostgresDestinationService } from './emitters/postgres-destination.service';
import { PostgresSchemaMapperService } from './transformers/postgres-schema-mapper.service';
import { PostgresPipelineQueueService } from './shared/postgres-pipeline-queue.service';

// Jobs
import { PostgresPipelineProcessor } from './shared/jobs/postgres-pipeline.processor';

// Import data-sources module services (for connection access)
import { PostgresDataSourceModule } from '../data-sources/postgres/postgres-data-source.module';

// Database
import { createDrizzleDatabase } from '../../database/drizzle/database';

@Module({
  imports: [
    // Import data-sources module to access connection services
    PostgresDataSourceModule,
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

    // Pipeline services
    PostgresDestinationService,
    PostgresSchemaMapperService,
    PostgresPipelineQueueService,

    // Jobs
    PostgresPipelineProcessor,
  ],
  exports: [PostgresPipelineService, PostgresPipelineQueueService],
})
export class DataPipelineModule { }

