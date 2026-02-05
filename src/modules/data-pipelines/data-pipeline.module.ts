/**
 * Data Pipeline Module
 * NestJS module that wires together all data pipeline components
 *
 * Supports only three data source types:
 * - PostgreSQL (relational database)
 * - MySQL (relational database)
 * - MongoDB (document database)
 *
 * Architecture:
 * - NestJS: Orchestration, CRUD, user/org management, activity logging
 * - Python FastAPI: ETL operations (collect, transform, emit)
 * - BullMQ + Redis: Job queuing, scheduling, CDC polling, real-time pub/sub
 * - Socket.io: Real-time updates
 *
 * Features:
 * - Incremental sync with checkpoint tracking (WAL CDC for PostgreSQL)
 * - BullMQ queues: pipeline-jobs, incremental-sync, polling-checks
 * - Polling-based CDC (delta check every 5 min via repeatable job)
 * - Redis pub/sub for status/progress → Socket.io gateway
 *
 * Guide: To add a new data source type:
 * 1. Add connector in Python service: etl-service/connectors/{source-name}.py
 * 2. Register in Python main.py CONNECTORS dict
 * 3. Add type to DataSourceType enum (postgresql, mysql, mongodb only)
 */

import { Module, forwardRef } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { HttpModule } from '@nestjs/axios';
import { createDrizzleDatabase } from '../../database/drizzle/database';
import { ActivityLogModule } from '../activity-logs/activity-log.module';
import { DataSourceModule } from '../data-sources/data-source.module';
import { OrganizationModule } from '../organizations/organization.module';

// Controllers
import { PipelineController } from './pipeline.controller';
import { SourceSchemaController } from './source-schema.controller';
import { DestinationSchemaController } from './destination-schema.controller';

// Services
import { PipelineService } from './services/pipeline.service';
import { SourceSchemaService } from './services/source-schema.service';
import { DestinationSchemaService } from './services/destination-schema.service';
import { PythonETLService } from './services/python-etl.service';
import { PipelineLifecycleService } from './services/pipeline-lifecycle.service';
import { PipelineSchedulerService } from './services/pipeline-scheduler.service';
import { ScheduledPipelineWorkerService } from './services/scheduled-pipeline-worker.service';
import { SchemaValidationService } from './services/schema-validation.service';
import {
  PipelineJobsProcessor,
  IncrementalSyncProcessor,
  PollingChecksProcessor,
} from './services/pipeline-job-processor.service';

// Queue (BullMQ + Redis)
import { BullmqModule } from '../queue/bullmq.module';

// Gateways
import { PipelineUpdatesGateway } from './gateways/pipeline-updates.gateway';

// Repositories
import { PipelineRepository } from './repositories/pipeline.repository';
import { PipelineSourceSchemaRepository } from './repositories/pipeline-source-schema.repository';
import { PipelineDestinationSchemaRepository } from './repositories/pipeline-destination-schema.repository';

@Module({
  imports: [
    // Import data-sources module to access connection services
    // Using forwardRef to handle potential circular dependencies
    forwardRef(() => DataSourceModule),

    // Import organization module for role-based authorization
    forwardRef(() => OrganizationModule),

    // Import activity log module for logging pipeline activities
    ActivityLogModule,

    // BullMQ + Redis for job queuing, scheduling, and real-time pub/sub
    BullmqModule,

    // HTTP module for API collector/emitter with custom configuration
    HttpModule.register({
      timeout: 30000, // 30 second timeout
      maxRedirects: 5,
      headers: {
        'User-Agent': 'DataPipeline/1.0',
      },
    }),
  ],
  controllers: [PipelineController, SourceSchemaController, DestinationSchemaController],
  providers: [
    // Database provider using Drizzle ORM
    {
      provide: 'DRIZZLE_DB',
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => {
        return createDrizzleDatabase(configService);
      },
    },

    // Repositories - Data access layer using Drizzle ORM
    PipelineRepository,
    PipelineSourceSchemaRepository,
    PipelineDestinationSchemaRepository,

    // Core Services
    PipelineService,
    SourceSchemaService,
    DestinationSchemaService,
    PipelineLifecycleService,
    PipelineSchedulerService, // Handles pipeline scheduling configuration
    ScheduledPipelineWorkerService, // Worker for processing scheduled pipeline jobs

    // Python ETL Service - HTTP client for Python FastAPI microservice
    PythonETLService, // Handles collect, transform, emit via Python service

    // BullMQ job processors (workers for pipeline-jobs, incremental-sync, polling-checks)
    PipelineJobsProcessor,
    IncrementalSyncProcessor,
    PollingChecksProcessor,

    // Schema Validation
    SchemaValidationService, // Validates database schema on startup

    // WebSocket Gateway
    PipelineUpdatesGateway, // Real-time updates via Socket.io
  ],
  exports: [
    // Export services for use in other modules
    // (PipelineQueueService is from BullmqModule; import BullmqModule where needed.)
    PipelineService,
    SourceSchemaService,
    DestinationSchemaService,
    PipelineLifecycleService,
    PipelineSchedulerService,
    PythonETLService,

    // Export repositories for advanced use cases
    PipelineRepository,
    PipelineSourceSchemaRepository,
    PipelineDestinationSchemaRepository,
  ],
})
export class DataPipelineModule {}
