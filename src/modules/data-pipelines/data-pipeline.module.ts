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
 * - Python FastAPI (apps/new-etl): ETL operations (discover, preview, runSync)
 * - pgmq + pg_cron: Job queuing, scheduling, CDC polling (Supabase-native)
 * - Socket.io: Real-time updates (via Supabase Realtime + Postgres NOTIFY)
 *
 * Features:
 * - Incremental sync with checkpoint tracking (WAL CDC for PostgreSQL)
 * - pgmq queues: pipeline_jobs, incremental_sync, polling_checks
 * - Polling-based CDC (delta check every 5 min via pg_cron → pgmq)
 * - Postgres NOTIFY + Supabase Realtime for status/progress → Socket.io gateway
 *
 * Guide: To add a new data source type:
 * 1. Add connector in Python service: apps/new-etl
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
import { InternalEtlController } from './internal.controller';

// Services
import { PipelineService } from './services/pipeline.service';
import { SourceSchemaService } from './services/source-schema.service';
import { DestinationSchemaService } from './services/destination-schema.service';
import { PythonETLService } from './services/python-etl.service';
import { PipelineLifecycleService } from './services/pipeline-lifecycle.service';
import { PipelineSchedulerService } from './services/pipeline-scheduler.service';
import { ScheduledPipelineWorkerService } from './services/scheduled-pipeline-worker.service';
import { SchemaValidationService } from './services/schema-validation.service';
import { PipelineJobProcessor } from './services/pipeline-job-processor.service';

// Queue (pgmq + pg_cron)
import { PgmqModule } from '../queue';

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

    // pgmq + pg_cron for job queuing, scheduling, and real-time status updates
    PgmqModule,

    // HTTP module for API collector/emitter with custom configuration
    HttpModule.register({
      timeout: 120_000, // ETL discover/preview/sync; per-request timeout overrides when set
      maxRedirects: 5,
      headers: {
        'User-Agent': 'DataPipeline/1.0',
      },
    }),
  ],
  controllers: [PipelineController, SourceSchemaController, DestinationSchemaController, InternalEtlController],
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

    // Python ETL Service - HTTP client for apps/new-etl (discover, preview, runSync)
    PythonETLService,

    // pgmq job processor (polls pipeline_jobs, incremental_sync, polling_checks queues)
    PipelineJobProcessor,

    // Schema Validation
    SchemaValidationService, // Validates database schema on startup

    // WebSocket Gateway
    PipelineUpdatesGateway, // Real-time updates via Socket.io
  ],
  exports: [
    // Export services for use in other modules
    // (PgmqQueueService is from PgmqModule; import PgmqModule where needed.)
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
