/**
 * Data Pipeline Module
 * NestJS module that wires together all data pipeline components
 *
 * Supports only three data source types:
 * - PostgreSQL (relational database)
 * - MySQL (relational database)
 * - MongoDB (document database)
 *
 * Architecture: Collector → Emitter (with transformation) → Transformer (post-processing)
 *
 * Features:
 * - Incremental sync with checkpoint tracking
 * - PgBoss for job queuing (exactly-once delivery, cron scheduling, retries)
 * - Socket.io for real-time updates
 *
 * Guide: To add a new data source type:
 * 1. Create handler in services/handlers/
 * 2. Register in handler-registry.ts
 * 3. Add type to DataSourceType enum
 * 4. Add emitter/collector methods for the new type
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
import { CollectorService } from './services/collector.service';
import { TransformerService } from './services/transformer.service';
import { EmitterService } from './services/emitter.service';
import { PipelineLifecycleService } from './services/pipeline-lifecycle.service';
import { PipelineSchedulerService } from './services/pipeline-scheduler.service';
import { ScheduledPipelineWorkerService } from './services/scheduled-pipeline-worker.service';
import { SchemaValidationService } from './services/schema-validation.service';

// PgBoss Services (replaces PGMQ and pg_cron)
import { PgBossService } from './services/pgboss.service';
import { PgBossJobHandlerService } from './services/pgboss-job-handler.service';

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

    // Generic Data Services (support PostgreSQL, MySQL, MongoDB only)
    CollectorService, // Collects data from sources (Postgres, MySQL, MongoDB)
    TransformerService, // Transforms data with mappings and transformations
    EmitterService, // Emits data to destinations with transformation

    // PgBoss Services - Job queue and handlers
    // PgBoss provides: exactly-once delivery, cron scheduling, priority queues,
    // automatic retries with exponential backoff, dead letter queues, pub/sub
    PgBossService, // Core PgBoss service for job management
    PgBossJobHandlerService, // Job handlers for sync operations

    // Schema Validation
    SchemaValidationService, // Validates database schema on startup

    // WebSocket Gateway
    PipelineUpdatesGateway, // Real-time updates via Socket.io
  ],
  exports: [
    // Export services for use in other modules
    PipelineService,
    SourceSchemaService,
    DestinationSchemaService,
    PipelineLifecycleService,
    PipelineSchedulerService,
    CollectorService,
    TransformerService,
    EmitterService,
    PgBossService,

    // Export repositories for advanced use cases
    PipelineRepository,
    PipelineSourceSchemaRepository,
    PipelineDestinationSchemaRepository,
  ],
})
export class DataPipelineModule {}
