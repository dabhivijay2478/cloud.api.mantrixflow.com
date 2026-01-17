/**
 * Data Pipeline Module
 * NestJS module that wires together all data pipeline components
 *
 * Supports all data source types:
 * - PostgreSQL, MySQL (relational databases)
 * - MongoDB (document database)
 * - S3 (object storage)
 * - REST API (external APIs with rate limiting)
 * - BigQuery, Snowflake (data warehouses)
 *
 * Architecture: Collector → Emitter (with transformation) → Transformer (post-processing)
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

    // Generic Data Services (support all source types)
    CollectorService, // Collects data from sources (Postgres, MySQL, MongoDB, S3, API, BigQuery, Snowflake)
    TransformerService, // Transforms data with mappings and transformations
    EmitterService, // Emits data to destinations with transformation
  ],
  exports: [
    // Export services for use in other modules
    PipelineService,
    SourceSchemaService,
    DestinationSchemaService,
    PipelineLifecycleService,
    CollectorService,
    TransformerService,
    EmitterService,

    // Export repositories for advanced use cases
    PipelineRepository,
    PipelineSourceSchemaRepository,
    PipelineDestinationSchemaRepository,
  ],
})
export class DataPipelineModule {}
