/**
 * Data Pipeline Module
 * NestJS module that wires together all data pipeline components
 */

import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createDrizzleDatabase } from '../../database/drizzle/database';
import { ActivityLogModule } from '../activity-logs/activity-log.module';
import { DataSourceModule } from '../data-sources/data-source.module';
import { PipelineController } from './pipeline.controller';
import { SourceSchemaController } from './source-schema.controller';
import { DestinationSchemaController } from './destination-schema.controller';
import { PipelineService } from './services/pipeline.service';
import { SourceSchemaService } from './services/source-schema.service';
import { DestinationSchemaService } from './services/destination-schema.service';
import { CollectorService } from './services/collector.service';
import { TransformerService } from './services/transformer.service';
import { EmitterService } from './services/emitter.service';
import { PipelineRepository } from './repositories/pipeline.repository';
import { PipelineSourceSchemaRepository } from './repositories/pipeline-source-schema.repository';
import { PipelineDestinationSchemaRepository } from './repositories/pipeline-destination-schema.repository';

@Module({
  imports: [
    // Import data-sources module to access connection services
    DataSourceModule,
    // Import activity log module for logging pipeline activities
    ActivityLogModule,
  ],
  controllers: [PipelineController, SourceSchemaController, DestinationSchemaController],
  providers: [
    // Database provider
    {
      provide: 'DRIZZLE_DB',
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => {
        return createDrizzleDatabase(configService);
      },
    },

    // Repositories
    PipelineRepository,
    PipelineSourceSchemaRepository,
    PipelineDestinationSchemaRepository,

    // Services
    PipelineService,
    SourceSchemaService,
    DestinationSchemaService,
    CollectorService,
    TransformerService,
    EmitterService,
  ],
  exports: [PipelineService, SourceSchemaService, DestinationSchemaService, PipelineRepository],
})
export class DataPipelineModule {}
