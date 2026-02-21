/**
 * ETL Jobs Module
 * Async ETL queue using pgmq (NO Redis, NO BullMQ)
 */

import { Module, forwardRef } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { HttpModule } from '@nestjs/axios';
import { createDrizzleDatabase } from '../../database/drizzle/database';
import { PgmqModule } from '../queue';
import { EtlJobsService } from './etl-jobs.service';
import { EtlJobsController } from './etl-jobs.controller';
import { EtlJobsSchedulerService } from './etl-jobs-scheduler.service';
import { DataPipelineModule } from '../data-pipelines/data-pipeline.module';
import { DataSourceModule } from '../data-sources/data-source.module';

@Module({
  imports: [
    ConfigModule,
    PgmqModule,
    forwardRef(() => DataPipelineModule),
    forwardRef(() => DataSourceModule),
    HttpModule.register({
      timeout: 15000,
      maxRedirects: 3,
    }),
  ],
  controllers: [EtlJobsController],
  providers: [
    EtlJobsService,
    EtlJobsSchedulerService,
    {
      provide: 'DRIZZLE_DB',
      inject: [ConfigService],
      useFactory: (configService: ConfigService) =>
        createDrizzleDatabase(configService),
    },
  ],
  exports: [EtlJobsService],
})
export class EtlJobsModule {}
