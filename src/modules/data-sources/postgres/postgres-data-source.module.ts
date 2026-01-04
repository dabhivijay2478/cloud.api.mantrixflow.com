/**
 * PostgreSQL Connector Module
 * NestJS module that wires together all PostgreSQL connector components
 */

import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
// Common services
import { EncryptionService } from '../../../common/encryption/encryption.service';
// Database
import { createDrizzleDatabase } from '../../../database/drizzle/database';
import { PostgresValidator } from './postgres.validator';
import { PostgresDataSourceController } from './postgres-data-source.controller';
import { PostgresDataSourceService } from './postgres-data-source.service';
// Repositories
import { PostgresConnectionRepository } from './repositories/postgres-connection.repository';
import { PostgresQueryLogRepository } from './repositories/postgres-query-log.repository';
import { PostgresSyncJobRepository } from './repositories/postgres-sync-job.repository';
// Services
import { PostgresConnectionPoolService } from './services/postgres-connection-pool.service';
import { PostgresHealthMonitorService } from './services/postgres-health-monitor.service';
import { PostgresQueryExecutorService } from './services/postgres-query-executor.service';
import { PostgresSchemaDiscoveryService } from './services/postgres-schema-discovery.service';
import { PostgresSyncService } from './services/postgres-sync.service';

@Module({
  imports: [
    // Enable scheduling for cron jobs (for health monitoring)
    ScheduleModule.forRoot(),
  ],
  controllers: [PostgresDataSourceController],
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
    PostgresDataSourceService,

    // Validator
    PostgresValidator,

    // Repositories
    PostgresConnectionRepository,
    PostgresSyncJobRepository,
    PostgresQueryLogRepository,

    // Core services
    PostgresConnectionPoolService,
    PostgresSchemaDiscoveryService,
    PostgresQueryExecutorService,
    PostgresSyncService,
    PostgresHealthMonitorService,

    // Common services
    EncryptionService,
  ],
  exports: [
    PostgresDataSourceService,
    PostgresConnectionPoolService,
    PostgresQueryExecutorService,
    PostgresConnectionRepository,
  ],
})
export class PostgresDataSourceModule {}
