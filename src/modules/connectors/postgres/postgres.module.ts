/**
 * PostgreSQL Connector Module
 * NestJS module that wires together all PostgreSQL connector components
 */

import { Module } from '@nestjs/common';
import { PostgresController } from './postgres.controller';
import { PostgresService } from './postgres.service';
import { PostgresValidator } from './postgres.validator';

// Repositories
import { PostgresConnectionRepository } from './repositories/postgres-connection.repository';
import { PostgresSyncJobRepository } from './repositories/postgres-sync-job.repository';
import { PostgresQueryLogRepository } from './repositories/postgres-query-log.repository';

// Services
import { PostgresConnectionPoolService } from './services/postgres-connection-pool.service';
import { PostgresSchemaDiscoveryService } from './services/postgres-schema-discovery.service';
import { PostgresQueryExecutorService } from './services/postgres-query-executor.service';
import { PostgresSyncService } from './services/postgres-sync.service';
import { PostgresHealthMonitorService } from './services/postgres-health-monitor.service';

// Common services
import { EncryptionService } from '../../../common/encryption/encryption.service';

@Module({
  controllers: [PostgresController],
  providers: [
    // Main service
    PostgresService,

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
  exports: [PostgresService],
})
export class PostgresModule {}
