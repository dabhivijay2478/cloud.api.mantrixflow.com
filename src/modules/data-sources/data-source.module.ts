/**
 * Data Source Module
 * Module for data source and connection management
 */

import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createDrizzleDatabase } from '../../database/drizzle/database';
import { ActivityLogModule } from '../activity-logs/activity-log.module';
import { OrganizationModule } from '../organizations/organization.module';
import { DataSourceController } from './data-source.controller';
import { ConnectionService } from './connection.service';
import { DataSourceService } from './data-source.service';
import { DataSourceConnectionRepository } from './repositories/data-source-connection.repository';
import { DataSourceRepository } from './repositories/data-source.repository';
import { EncryptionService } from '../../common/encryption/encryption.service';

@Module({
  imports: [ActivityLogModule, OrganizationModule],
  controllers: [DataSourceController],
  providers: [
    // Database provider
    {
      provide: 'DRIZZLE_DB',
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => {
        return createDrizzleDatabase(configService);
      },
    },
    // Services
    DataSourceService,
    ConnectionService,
    // Repositories
    DataSourceRepository,
    DataSourceConnectionRepository,
    // Common services
    EncryptionService,
  ],
  exports: [DataSourceService, ConnectionService, DataSourceRepository, DataSourceConnectionRepository],
})
export class DataSourceModule {}
