/**
 * Data Source Module
 * Module for data source and connection management
 */

import { forwardRef, Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { HttpModule } from '@nestjs/axios';
import { createDrizzleDatabase } from '../../database/drizzle/database';
import { ActivityLogModule } from '../activity-logs/activity-log.module';
import { ConnectorsModule } from '../connectors/connectors.module';
import { OrganizationModule } from '../organizations/organization.module';
import { UserModule } from '../users/user.module';
import { DataSourceController } from './data-source.controller';
import { CdcVerifyService } from './cdc-verify.service';
import { ConnectionService } from './connection.service';
import { DataSourceService } from './data-source.service';
import { DataSourceConnectionRepository } from './repositories/data-source-connection.repository';
import { DataSourceRepository } from './repositories/data-source.repository';
import { EncryptionService } from '../../common/encryption/encryption.service';

@Module({
  imports: [
    ActivityLogModule,
    forwardRef(() => ConnectorsModule),
    OrganizationModule,
    UserModule,
    HttpModule.register({
      timeout: 30000,
      maxRedirects: 5,
    }),
  ],
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
    CdcVerifyService,
    // Repositories
    DataSourceRepository,
    DataSourceConnectionRepository,
    // Common services
    EncryptionService,
  ],
  exports: [
    DataSourceService,
    ConnectionService,
    DataSourceRepository,
    DataSourceConnectionRepository,
  ],
})
export class DataSourceModule {}
