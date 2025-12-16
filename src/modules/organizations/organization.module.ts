import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { OrganizationController } from './organization.controller';
import { OrganizationService } from './organization.service';
import { OrganizationRepository } from './repositories/organization.repository';
import { createDrizzleDatabase } from '../../database/drizzle/database';

@Module({
  controllers: [OrganizationController],
  providers: [
    // Database provider
    {
      provide: 'DRIZZLE_DB',
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => {
        return createDrizzleDatabase(configService);
      },
    },
    OrganizationService,
    OrganizationRepository,
  ],
  exports: [OrganizationService],
})
export class OrganizationModule {}
