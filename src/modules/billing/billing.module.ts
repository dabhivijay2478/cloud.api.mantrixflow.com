/**
 * Billing Module
 * Module for billing information and Stripe integration
 * Billing is organization-scoped with OWNER/ADMIN access
 */

import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createDrizzleDatabase } from '../../database/drizzle/database';
import { OrganizationModule } from '../organizations/organization.module';
import { BillingController } from './billing.controller';
import { BillingService } from './billing.service';
import { BillingRepository } from './repositories/billing.repository';

@Module({
  imports: [OrganizationModule],
  controllers: [BillingController],
  providers: [
    // Database provider
    {
      provide: 'DRIZZLE_DB',
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => {
        return createDrizzleDatabase(configService);
      },
    },
    BillingService,
    BillingRepository,
  ],
  exports: [BillingService],
})
export class BillingModule {}
