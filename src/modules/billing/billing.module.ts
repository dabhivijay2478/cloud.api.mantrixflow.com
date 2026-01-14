/**
 * Billing Module
 * Module for billing information and Dodo Payments integration
 * Provider-agnostic billing system
 */

import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createDrizzleDatabase } from '../../database/drizzle/database';
import { OrganizationModule } from '../organizations/organization.module';
import { UserModule } from '../users/user.module';
import { BillingController } from './billing.controller';
import { BillingService } from './billing.service';
import { SubscriptionRepository } from './repositories/subscription.repository';
import { SubscriptionEventRepository } from './repositories/subscription-event.repository';
import { DodoBillingProvider } from './providers/dodo-billing.provider';

@Module({
  imports: [OrganizationModule, UserModule],
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
    // Billing providers
    DodoBillingProvider,
    // Repositories
    SubscriptionRepository,
    SubscriptionEventRepository,
    // Services
    BillingService,
  ],
  exports: [BillingService],
})
export class BillingModule {}
