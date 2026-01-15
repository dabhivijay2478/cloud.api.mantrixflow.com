import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createDrizzleDatabase } from '../../database/drizzle/database';
import { BillingController } from './billing.controller';
import { BillingService } from './billing.service';
import { SubscriptionRepository } from './repositories/subscription.repository';
import { SubscriptionEventRepository } from './repositories/subscription-event.repository';
import { DodoCustomerRepository } from './repositories/dodo-customer.repository';

@Module({
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
    SubscriptionRepository,
    SubscriptionEventRepository,
    DodoCustomerRepository,
  ],
  exports: [BillingService, SubscriptionRepository],
})
export class BillingModule {}
