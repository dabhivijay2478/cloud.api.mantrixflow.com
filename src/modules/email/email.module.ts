/**
 * Email Module
 * Transactional emails via UnoSend
 */

import { HttpModule } from '@nestjs/axios';
import { forwardRef, Module } from '@nestjs/common';
import { EmailService } from './email.service';
import { BillingWebhookController } from './billing-webhook.controller';
import { EmailWebhookController } from './email-webhook.controller';
import { EmailTestController } from './email-test.controller';
import { EmailRepository } from './repositories/email-repository';
import { TrialEmailCronService } from './services/trial-email-cron.service';
import { WeeklyDigestCronService } from './services/weekly-digest-cron.service';
import { OrganizationModule } from '../organizations/organization.module';
import { UserModule } from '../users/user.module';

@Module({
  imports: [
    HttpModule.register({
      timeout: 10_000,
      maxRedirects: 0,
    }),
    forwardRef(() => OrganizationModule),
    forwardRef(() => UserModule),
  ],
  controllers: [EmailWebhookController, BillingWebhookController, EmailTestController],
  providers: [EmailService, EmailRepository, TrialEmailCronService, WeeklyDigestCronService],
  exports: [EmailService, EmailRepository],
})
export class EmailModule {}
