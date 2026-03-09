/**
 * Email Test Controller
 * Triggers all transactional emails with fake data for testing.
 * Protected by INTERNAL_TOKEN.
 *
 * POST /internal/email/test-all
 * Body: { "to": "vijaydabhi0428@gmail.com" } (optional, defaults to vijaydabhi0428@gmail.com)
 */

import {
  Body,
  Controller,
  Headers,
  Logger,
  Post,
  UnauthorizedException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { IsEmail, IsOptional } from 'class-validator';
import { EmailService } from './email.service';

const TEST_EMAIL = 'vijaydabhi0428@gmail.com';

class TestEmailDto {
  @IsOptional()
  @IsEmail()
  to?: string;
}
const FAKE_ORG_ID = '00000000-0000-0000-0000-000000000001';
const FAKE_USER_ID = '00000000-0000-0000-0000-000000000002';
const FAKE_PIPELINE_ID = '00000000-0000-0000-0000-000000000003';

@Controller('internal/email')
export class EmailTestController {
  private readonly logger = new Logger(EmailTestController.name);

  constructor(
    private readonly emailService: EmailService,
    private readonly configService: ConfigService,
  ) {}

  private ensureInternalToken(token: string | undefined) {
    const expected = this.configService.get<string>('INTERNAL_TOKEN');
    if (!expected || token !== expected) {
      throw new UnauthorizedException('Invalid internal token');
    }
  }

  @Post('test-all')
  async testAll(@Headers('x-internal-token') token: string, @Body() body?: TestEmailDto) {
    this.ensureInternalToken(token);
    const to = body?.to ?? TEST_EMAIL;
    const baseUrl = this.configService.get<string>('FRONTEND_URL') ?? 'https://mantrixflow.com';

    const results: Record<string, { id?: string; skipped?: boolean; error?: string }> = {};

    const run = async (
      name: string,
      fn: () => Promise<{ id?: string; skipped?: boolean }>,
    ) => {
      try {
        const r = await fn();
        results[name] = r;
        this.logger.log(`${name}: ${r.skipped ? 'skipped' : 'sent'}`);
      } catch (e) {
        results[name] = { error: e instanceof Error ? e.message : String(e) };
        this.logger.error(`${name}: ${results[name].error}`);
      }
    };

    // Pipeline Lifecycle
    await run('pipeline_run_failed', () =>
      this.emailService.sendPipelineRunFailed({
        recipientEmails: [to],
        pipelineName: 'Test Pipeline (orders → warehouse)',
        sourceStream: 'public.orders',
        destTable: 'warehouse.orders',
        errorMessage: 'Connection timeout: could not reach database after 30s',
        startedAt: '2025-03-08 10:00:00 UTC',
        failedAt: '2025-03-08 10:05:32 UTC',
        runDetailUrl: `${baseUrl}/workspace/data-pipelines/${FAKE_PIPELINE_ID}?run=test-run-id`,
        editPipelineUrl: `${baseUrl}/workspace/data-pipelines/${FAKE_PIPELINE_ID}/edit`,
        orgId: FAKE_ORG_ID,
        pipelineId: FAKE_PIPELINE_ID,
      }),
    );

    await run('pipeline_recovered', () =>
      this.emailService.sendPipelineRecovered({
        recipientEmails: [to],
        pipelineName: 'Test Pipeline (orders → warehouse)',
        rowsUpserted: 1250,
        durationSeconds: 45,
        runHistoryUrl: `${baseUrl}/workspace/data-pipelines/${FAKE_PIPELINE_ID}`,
        orgId: FAKE_ORG_ID,
        pipelineId: FAKE_PIPELINE_ID,
      }),
    );

    await run('pipeline_disabled', () =>
      this.emailService.sendPipelineDisabled({
        recipientEmails: [to],
        pipelineName: 'Test Pipeline (orders → warehouse)',
        failureCount: 5,
        lastErrorMessage: 'Connection timeout: could not reach database',
        editPipelineUrl: `${baseUrl}/workspace/data-pipelines/${FAKE_PIPELINE_ID}/edit`,
        supportUrl: 'https://mantrixflow.com/support',
        orgId: FAKE_ORG_ID,
        pipelineId: FAKE_PIPELINE_ID,
      }),
    );

    await run('first_success', () =>
      this.emailService.sendFirstSuccess({
        recipientEmail: to,
        pipelineName: 'Test Pipeline (orders → warehouse)',
        rowsUpserted: 1250,
        destTable: 'warehouse.orders',
        durationSeconds: 45,
        pipelineUrl: `${baseUrl}/workspace/data-pipelines/${FAKE_PIPELINE_ID}`,
        orgId: FAKE_ORG_ID,
        userId: FAKE_USER_ID,
        pipelineId: FAKE_PIPELINE_ID,
      }),
    );

    await run('log_based_initial_complete', () =>
      this.emailService.sendLogBasedInitialComplete({
        recipientEmail: to,
        pipelineName: 'Test CDC Pipeline',
        rowsUpserted: 50000,
        destTable: 'warehouse.orders',
        pipelineUrl: `${baseUrl}/workspace/data-pipelines/${FAKE_PIPELINE_ID}`,
        orgId: FAKE_ORG_ID,
        userId: FAKE_USER_ID,
        pipelineId: FAKE_PIPELINE_ID,
      }),
    );

    await run('pipeline_partial_success', () =>
      this.emailService.sendPipelinePartialSuccess({
        recipientEmail: to,
        pipelineName: 'Test Pipeline (large sync)',
        rowsUpserted: 45000,
        timeoutSeconds: 3600,
        runDetailUrl: `${baseUrl}/workspace/data-pipelines/${FAKE_PIPELINE_ID}?run=test-run-id`,
        orgId: FAKE_ORG_ID,
        userId: FAKE_USER_ID,
        pipelineId: FAKE_PIPELINE_ID,
      }),
    );

    // Connection Management
    await run('log_based_setup_complete', () =>
      this.emailService.sendLogBasedSetupComplete({
        recipientEmail: to,
        connectionName: 'Production PostgreSQL',
        createPipelineUrl: `${baseUrl}/workspace/data-pipelines/new`,
        orgId: FAKE_ORG_ID,
        userId: FAKE_USER_ID,
      }),
    );

    // Member Removed
    await run('member_removed', () =>
      this.emailService.sendMemberRemoved({
        recipientEmail: to,
        firstName: 'Vijay',
        orgName: 'Test Organization',
        dashboardUrl: `${baseUrl}/workspace`,
        userId: FAKE_USER_ID,
      }),
    );

    // Billing / Trial
    await run('trial_started', () =>
      this.emailService.sendTrialStarted({
        recipientEmail: to,
        firstName: 'Vijay',
        orgName: 'Test Organization',
        trialEndDate: '2025-03-22',
        pricingUrl: `${baseUrl}/pricing`,
        dashboardUrl: `${baseUrl}/workspace`,
        orgId: FAKE_ORG_ID,
        userId: FAKE_USER_ID,
      }),
    );

    await run('trial_ends_7_days', () =>
      this.emailService.sendTrialEnds7Days({
        recipientEmails: [to],
        orgName: 'Test Organization',
        trialEndDate: '2025-03-15',
        pipelineCount: 5,
        connectionCount: 3,
        rowsSyncedTotal: 125000,
        upgradeUrl: `${baseUrl}/pricing`,
        orgId: FAKE_ORG_ID,
      }),
    );

    await run('trial_ends_1_day', () =>
      this.emailService.sendTrialEnds1Day({
        recipientEmail: to,
        orgName: 'Test Organization',
        trialEndDate: '2025-03-09',
        upgradeUrl: `${baseUrl}/pricing`,
        orgId: FAKE_ORG_ID,
        userId: FAKE_USER_ID,
      }),
    );

    await run('trial_expired', () =>
      this.emailService.sendTrialExpired({
        recipientEmails: [to],
        orgName: 'Test Organization',
        pausedPipelineCount: 5,
        upgradeUrl: `${baseUrl}/pricing`,
        orgId: FAKE_ORG_ID,
      }),
    );

    await run('payment_failed', () =>
      this.emailService.sendPaymentFailed({
        recipientEmail: to,
        orgName: 'Test Organization',
        amount: '$29.00',
        retryDate: '2025-03-10',
        gracePeriodEndDate: '2025-03-15',
        billingUrl: `${baseUrl}/settings/billing`,
        orgId: FAKE_ORG_ID,
        userId: FAKE_USER_ID,
      }),
    );

    await run('weekly_digest', () =>
      this.emailService.sendWeeklyDigest({
        recipientEmails: [to],
        orgName: 'Test Organization',
        weekStartDate: '2025-03-03',
        totalRuns: 84,
        successRate: 96,
        failedRuns: 3,
        rowsSynced: 125000,
        topPipelineName: 'orders → warehouse',
        analyticsUrl: `${baseUrl}/workspace/analytics`,
        orgId: FAKE_ORG_ID,
      }),
    );

    // Engagement (Onboarding)
    await run('onboarding_day3_nudge', () =>
      this.emailService.sendOnboardingDay3Nudge({
        recipientEmail: to,
        firstName: 'Vijay',
        pipelineBuilderUrl: `${baseUrl}/workspace/data-pipelines/new`,
        quickstartUrl: `${baseUrl}/docs/quickstart`,
        userId: FAKE_USER_ID,
      }),
    );

    await run('onboarding_day7_nudge', () =>
      this.emailService.sendOnboardingDay7Nudge({
        recipientEmail: to,
        firstName: 'Vijay',
        demoVideoUrl: `${baseUrl}/docs/cdc-demo`,
        pipelineBuilderUrl: `${baseUrl}/workspace/data-pipelines/new`,
        userId: FAKE_USER_ID,
      }),
    );

    return {
      message: `Test emails triggered for ${to}`,
      results,
    };
  }
}
