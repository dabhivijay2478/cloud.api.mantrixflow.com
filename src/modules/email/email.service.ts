/**
 * Email Service
 * Wraps UnoSend REST API for transactional emails.
 * All email sending goes through this service.
 */

import { HttpService } from '@nestjs/axios';
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { EMAIL_TYPES, type EmailType } from './constants/email-types.constants';
import { EmailRepository } from './repositories/email-repository';

const UNOSEND_API_URL = 'https://www.unosend.co/api/v1/emails';

export interface SendEmailOptions {
  to: string[];
  templateId?: string;
  variables?: Record<string, string | number | boolean>;
  from?: string;
  subject?: string;
  html?: string;
  emailType: EmailType;
  orgId?: string;
  userId?: string;
  pipelineId?: string;
  connectionId?: string;
}

@Injectable()
export class EmailService {
  private readonly logger = new Logger(EmailService.name);
  private readonly apiKey: string | undefined;
  private readonly enabled: boolean;
  private readonly fromDefault: string;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly emailRepository: EmailRepository,
  ) {
    this.apiKey = this.configService.get<string>('UNOSEND_API_KEY');
    this.enabled = this.configService.get<string>('EMAIL_ENABLED', 'true') === 'true';
    this.fromDefault =
      this.configService.get<string>('UNOSEND_FROM_DEFAULT') ?? 'alerts@mantrixflow.com';
  }

  /** Logo URL for email templates - UnoSend CDN or UNOSEND_LOGO_URL env */
  private getLogoUrl(): string {
    return (
      this.configService.get<string>('UNOSEND_LOGO_URL') ??
      'https://www.unosend.co/cdn/53483ed3-39f0-4330-b156-27176282bdf4/1772965120678-em5heh.png'
    );
  }

  /**
   * Send a transactional email via UnoSend
   */
  async send(options: SendEmailOptions): Promise<{ id?: string; skipped?: boolean }> {
    if (!this.enabled) {
      this.logger.debug(`Email disabled (EMAIL_ENABLED=false), skipping ${options.emailType}`);
      return { skipped: true };
    }
    if (!this.apiKey) {
      this.logger.warn('UNOSEND_API_KEY not set, skipping email send');
      return { skipped: true };
    }
    const recipient = options.to[0];
    if (!recipient) {
      this.logger.warn('No recipient for email');
      return { skipped: true };
    }
    const suppressed = await this.emailRepository.isSuppressed(recipient);
    if (suppressed) {
      this.logger.debug(`Recipient ${recipient} is suppressed, skipping`);
      return { skipped: true };
    }
    const payload: Record<string, unknown> = {
      from: options.from ?? this.fromDefault,
      to: options.to,
      tracking: { open: true, click: true },
    };
    if (options.templateId && options.variables) {
      payload.template_id = options.templateId;
      payload.variables = { ...options.variables, logo_url: this.getLogoUrl() };
    } else if (options.html) {
      payload.subject = options.subject ?? 'MantrixFlow';
      payload.html = options.html;
    } else {
      this.logger.warn(`Email ${options.emailType}: need templateId+variables or html`);
      return { skipped: true };
    }
    try {
      const response = await firstValueFrom(
        this.httpService.post(UNOSEND_API_URL, payload, {
          headers: {
            Authorization: `Bearer ${this.apiKey}`,
            'Content-Type': 'application/json',
          },
        }),
      );
      const id = response.data?.id as string | undefined;
      await this.emailRepository.logSend({
        orgId: options.orgId ?? null,
        userId: options.userId ?? null,
        emailType: options.emailType,
        recipientEmail: recipient,
        pipelineId: options.pipelineId ?? null,
        connectionId: options.connectionId ?? null,
        unosendMessageId: id ?? null,
      });
      this.logger.log(`Sent ${options.emailType} to ${recipient}`);
      return { id };
    } catch (error: unknown) {
      this.logger.error(
        `Failed to send ${options.emailType}: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error;
    }
  }

  /**
   * Get template ID from env or use default placeholder
   */
  private getTemplateId(emailType: EmailType): string | undefined {
    const envKey = `UNOSEND_TEMPLATE_${emailType.toUpperCase().replace(/-/g, '_')}`;
    return this.configService.get<string>(envKey);
  }

  // ─── Pipeline Lifecycle ─────────────────────────────────────────────────────

  async sendPipelineRunFailed(params: {
    recipientEmails: string[];
    pipelineName: string;
    sourceStream: string;
    destTable: string;
    errorMessage: string;
    startedAt: string;
    failedAt: string;
    runDetailUrl: string;
    editPipelineUrl: string;
    orgId: string;
    pipelineId: string;
  }): Promise<{ id?: string; skipped?: boolean }> {
    const templateId = this.getTemplateId(EMAIL_TYPES.PIPELINE_RUN_FAILED);
    if (!templateId) {
      this.logger.debug('No template for pipeline_run_failed, skipping');
      return { skipped: true };
    }
    const variables = {
      pipeline_name: params.pipelineName,
      source_stream: params.sourceStream,
      dest_table: params.destTable,
      error_message: params.errorMessage,
      started_at: params.startedAt,
      failed_at: params.failedAt,
      run_detail_url: params.runDetailUrl,
      edit_pipeline_url: params.editPipelineUrl,
    };
    let lastResult: { id?: string; skipped?: boolean } = { skipped: true };
    for (const email of params.recipientEmails) {
      lastResult = await this.send({
        to: [email],
        templateId,
        variables,
        emailType: EMAIL_TYPES.PIPELINE_RUN_FAILED,
        orgId: params.orgId,
        pipelineId: params.pipelineId,
      });
    }
    return lastResult;
  }

  async sendPipelineRecovered(params: {
    recipientEmails: string[];
    pipelineName: string;
    rowsUpserted: number;
    durationSeconds: number;
    runHistoryUrl: string;
    orgId: string;
    pipelineId: string;
  }): Promise<{ id?: string; skipped?: boolean }> {
    const templateId = this.getTemplateId(EMAIL_TYPES.PIPELINE_RECOVERED);
    if (!templateId) return { skipped: true };
    const variables = {
      pipeline_name: params.pipelineName,
      rows_upserted: params.rowsUpserted,
      duration_seconds: params.durationSeconds,
      run_history_url: params.runHistoryUrl,
    };
    let lastResult: { id?: string; skipped?: boolean } = { skipped: true };
    for (const email of params.recipientEmails) {
      lastResult = await this.send({
        to: [email],
        templateId,
        variables,
        emailType: EMAIL_TYPES.PIPELINE_RECOVERED,
        orgId: params.orgId,
        pipelineId: params.pipelineId,
      });
    }
    return lastResult;
  }

  async sendPipelineDisabled(params: {
    recipientEmails: string[];
    pipelineName: string;
    failureCount: number;
    lastErrorMessage: string;
    editPipelineUrl: string;
    supportUrl: string;
    orgId: string;
    pipelineId: string;
  }): Promise<{ id?: string; skipped?: boolean }> {
    const templateId = this.getTemplateId(EMAIL_TYPES.PIPELINE_DISABLED);
    if (!templateId) return { skipped: true };
    const variables = {
      pipeline_name: params.pipelineName,
      failure_count: params.failureCount,
      last_error_message: params.lastErrorMessage,
      edit_pipeline_url: params.editPipelineUrl,
      support_url: params.supportUrl,
    };
    let lastResult: { id?: string; skipped?: boolean } = { skipped: true };
    for (const email of params.recipientEmails) {
      lastResult = await this.send({
        to: [email],
        templateId,
        variables,
        emailType: EMAIL_TYPES.PIPELINE_DISABLED,
        orgId: params.orgId,
        pipelineId: params.pipelineId,
      });
    }
    return lastResult;
  }

  async sendFirstSuccess(params: {
    recipientEmail: string;
    pipelineName: string;
    rowsUpserted: number;
    destTable: string;
    durationSeconds: number;
    pipelineUrl: string;
    orgId: string;
    userId: string;
    pipelineId: string;
  }): Promise<{ id?: string; skipped?: boolean }> {
    const templateId = this.getTemplateId(EMAIL_TYPES.FIRST_SUCCESS);
    if (!templateId) return { skipped: true };
    return this.send({
      to: [params.recipientEmail],
      templateId,
      variables: {
        pipeline_name: params.pipelineName,
        rows_upserted: params.rowsUpserted,
        dest_table: params.destTable,
        duration_seconds: params.durationSeconds,
        pipeline_url: params.pipelineUrl,
      },
      emailType: EMAIL_TYPES.FIRST_SUCCESS,
      orgId: params.orgId,
      userId: params.userId,
      pipelineId: params.pipelineId,
    });
  }

  async sendLogBasedInitialComplete(params: {
    recipientEmail: string;
    pipelineName: string;
    rowsUpserted: number;
    destTable: string;
    pipelineUrl: string;
    orgId: string;
    userId: string;
    pipelineId: string;
  }): Promise<{ id?: string; skipped?: boolean }> {
    const templateId = this.getTemplateId(EMAIL_TYPES.LOG_BASED_INITIAL_COMPLETE);
    if (!templateId) return { skipped: true };
    return this.send({
      to: [params.recipientEmail],
      templateId,
      variables: {
        pipeline_name: params.pipelineName,
        rows_upserted: params.rowsUpserted,
        dest_table: params.destTable,
        pipeline_url: params.pipelineUrl,
      },
      emailType: EMAIL_TYPES.LOG_BASED_INITIAL_COMPLETE,
      orgId: params.orgId,
      userId: params.userId,
      pipelineId: params.pipelineId,
    });
  }

  async sendPipelinePartialSuccess(params: {
    recipientEmail: string;
    pipelineName: string;
    rowsUpserted: number;
    timeoutSeconds: number;
    runDetailUrl: string;
    orgId: string;
    userId: string;
    pipelineId: string;
  }): Promise<{ id?: string; skipped?: boolean }> {
    const templateId = this.getTemplateId(EMAIL_TYPES.PIPELINE_PARTIAL_SUCCESS);
    if (!templateId) return { skipped: true };
    return this.send({
      to: [params.recipientEmail],
      templateId,
      variables: {
        pipeline_name: params.pipelineName,
        rows_upserted: params.rowsUpserted,
        timeout_seconds: params.timeoutSeconds,
        run_detail_url: params.runDetailUrl,
      },
      emailType: EMAIL_TYPES.PIPELINE_PARTIAL_SUCCESS,
      orgId: params.orgId,
      userId: params.userId,
      pipelineId: params.pipelineId,
    });
  }

  // ─── Connection Management ─────────────────────────────────────────────────

  async sendLogBasedSetupComplete(params: {
    recipientEmail: string;
    connectionName: string;
    createPipelineUrl: string;
    orgId: string;
    userId: string;
  }): Promise<{ id?: string; skipped?: boolean }> {
    const templateId = this.getTemplateId(EMAIL_TYPES.LOG_BASED_SETUP_COMPLETE);
    if (!templateId) return { skipped: true };
    return this.send({
      to: [params.recipientEmail],
      templateId,
      variables: {
        connection_name: params.connectionName,
        create_pipeline_url: params.createPipelineUrl,
      },
      emailType: EMAIL_TYPES.LOG_BASED_SETUP_COMPLETE,
      orgId: params.orgId,
      userId: params.userId,
    });
  }

  // ─── Member Removed ─────────────────────────────────────────────────────────

  async sendMemberRemoved(params: {
    recipientEmail: string;
    firstName: string | null;
    orgName: string;
    dashboardUrl: string;
    userId: string;
  }): Promise<{ id?: string; skipped?: boolean }> {
    const templateId = this.getTemplateId(EMAIL_TYPES.MEMBER_REMOVED);
    if (!templateId) return { skipped: true };
    return this.send({
      to: [params.recipientEmail],
      templateId,
      variables: {
        first_name: params.firstName ?? 'there',
        org_name: params.orgName,
        dashboard_url: params.dashboardUrl,
      },
      emailType: EMAIL_TYPES.MEMBER_REMOVED,
      userId: params.userId,
    });
  }

  // ─── Billing (stubs for Phase 3) ───────────────────────────────────────────

  async sendTrialStarted(params: {
    recipientEmail: string;
    firstName: string | null;
    orgName: string;
    trialEndDate: string;
    pricingUrl: string;
    dashboardUrl: string;
    orgId: string;
    userId: string;
  }): Promise<{ id?: string; skipped?: boolean }> {
    const templateId = this.getTemplateId(EMAIL_TYPES.TRIAL_STARTED);
    if (!templateId) return { skipped: true };
    return this.send({
      to: [params.recipientEmail],
      templateId,
      variables: {
        first_name: params.firstName ?? 'there',
        org_name: params.orgName,
        trial_end_date: params.trialEndDate,
        pricing_url: params.pricingUrl,
        dashboard_url: params.dashboardUrl,
      },
      emailType: EMAIL_TYPES.TRIAL_STARTED,
      orgId: params.orgId,
      userId: params.userId,
    });
  }

  async sendTrialExpired(params: {
    recipientEmails: string[];
    orgName: string;
    pausedPipelineCount: number;
    upgradeUrl: string;
    orgId: string;
  }): Promise<{ id?: string; skipped?: boolean }> {
    const templateId = this.getTemplateId(EMAIL_TYPES.TRIAL_EXPIRED);
    if (!templateId) return { skipped: true };
    const variables = {
      org_name: params.orgName,
      paused_pipeline_count: params.pausedPipelineCount,
      upgrade_url: params.upgradeUrl,
    };
    let lastResult: { id?: string; skipped?: boolean } = { skipped: true };
    for (const email of params.recipientEmails) {
      lastResult = await this.send({
        to: [email],
        templateId,
        variables,
        emailType: EMAIL_TYPES.TRIAL_EXPIRED,
        orgId: params.orgId,
      });
    }
    return lastResult;
  }

  async sendTrialEnds7Days(params: {
    recipientEmails: string[];
    orgName: string;
    trialEndDate: string;
    pipelineCount: number;
    connectionCount: number;
    rowsSyncedTotal: number;
    upgradeUrl: string;
    orgId: string;
  }): Promise<{ id?: string; skipped?: boolean }> {
    const templateId = this.getTemplateId(EMAIL_TYPES.TRIAL_ENDS_7_DAYS);
    if (!templateId) return { skipped: true };
    const variables = {
      org_name: params.orgName,
      trial_end_date: params.trialEndDate,
      pipeline_count: params.pipelineCount,
      connection_count: params.connectionCount,
      rows_synced_total: params.rowsSyncedTotal,
      upgrade_url: params.upgradeUrl,
    };
    let lastResult: { id?: string; skipped?: boolean } = { skipped: true };
    for (const email of params.recipientEmails) {
      lastResult = await this.send({
        to: [email],
        templateId,
        variables,
        emailType: EMAIL_TYPES.TRIAL_ENDS_7_DAYS,
        orgId: params.orgId,
      });
    }
    return lastResult;
  }

  async sendTrialEnds1Day(params: {
    recipientEmail: string;
    orgName: string;
    trialEndDate: string;
    upgradeUrl: string;
    orgId: string;
    userId: string;
  }): Promise<{ id?: string; skipped?: boolean }> {
    const templateId = this.getTemplateId(EMAIL_TYPES.TRIAL_ENDS_1_DAY);
    if (!templateId) return { skipped: true };
    return this.send({
      to: [params.recipientEmail],
      templateId,
      variables: {
        org_name: params.orgName,
        trial_end_date: params.trialEndDate,
        upgrade_url: params.upgradeUrl,
      },
      emailType: EMAIL_TYPES.TRIAL_ENDS_1_DAY,
      orgId: params.orgId,
      userId: params.userId,
    });
  }

  async sendWeeklyDigest(params: {
    recipientEmails: string[];
    orgName: string;
    weekStartDate: string;
    totalRuns: number;
    successRate: number;
    failedRuns: number;
    rowsSynced: number;
    topPipelineName: string;
    analyticsUrl: string;
    orgId: string;
  }): Promise<{ id?: string; skipped?: boolean }> {
    const templateId = this.getTemplateId(EMAIL_TYPES.WEEKLY_DIGEST);
    if (!templateId) return { skipped: true };
    const variables = {
      org_name: params.orgName,
      week_start_date: params.weekStartDate,
      total_runs: params.totalRuns,
      success_rate: params.successRate,
      failed_runs: params.failedRuns,
      rows_synced: params.rowsSynced,
      top_pipeline_name: params.topPipelineName,
      analytics_url: params.analyticsUrl,
    };
    let lastResult: { id?: string; skipped?: boolean } = { skipped: true };
    for (const email of params.recipientEmails) {
      lastResult = await this.send({
        to: [email],
        templateId,
        variables,
        emailType: EMAIL_TYPES.WEEKLY_DIGEST,
        orgId: params.orgId,
      });
    }
    return lastResult;
  }

  async sendPaymentFailed(params: {
    recipientEmail: string;
    orgName: string;
    amount: string;
    retryDate: string;
    gracePeriodEndDate: string;
    billingUrl: string;
    orgId: string;
    userId: string;
  }): Promise<{ id?: string; skipped?: boolean }> {
    const templateId = this.getTemplateId(EMAIL_TYPES.PAYMENT_FAILED);
    if (!templateId) return { skipped: true };
    return this.send({
      to: [params.recipientEmail],
      templateId,
      variables: {
        org_name: params.orgName,
        amount: params.amount,
        retry_date: params.retryDate,
        grace_period_end_date: params.gracePeriodEndDate,
        billing_url: params.billingUrl,
      },
      emailType: EMAIL_TYPES.PAYMENT_FAILED,
      orgId: params.orgId,
      userId: params.userId,
    });
  }
}
