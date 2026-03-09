/**
 * Email Service
 * Wraps UnoSend REST API for transactional emails.
 * All email sending goes through this service.
 *
 * LOCAL RENDERING (default — EMAIL_USE_LOCAL_TEMPLATES=true):
 *   Templates are rendered locally from src/modules/email/templates/*.html
 *   and sent as raw HTML + subject. This bypasses UnoSend's template variable
 *   substitution entirely, guaranteeing variables resolve and subjects are
 *   always set — regardless of UnoSend dashboard configuration.
 *
 * UNOSEND TEMPLATE PATH (EMAIL_USE_LOCAL_TEMPLATES=false):
 *   Sends template_id + template_data + subject to UnoSend. Requires
 *   UNOSEND_TEMPLATE_<TYPE> env vars AND matching variable names in the
 *   UnoSend dashboard template.
 *
 * ROOT CAUSE FIXES APPLIED:
 *   1. send() no longer requires templateId — local templates work without it.
 *   2. All public send*() methods no longer early-return when templateId is
 *      absent; send() owns all routing/skip logic.
 *   3. getTemplatePath() checks multiple candidate paths so it works in dev,
 *      Docker (dist/), Vercel (src/ via @vercel/node), and ts-node.
 *   4. subject is ALWAYS set in the payload — emails never show "(no subject)".
 *   5. Variables are sent as `template_data` per UnoSend API docs (NOT `variables`
 *      — that field is not recognised by UnoSend and causes substitution to silently fail).
 *   6. onModuleInit() pre-scans every registered local template at boot and logs
 *      found/missing so path or build issues surface immediately in server logs.
 *   7. A debug log of payload keys is emitted before every UnoSend HTTP request
 *      for easy observability without exposing sensitive content.
 */

import * as fs from 'fs';
import * as path from 'path';

import { HttpService } from '@nestjs/axios';
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import {
  EMAIL_TYPES,
  EMAIL_TYPE_TO_TEMPLATE,
  type EmailType,
} from './constants/email-types.constants';
import { EmailRepository } from './repositories/email-repository';

const UNOSEND_API_URL = 'https://www.unosend.co/api/v1/emails';

/** Email types that are always sent (critical transactional) — no preference check */
const CRITICAL_EMAIL_TYPES: readonly EmailType[] = [
  EMAIL_TYPES.FIRST_SUCCESS,
  EMAIL_TYPES.LOG_BASED_INITIAL_COMPLETE,
  EMAIL_TYPES.LOG_BASED_SETUP_COMPLETE,
  EMAIL_TYPES.MEMBER_REMOVED,
  EMAIL_TYPES.PAYMENT_FAILED,
  EMAIL_TYPES.CONNECTION_TEST_FAILED,
  EMAIL_TYPES.CONNECTION_RESTORED,
] as const;

/** Email type → preference key for preference-gated emails */
const EMAIL_TYPE_TO_PREFERENCE: Partial<Record<EmailType, 'weeklyDigestEnabled' | 'pipelineFailureEmails' | 'marketingEmails'>> = {
  [EMAIL_TYPES.WEEKLY_DIGEST]: 'weeklyDigestEnabled',
  [EMAIL_TYPES.PIPELINE_RUN_FAILED]: 'pipelineFailureEmails',
  [EMAIL_TYPES.PIPELINE_DISABLED]: 'pipelineFailureEmails',
  [EMAIL_TYPES.PIPELINE_RECOVERED]: 'pipelineFailureEmails',
  [EMAIL_TYPES.PIPELINE_PARTIAL_SUCCESS]: 'pipelineFailureEmails',
  [EMAIL_TYPES.TRIAL_STARTED]: 'marketingEmails',
  [EMAIL_TYPES.TRIAL_ENDS_7_DAYS]: 'marketingEmails',
  [EMAIL_TYPES.TRIAL_ENDS_1_DAY]: 'marketingEmails',
  [EMAIL_TYPES.TRIAL_EXPIRED]: 'marketingEmails',
  [EMAIL_TYPES.ONBOARDING_DAY3_NUDGE]: 'marketingEmails',
  [EMAIL_TYPES.ONBOARDING_DAY7_NUDGE]: 'marketingEmails',
};

export interface SendEmailOptions {
  to: string[];
  /**
   * UnoSend template ID.
   * Required when EMAIL_USE_LOCAL_TEMPLATES=false.
   * Used as fallback when local template is not found.
   * Optional when EMAIL_USE_LOCAL_TEMPLATES=true and a local template exists.
   */
  templateId?: string;
  variables?: Record<string, string | number | boolean>;
  from?: string;
  /** Override subject — auto-generated from emailType when omitted. */
  subject?: string;
  /** Raw pre-rendered HTML — bypasses both local and UnoSend templates. */
  html?: string;
  emailType: EmailType;
  orgId?: string;
  userId?: string;
  pipelineId?: string;
  connectionId?: string;
}

@Injectable()
export class EmailService implements OnModuleInit {
  private readonly logger = new Logger(EmailService.name);
  private readonly apiKey: string | undefined;
  private readonly enabled: boolean;
  private readonly fromDefault: string;
  private readonly useLocalTemplates: boolean;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly emailRepository: EmailRepository,
  ) {
    this.apiKey = this.configService.get<string>('UNOSEND_API_KEY');
    this.enabled = this.configService.get<string>('EMAIL_ENABLED', 'true') === 'true';
    this.fromDefault =
      this.configService.get<string>('UNOSEND_FROM_DEFAULT') ?? 'alerts@mantrixflow.com';
    this.useLocalTemplates =
      this.configService.get<string>('EMAIL_USE_LOCAL_TEMPLATES', 'true') === 'true';

    this.logger.log(
      `EmailService init — enabled=${this.enabled}, useLocalTemplates=${this.useLocalTemplates}, from=${this.fromDefault}`,
    );
  }

  // ─── Lifecycle ───────────────────────────────────────────────────────────────

  /**
   * Pre-scan all registered local template files at startup.
   * Logs found / missing at boot so path or build issues are immediately visible
   * in server logs instead of silently skipping emails at runtime.
   */
  onModuleInit(): void {
    if (!this.useLocalTemplates) {
      this.logger.log('EmailService: useLocalTemplates=false — skipping local template pre-scan');
      return;
    }

    const emailTypes = Object.keys(EMAIL_TYPE_TO_TEMPLATE) as EmailType[];
    let found = 0;
    let missing = 0;

    for (const emailType of emailTypes) {
      const resolved = this.getTemplatePath(emailType);
      if (resolved) {
        found++;
        this.logger.debug(`Template OK: ${emailType} → ${resolved}`);
      } else {
        missing++;
        this.logger.warn(
          `Template MISSING: "${emailType}" — this email type will be skipped until the ` +
            `template file is present at one of the expected paths. ` +
            `Run \`nest build\` to ensure assets are copied to dist/.`,
        );
      }
    }

    this.logger.log(
      `EmailService template scan: ${found}/${emailTypes.length} found` +
        (missing > 0 ? `, ${missing} MISSING — check logs above` : ' ✓ all OK'),
    );
  }

  // ─── Private Helpers ────────────────────────────────────────────────────────

  /** Logo URL injected into every template as {{logo_url}} */
  private getLogoUrl(): string {
    return (
      this.configService.get<string>('UNOSEND_LOGO_URL') ??
      'https://www.unosend.co/cdn/53483ed3-39f0-4330-b156-27176282bdf4/1772965120678-em5heh.png'
    );
  }

  /**
   * Derive a meaningful subject line from the email type + variables.
   * Called for every send so emails NEVER land with "(no subject)".
   */
  private getSubjectForEmailType(
    emailType: EmailType,
    variables?: Record<string, string | number | boolean>,
  ): string {
    const name = String(variables?.pipeline_name ?? 'Pipeline');
    const org = String(variables?.org_name ?? 'your organization');

    const subj: Record<string, string> = {
      [EMAIL_TYPES.PIPELINE_RUN_FAILED]: `Pipeline Failed: ${name}`,
      [EMAIL_TYPES.PIPELINE_RECOVERED]: `Pipeline Recovered: ${name}`,
      [EMAIL_TYPES.PIPELINE_DISABLED]: `Pipeline Paused: ${name}`,
      [EMAIL_TYPES.FIRST_SUCCESS]: `🎉 First Pipeline Success — MantrixFlow`,
      [EMAIL_TYPES.LOG_BASED_INITIAL_COMPLETE]: `Log-Based Sync Ready — MantrixFlow`,
      [EMAIL_TYPES.PIPELINE_PARTIAL_SUCCESS]: `Pipeline Ran Partially: ${name}`,
      [EMAIL_TYPES.LOG_BASED_SETUP_COMPLETE]: `Log-Based Setup Complete — MantrixFlow`,
      [EMAIL_TYPES.MEMBER_REMOVED]: `You've Been Removed from ${org} — MantrixFlow`,
      [EMAIL_TYPES.TRIAL_STARTED]: `Your MantrixFlow Trial Has Started`,
      [EMAIL_TYPES.TRIAL_ENDS_7_DAYS]: `Your Trial Ends in 7 Days — MantrixFlow`,
      [EMAIL_TYPES.TRIAL_ENDS_1_DAY]: `Trial Ends Tomorrow — Act Now`,
      [EMAIL_TYPES.TRIAL_EXPIRED]: `Your MantrixFlow Trial Has Expired`,
      [EMAIL_TYPES.PAYMENT_FAILED]: `Action Required: Payment Failed — MantrixFlow`,
      [EMAIL_TYPES.WEEKLY_DIGEST]: `Your Weekly Pipeline Digest — MantrixFlow`,
      [EMAIL_TYPES.ONBOARDING_DAY3_NUDGE]: `Set Up Your First Pipeline — MantrixFlow`,
      [EMAIL_TYPES.ONBOARDING_DAY7_NUDGE]: `Discover Log-Based CDC — MantrixFlow`,
    };

    return subj[emailType] ?? 'MantrixFlow';
  }

  /** Coerce all variable values to strings for safe HTML injection and UnoSend API. */
  private stringifyVariables(
    vars: Record<string, string | number | boolean>,
  ): Record<string, string> {
    const out: Record<string, string> = {};
    for (const [k, v] of Object.entries(vars)) {
      out[k] = v === null || v === undefined ? '' : String(v);
    }
    return out;
  }

  /**
   * Replace {{var}} placeholders with actual values.
   * Intentionally simple — no Handlebars/Mustache dependency.
   * Unknown keys are replaced with an empty string, never left as raw {{var}}.
   */
  private renderTemplate(html: string, vars: Record<string, string | number | boolean>): string {
    const stringVars = this.stringifyVariables(vars);
    return html.replace(/\{\{(\w+)\}\}/g, (_, key: string) => stringVars[key] ?? '');
  }

  /**
   * Resolve the filesystem path to a local template file.
   *
   * Checks multiple candidate paths so the service works correctly in:
   *   • `nest start:dev` / ts-node   — src/ tree via process.cwd()
   *   • `nest build` + start:prod    — dist/ next to compiled .js via __dirname
   *   • Docker (Dockerfile)          — /app/dist/modules/email/templates/
   *   • Vercel (@vercel/node)        — source tree mounted at /var/task/src/
   *   • Any other CWD-based layout   — dist/ under process.cwd()
   */
  private getTemplatePath(emailType: EmailType): string | null {
    const filename = EMAIL_TYPE_TO_TEMPLATE[emailType];
    if (!filename) return null;

    const candidates: string[] = [
      // 1. Compiled output — __dirname is dist/modules/email/ after nest build
      path.join(__dirname, 'templates', filename),

      // 2. Source tree — __dirname is src/modules/email/ in ts-node / Vercel
      path.join(__dirname, '..', 'email', 'templates', filename),

      // 3. Source tree relative to CWD — works in `nest start:dev`
      path.join(process.cwd(), 'src', 'modules', 'email', 'templates', filename),

      // 4. Dist tree relative to CWD — fallback for start:prod
      path.join(process.cwd(), 'dist', 'modules', 'email', 'templates', filename),

      // 5. Vercel /var/task layout
      path.join('/var/task', 'src', 'modules', 'email', 'templates', filename),
    ];

    for (const candidate of candidates) {
      if (fs.existsSync(candidate)) {
        this.logger.debug(`Template resolved: ${candidate}`);
        return candidate;
      }
    }

    this.logger.warn(
      `Local template not found for "${emailType}" (tried ${candidates.length} paths). ` +
        `Candidates: ${candidates.join(', ')}`,
    );
    return null;
  }

  /**
   * Read a local template HTML file by email type.
   * Returns null if the file cannot be found or read — callers fall back to
   * the UnoSend template_id path.
   */
  private readTemplateSync(emailType: EmailType): string | null {
    const templatePath = this.getTemplatePath(emailType);
    if (!templatePath) return null;
    try {
      return fs.readFileSync(templatePath, 'utf-8');
    } catch (err) {
      this.logger.warn(
        `Failed to read template "${emailType}" at "${templatePath}": ${err instanceof Error ? err.message : String(err)}`,
      );
      return null;
    }
  }

  // ─── Core send() ────────────────────────────────────────────────────────────

  /**
   * Send a transactional email via UnoSend.
   *
   * Routing priority when `variables` are provided:
   *   1. EMAIL_USE_LOCAL_TEMPLATES=true  → render HTML locally → send as html + subject
   *   2. Local template not found        → fall back to UnoSend template_id + variables + subject
   *   3. EMAIL_USE_LOCAL_TEMPLATES=false → send as UnoSend template_id + variables + subject
   *   4. Caller provided options.html    → send raw HTML + subject directly
   *
   * `templateId` is OPTIONAL when useLocalTemplates=true and a local template
   * exists. It is only required when falling back to the UnoSend template path.
   *
   * `subject` is ALWAYS included in the payload — never omitted.
   */
  async send(options: SendEmailOptions): Promise<{ id?: string; skipped?: boolean }> {
    // ── Guards ────────────────────────────────────────────────────────────────
    if (!this.enabled) {
      this.logger.debug(`Email disabled (EMAIL_ENABLED=false) — skipping ${options.emailType}`);
      return { skipped: true };
    }
    if (!this.apiKey) {
      this.logger.warn('UNOSEND_API_KEY not set — skipping email send');
      return { skipped: true };
    }

    const recipient = options.to[0];
    if (!recipient) {
      this.logger.warn(`No recipient address for ${options.emailType}`);
      return { skipped: true };
    }

    const suppressed = await this.emailRepository.isSuppressed(recipient);
    if (suppressed) {
      this.logger.debug(`Recipient ${recipient} is suppressed — skipping ${options.emailType}`);
      return { skipped: true };
    }

    const prefKey = EMAIL_TYPE_TO_PREFERENCE[options.emailType];
    if (prefKey && !CRITICAL_EMAIL_TYPES.includes(options.emailType)) {
      const prefs = await this.emailRepository.getPreferencesByEmail(recipient);
      if (!prefs[prefKey]) {
        this.logger.debug(
          `Recipient ${recipient} has opted out of ${prefKey} — skipping ${options.emailType}`,
        );
        return { skipped: true };
      }
    }

    // ── Build base payload ────────────────────────────────────────────────────
    const payload: Record<string, unknown> = {
      from: options.from ?? this.fromDefault,
      to: options.to,
      tracking: { open: true, click: true },
    };

    // Subject is derived once and always included — fixes "(no subject)" bug.
    const subject =
      options.subject ?? this.getSubjectForEmailType(options.emailType, options.variables);

    // ── Route to the right sending strategy ──────────────────────────────────
    if (options.variables) {
      // Inject logo so every template can use {{logo_url}}
      const variablesWithLogo: Record<string, string | number | boolean> = {
        ...options.variables,
        logo_url: this.getLogoUrl(),
      };

      if (this.useLocalTemplates) {
        // Strategy 1 — local HTML rendering (most reliable, default)
        const rawHtml = this.readTemplateSync(options.emailType);

        if (rawHtml) {
          payload.subject = subject;
          payload.html = this.renderTemplate(rawHtml, variablesWithLogo);
          this.logger.debug(`${options.emailType}: using local template rendering`);
        } else if (options.templateId) {
          // Strategy 2 — local template missing, fall back to UnoSend template
          this.logger.warn(
            `${options.emailType}: local template not found — falling back to UnoSend template_id "${options.templateId}"`,
          );
          payload.template_id = options.templateId;
          payload.subject = subject;
          // UnoSend API field for template variable substitution is `template_data`
          // (NOT `variables` — that field is undocumented and silently ignored).
          payload.template_data = this.stringifyVariables(variablesWithLogo);
        } else {
          // No local template AND no templateId — nothing to send
          this.logger.warn(
            `${options.emailType}: no local template and no templateId configured — skipping. ` +
              `Set UNOSEND_TEMPLATE_${options.emailType.toUpperCase().replace(/-/g, '_')} or ensure the template file exists.`,
          );
          return { skipped: true };
        }
      } else {
        // Strategy 3 — UnoSend template path (EMAIL_USE_LOCAL_TEMPLATES=false)
        if (!options.templateId) {
          this.logger.warn(
            `${options.emailType}: EMAIL_USE_LOCAL_TEMPLATES=false but no templateId — skipping. ` +
              `Set UNOSEND_TEMPLATE_${options.emailType.toUpperCase().replace(/-/g, '_')}.`,
          );
          return { skipped: true };
        }
        payload.template_id = options.templateId;
        payload.subject = subject;
        // UnoSend API field for template variable substitution is `template_data`
        // (NOT `variables` — that field is undocumented and silently ignored).
        payload.template_data = this.stringifyVariables(variablesWithLogo);
      }
    } else if (options.html) {
      // Strategy 4 — caller-provided raw HTML
      payload.subject = subject;
      payload.html = options.html;
    } else {
      this.logger.warn(
        `${options.emailType}: send() requires either variables (for templates) or html — skipping`,
      );
      return { skipped: true };
    }

    // ── Debug: emit payload summary before sending (keys only — no sensitive values) ──
    this.logger.debug(
      `[${options.emailType}] → UnoSend payload keys: [${Object.keys(payload).join(', ')}] | ` +
        `subject="${(payload.subject as string) ?? '(none)'}" | ` +
        `strategy=${payload.html ? 'local-html' : payload.template_id ? 'unosend-template' : 'unknown'}`,
    );

    // ── Fire the request ──────────────────────────────────────────────────────
    try {
      const response = await firstValueFrom(
        this.httpService.post(UNOSEND_API_URL, payload, {
          headers: {
            Authorization: `Bearer ${this.apiKey}`,
            'Content-Type': 'application/json',
          },
        }),
      );

      const responseData = response.data as { id?: string } | null | undefined;
      const id: string | undefined = responseData?.id;

      try {
        await this.emailRepository.logSend({
          orgId: options.orgId ?? null,
          userId: options.userId ?? null,
          emailType: options.emailType,
          recipientEmail: recipient,
          pipelineId: options.pipelineId ?? null,
          connectionId: options.connectionId ?? null,
          unosendMessageId: id ?? null,
        });
      } catch (logErr) {
        this.logger.warn(
          `Could not log send for ${options.emailType}: ${logErr instanceof Error ? logErr.message : String(logErr)}`,
        );
      }

      this.logger.log(
        `Sent ${options.emailType} → ${recipient} | subject: "${payload.subject as string}" | id: ${id ?? 'n/a'}`,
      );

      return { id };
    } catch (error: unknown) {
      const msg = error instanceof Error ? error.message : String(error);
      this.logger.error(`Failed to send ${options.emailType} → ${recipient}: ${msg}`);
      throw error;
    }
  }

  // ─── Template ID helper ──────────────────────────────────────────────────────

  /**
   * Read a UnoSend template ID from env.
   * Returns undefined when not configured — callers must not gate on this
   * when EMAIL_USE_LOCAL_TEMPLATES=true; send() handles the missing templateId.
   */
  private getTemplateId(emailType: EmailType): string | undefined {
    const envKey = `UNOSEND_TEMPLATE_${emailType.toUpperCase().replace(/-/g, '_')}`;
    return this.configService.get<string>(envKey);
  }

  // ─── Pipeline Lifecycle ──────────────────────────────────────────────────────

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
        templateId: this.getTemplateId(EMAIL_TYPES.PIPELINE_RUN_FAILED),
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
        templateId: this.getTemplateId(EMAIL_TYPES.PIPELINE_RECOVERED),
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
        templateId: this.getTemplateId(EMAIL_TYPES.PIPELINE_DISABLED),
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
    return this.send({
      to: [params.recipientEmail],
      templateId: this.getTemplateId(EMAIL_TYPES.FIRST_SUCCESS),
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
    return this.send({
      to: [params.recipientEmail],
      templateId: this.getTemplateId(EMAIL_TYPES.LOG_BASED_INITIAL_COMPLETE),
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
    return this.send({
      to: [params.recipientEmail],
      templateId: this.getTemplateId(EMAIL_TYPES.PIPELINE_PARTIAL_SUCCESS),
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

  // ─── Connection Management ────────────────────────────────────────────────

  async sendLogBasedSetupComplete(params: {
    recipientEmail: string;
    connectionName: string;
    createPipelineUrl: string;
    orgId: string;
    userId: string;
  }): Promise<{ id?: string; skipped?: boolean }> {
    return this.send({
      to: [params.recipientEmail],
      templateId: this.getTemplateId(EMAIL_TYPES.LOG_BASED_SETUP_COMPLETE),
      variables: {
        connection_name: params.connectionName,
        create_pipeline_url: params.createPipelineUrl,
      },
      emailType: EMAIL_TYPES.LOG_BASED_SETUP_COMPLETE,
      orgId: params.orgId,
      userId: params.userId,
    });
  }

  // ─── Member Management ────────────────────────────────────────────────────

  async sendMemberRemoved(params: {
    recipientEmail: string;
    firstName: string | null;
    orgName: string;
    dashboardUrl: string;
    userId: string;
  }): Promise<{ id?: string; skipped?: boolean }> {
    return this.send({
      to: [params.recipientEmail],
      templateId: this.getTemplateId(EMAIL_TYPES.MEMBER_REMOVED),
      variables: {
        first_name: params.firstName ?? 'there',
        org_name: params.orgName,
        dashboard_url: params.dashboardUrl,
      },
      emailType: EMAIL_TYPES.MEMBER_REMOVED,
      userId: params.userId,
    });
  }

  // ─── Billing / Trial ─────────────────────────────────────────────────────

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
    return this.send({
      to: [params.recipientEmail],
      templateId: this.getTemplateId(EMAIL_TYPES.TRIAL_STARTED),
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
    const variables = {
      org_name: params.orgName,
      paused_pipeline_count: params.pausedPipelineCount,
      upgrade_url: params.upgradeUrl,
    };
    let lastResult: { id?: string; skipped?: boolean } = { skipped: true };
    for (const email of params.recipientEmails) {
      lastResult = await this.send({
        to: [email],
        templateId: this.getTemplateId(EMAIL_TYPES.TRIAL_EXPIRED),
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
        templateId: this.getTemplateId(EMAIL_TYPES.TRIAL_ENDS_7_DAYS),
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
    return this.send({
      to: [params.recipientEmail],
      templateId: this.getTemplateId(EMAIL_TYPES.TRIAL_ENDS_1_DAY),
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
    return this.send({
      to: [params.recipientEmail],
      templateId: this.getTemplateId(EMAIL_TYPES.PAYMENT_FAILED),
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

  // ─── Engagement (Weekly / Onboarding) ────────────────────────────────────

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
        templateId: this.getTemplateId(EMAIL_TYPES.WEEKLY_DIGEST),
        variables,
        emailType: EMAIL_TYPES.WEEKLY_DIGEST,
        orgId: params.orgId,
      });
    }
    return lastResult;
  }

  async sendOnboardingDay3Nudge(params: {
    recipientEmail: string;
    firstName: string | null;
    pipelineBuilderUrl: string;
    quickstartUrl: string;
    userId: string;
  }): Promise<{ id?: string; skipped?: boolean }> {
    return this.send({
      to: [params.recipientEmail],
      templateId: this.getTemplateId(EMAIL_TYPES.ONBOARDING_DAY3_NUDGE),
      variables: {
        first_name: params.firstName ?? 'there',
        pipeline_builder_url: params.pipelineBuilderUrl,
        quickstart_url: params.quickstartUrl,
      },
      emailType: EMAIL_TYPES.ONBOARDING_DAY3_NUDGE,
      userId: params.userId,
    });
  }

  async sendOnboardingDay7Nudge(params: {
    recipientEmail: string;
    firstName: string | null;
    demoVideoUrl: string;
    pipelineBuilderUrl: string;
    userId: string;
  }): Promise<{ id?: string; skipped?: boolean }> {
    return this.send({
      to: [params.recipientEmail],
      templateId: this.getTemplateId(EMAIL_TYPES.ONBOARDING_DAY7_NUDGE),
      variables: {
        first_name: params.firstName ?? 'there',
        demo_video_url: params.demoVideoUrl,
        pipeline_builder_url: params.pipelineBuilderUrl,
      },
      emailType: EMAIL_TYPES.ONBOARDING_DAY7_NUDGE,
      userId: params.userId,
    });
  }
}
