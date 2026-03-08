/**
 * Internal ETL Controller
 * Endpoints called by the Python ETL pods (internal network only, no auth).
 *
 * - POST /internal/etl-callback — receive sync results from ETL pods
 * - GET  /internal/singer-state/:pipelineId — serve Singer state for --state file
 */

import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Logger,
  NotFoundException,
  Param,
  Post,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ApiExcludeController } from '@nestjs/swagger';
import { DataSourceConnectionRepository } from '../data-sources/repositories/data-source-connection.repository';
import { EmailService } from '../email/email.service';
import { EmailRepository } from '../email/repositories/email-repository';
import { OrganizationMemberRepository } from '../organizations/repositories/organization-member.repository';
import { PgmqQueueService } from '../queue';
import { UserRepository } from '../users/repositories/user.repository';
import { PipelineRepository } from './repositories/pipeline.repository';
import { PipelineDestinationSchemaRepository } from './repositories/pipeline-destination-schema.repository';
import { PipelineSourceSchemaRepository } from './repositories/pipeline-source-schema.repository';
import { PipelineLifecycleService } from './services/pipeline-lifecycle.service';
import { PipelineStatus } from './types/pipeline-lifecycle.types';
import { sanitizeEtlError } from './utils/sanitize-etl-error';

interface EtlCallbackPayload {
  job_id: string;
  pipeline_id: string;
  organization_id: string;
  status: 'completed' | 'failed' | 'interrupted';
  rows_read?: number;
  rows_upserted?: number;
  rows_dropped?: number;
  rows_deleted?: number;
  lsn_end?: number;
  singer_state?: Record<string, unknown> | null;
  error?: string;
  duration_seconds?: number;
  source_tool?: string;
  dest_tool?: string;
  replication_method_used?: string;
}

@ApiExcludeController()
@Controller('internal')
export class InternalEtlController {
  private readonly logger = new Logger(InternalEtlController.name);

  constructor(
    private readonly pipelineRepository: PipelineRepository,
    private readonly pipelineSourceSchemaRepository: PipelineSourceSchemaRepository,
    private readonly pipelineDestinationSchemaRepository: PipelineDestinationSchemaRepository,
    private readonly connectionRepository: DataSourceConnectionRepository,
    readonly _lifecycleService: PipelineLifecycleService,
    private readonly pipelineQueueService: PgmqQueueService,
    private readonly emailService: EmailService,
    private readonly emailRepository: EmailRepository,
    private readonly configService: ConfigService,
    private readonly organizationMemberRepository: OrganizationMemberRepository,
    private readonly userRepository: UserRepository,
  ) {}

  /**
   * POST /internal/etl-callback
   * Called by ETL pods when a sync completes (or fails).
   */
  @Post('etl-callback')
  @HttpCode(HttpStatus.OK)
  async etlCallback(@Body() payload: EtlCallbackPayload) {
    const {
      job_id: runId,
      pipeline_id: pipelineId,
      organization_id: organizationId,
      status,
      rows_read: rowsRead = 0,
      rows_upserted: rowsUpserted = 0,
      rows_dropped: rowsDropped = 0,
      singer_state: singerState,
      error: errorMessage,
      duration_seconds: durationSeconds = 0,
      replication_method_used: replicationMethodUsed,
    } = payload;

    this.logger.log(
      `ETL callback: pipeline=${pipelineId} run=${runId} status=${status} rows_read=${rowsRead} rows_written=${rowsUpserted} rows_dropped=${rowsDropped}`,
    );

    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (!pipeline) {
      this.logger.warn(`ETL callback for unknown pipeline ${pipelineId}`);
      return { received: true, warning: 'Pipeline not found' };
    }

    // Determine run status
    const runStatus = status === 'completed' ? 'success' : 'failed';
    const jobState = status === 'interrupted' ? 'failed' : 'completed';
    const sanitizedError = sanitizeEtlError(errorMessage);
    const rowsFailed = status === 'failed' ? Math.max(rowsRead - rowsUpserted - rowsDropped, 1) : 0;

    // Update pipeline_runs row
    try {
      await this.pipelineRepository.updateRun(runId, {
        status: runStatus,
        jobState,
        rowsRead,
        rowsWritten: rowsUpserted,
        rowsSkipped: rowsDropped,
        rowsFailed,
        completedAt: new Date(),
        durationSeconds: Math.round(durationSeconds),
        errorMessage: sanitizedError,
      });
    } catch (err) {
      this.logger.error(`Failed to update run ${runId}: ${err}`);
    }

    // Save singer_state to pipeline (opaque blob — never parsed)
    if (singerState && status === 'completed') {
      try {
        await this.pipelineRepository.update(pipelineId, {
          checkpoint: singerState,
        });
      } catch (err) {
        this.logger.error(`Failed to save singer state for ${pipelineId}: ${err}`);
      }
    }

    // Determine target pipeline status after run
    const syncMode = pipeline.syncMode || 'full';
    let targetStatus: PipelineStatus;
    if (status !== 'completed') {
      targetStatus = PipelineStatus.FAILED;
    } else if (syncMode === 'cdc' || syncMode === 'log_based' || syncMode === 'incremental') {
      targetStatus = PipelineStatus.LISTING;
    } else {
      targetStatus = PipelineStatus.IDLE;
    }

    // Save replication slot to connection when first LOG_BASED run completes
    if (
      status === 'completed' &&
      (syncMode === 'cdc' || syncMode === 'log_based') &&
      replicationMethodUsed === 'LOG_BASED'
    ) {
      try {
        const sourceSchema = await this.pipelineSourceSchemaRepository.findById(
          pipeline.sourceSchemaId,
        );
        if (sourceSchema?.dataSourceId) {
          const connection = await this.connectionRepository.findByDataSourceId(
            sourceSchema.dataSourceId,
          );
          if (connection && !connection.replicationSlotName) {
            const slotName = `mxf_${connection.id.replace(/-/g, '').slice(0, 8)}`;
            await this.connectionRepository.updateByDataSourceId(sourceSchema.dataSourceId, {
              replicationSlotName: slotName,
            });
            this.logger.log(
              `Saved replication slot ${slotName} to connection for data source ${sourceSchema.dataSourceId}`,
            );
          }
        }
      } catch (err) {
        this.logger.error(`Failed to save replication slot for pipeline ${pipelineId}: ${err}`);
      }
    }

    // Update cumulative stats on pipeline
    const newTotalProcessed = (pipeline.totalRowsProcessed || 0) + rowsUpserted;
    const setFullRefreshCompletedAt =
      status === 'completed' &&
      (syncMode === 'cdc' || syncMode === 'log_based') &&
      replicationMethodUsed === 'FULL_TABLE';
    try {
      await this.pipelineRepository.update(pipelineId, {
        lastRunAt: new Date(),
        lastRunStatus: runStatus,
        status: targetStatus,
        totalRowsProcessed: newTotalProcessed,
        totalRunsSuccessful: (pipeline.totalRunsSuccessful || 0) + (status === 'completed' ? 1 : 0),
        totalRunsFailed: (pipeline.totalRunsFailed || 0) + (status !== 'completed' ? 1 : 0),
        lastSyncAt: new Date(),
        ...(sanitizedError ? { lastError: sanitizedError } : {}),
        ...(setFullRefreshCompletedAt ? { fullRefreshCompletedAt: new Date() } : {}),
      });
    } catch (err) {
      this.logger.error(`Failed to update pipeline ${pipelineId}: ${err}`);
    }

    // Publish Socket.io status update
    if (this.pipelineQueueService.isReady()) {
      await this.pipelineQueueService.publishStatusUpdate({
        pipelineId,
        organizationId,
        status: targetStatus,
        rowsProcessed: newTotalProcessed,
        newRowsCount: rowsUpserted,
        error: sanitizedError || undefined,
        timestamp: new Date().toISOString(),
      });
    }

    // Email notifications (non-blocking)
    this.sendPipelineEmails({
      pipeline,
      pipelineId,
      organizationId,
      status,
      runStatus,
      runId,
      rowsUpserted,
      durationSeconds,
      sanitizedError: sanitizedError ?? null,
      newTotalProcessed,
      setFullRefreshCompletedAt,
      replicationMethodUsed,
      syncMode,
    }).catch((err) => this.logger.warn(`Pipeline email send failed: ${err}`));

    return { received: true };
  }

  /**
   * Send pipeline lifecycle emails (failure, recovered, disabled, first success, log-based complete)
   */
  private async sendPipelineEmails(params: {
    pipeline: Awaited<ReturnType<PipelineRepository['findById']>>;
    pipelineId: string;
    organizationId: string;
    status: string;
    runStatus: string;
    runId: string;
    rowsUpserted: number;
    durationSeconds: number;
    sanitizedError: string | null;
    newTotalProcessed: number;
    setFullRefreshCompletedAt: boolean;
    replicationMethodUsed?: string;
    syncMode: string;
  }): Promise<void> {
    const {
      pipeline,
      pipelineId,
      organizationId,
      status,
      runStatus,
      runId,
      rowsUpserted,
      durationSeconds,
      sanitizedError,
      newTotalProcessed,
      setFullRefreshCompletedAt,
      replicationMethodUsed,
      syncMode,
    } = params;
    if (!pipeline) return;

    const frontendUrl = this.configService.get<string>('FRONTEND_URL') ?? '';
    const runDetailUrl = `${frontendUrl}/workspace/data-pipelines/${pipelineId}?run=${runId}`;
    const editPipelineUrl = `${frontendUrl}/workspace/data-pipelines/${pipelineId}/edit`;
    const runHistoryUrl = `${frontendUrl}/workspace/data-pipelines/${pipelineId}`;
    const pipelineUrl = runHistoryUrl;
    const supportUrl = `${frontendUrl}/support`;

    const [sourceSchema, destSchema] = await Promise.all([
      this.pipelineSourceSchemaRepository.findById(pipeline.sourceSchemaId),
      this.pipelineDestinationSchemaRepository.findById(pipeline.destinationSchemaId),
    ]);
    const sourceStream =
      sourceSchema?.sourceSchema && sourceSchema?.sourceTable
        ? `${sourceSchema.sourceSchema}-${sourceSchema.sourceTable}`
        : sourceSchema?.sourceTable ?? 'unknown';
    const destTable = destSchema?.destinationTable ?? 'unknown';

    const recipientEmails = await this.getPipelineRecipientEmails(
      organizationId,
      pipeline.createdBy,
    );

    const newTotalFailed = (pipeline.totalRunsFailed || 0) + (status !== 'completed' ? 1 : 0);
    const newTotalSuccessful = (pipeline.totalRunsSuccessful || 0) + (status === 'completed' ? 1 : 0);

    // pipeline_run_failed — with 1-hour cooldown
    if (status === 'failed' && recipientEmails.length > 0) {
      const withinCooldown =
        await this.emailRepository.wasPipelineFailureEmailSentRecently(pipelineId, 1);
      if (!withinCooldown) {
        await this.emailService.sendPipelineRunFailed({
          recipientEmails,
          pipelineName: pipeline.name,
          sourceStream,
          destTable,
          errorMessage: sanitizedError ?? 'Unknown error',
          startedAt: new Date().toISOString(),
          failedAt: new Date().toISOString(),
          runDetailUrl,
          editPipelineUrl,
          orgId: organizationId,
          pipelineId,
        });
      }
    }

    // pipeline_disabled — 5 consecutive failures, pause pipeline
    if (status === 'failed' && newTotalFailed >= 5) {
      const recentRuns = await this.pipelineRepository.findRunsByPipeline(pipelineId, 5, 0);
      const lastFiveAllFailed =
        recentRuns.length >= 5 && recentRuns.every((r) => r.status === 'failed');
      if (lastFiveAllFailed) {
        await this.pipelineRepository.update(pipelineId, {
          status: PipelineStatus.PAUSED,
          updatedAt: new Date(),
        });
        if (recipientEmails.length > 0) {
          await this.emailService.sendPipelineDisabled({
            recipientEmails,
            pipelineName: pipeline.name,
            failureCount: 5,
            lastErrorMessage: sanitizedError ?? 'Unknown error',
            editPipelineUrl,
            supportUrl,
            orgId: organizationId,
            pipelineId,
          });
        }
      }
    }

    // pipeline_recovered — success after previous failure
    if (status === 'completed' && pipeline.lastRunStatus === 'failed') {
      if (recipientEmails.length > 0) {
        await this.emailService.sendPipelineRecovered({
          recipientEmails,
          pipelineName: pipeline.name,
          rowsUpserted,
          durationSeconds: Math.round(durationSeconds),
          runHistoryUrl,
          orgId: organizationId,
          pipelineId,
        });
      }
    }

    // first_success — first-ever successful run
    if (status === 'completed' && newTotalSuccessful === 1) {
      const creator = pipeline.createdBy
        ? await this.userRepository.findById(pipeline.createdBy)
        : null;
      const creatorEmail = creator?.email;
      if (creatorEmail) {
        await this.emailService.sendFirstSuccess({
          recipientEmail: creatorEmail,
          pipelineName: pipeline.name,
          rowsUpserted,
          destTable,
          durationSeconds: Math.round(durationSeconds),
          pipelineUrl,
          orgId: organizationId,
          userId: pipeline.createdBy,
          pipelineId,
        });
      }
    }

    // log_based_initial_complete — LOG_BASED full refresh done
    if (status === 'completed' && setFullRefreshCompletedAt) {
      const creator = pipeline.createdBy
        ? await this.userRepository.findById(pipeline.createdBy)
        : null;
      const creatorEmail = creator?.email;
      if (creatorEmail) {
        await this.emailService.sendLogBasedInitialComplete({
          recipientEmail: creatorEmail,
          pipelineName: pipeline.name,
          rowsUpserted,
          destTable,
          pipelineUrl,
          orgId: organizationId,
          userId: pipeline.createdBy,
          pipelineId,
        });
      }
    }

    // pipeline_partial_success — interrupted (timeout)
    if (status === 'interrupted') {
      const creator = pipeline.createdBy
        ? await this.userRepository.findById(pipeline.createdBy)
        : null;
      const creatorEmail = creator?.email;
      if (creatorEmail) {
        await this.emailService.sendPipelinePartialSuccess({
          recipientEmail: creatorEmail,
          pipelineName: pipeline.name,
          rowsUpserted,
          timeoutSeconds: Math.round(durationSeconds),
          runDetailUrl,
          orgId: organizationId,
          userId: pipeline.createdBy,
          pipelineId,
        });
      }
    }
  }

  private async getPipelineRecipientEmails(
    organizationId: string,
    createdBy: string | null,
  ): Promise<string[]> {
    const emails = new Set<string>();
    if (createdBy) {
      const creator = await this.userRepository.findById(createdBy);
      if (creator?.email) emails.add(creator.email);
    }
    const members = await this.organizationMemberRepository.findByOrganizationId(organizationId);
    for (const m of members) {
      if ((m.role === 'OWNER' || m.role === 'ADMIN') && m.email) {
        emails.add(m.email);
      }
    }
    return Array.from(emails);
  }

  /**
   * GET /internal/singer-state/:pipelineId
   * Called by ETL pods before starting a LOG_BASED sync to get previous state.
   */
  @Get('singer-state/:pipelineId')
  async getSingerState(@Param('pipelineId') pipelineId: string) {
    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    const singerState = (pipeline.checkpoint as Record<string, unknown>) ?? null;

    return { singer_state: singerState };
  }
}
