/**
 * PgMQ Job Processor
 * Polls pgmq queues for pipeline jobs (full sync, incremental sync, polling checks)
 * and processes them.
 *
 * Replaces BullMQ workers with pgmq-based queue polling.
 * Each queue is polled independently so long-running jobs on one queue
 * do not block processing on other queues.
 *
 * Architecture:
 * - pg_cron inserts poll-cycle messages into the polling_checks queue every 5 min
 * - This processor reads messages from all three queues and dispatches to handlers
 * - Failed jobs are retried with exponential backoff via pgmq.send_delay()
 */

import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ActivityLoggerService } from '../../../common/logger';
import {
  areSourceDbMutationsAllowed,
  SOURCE_DB_MUTATION_POLICY_MESSAGE,
} from '../../../common/utils/source-db-mutation-policy';
import {
  resolveDestinationConnectorType,
  resolveSourceConnectorType,
} from '../../connectors/utils/connector-resolver';
import { ConnectionService } from '../../data-sources/connection.service';
import { DataSourceConnectionRepository } from '../../data-sources/repositories/data-source-connection.repository';
import {
  DeltaCheckJobData,
  FullSyncJobData,
  IncrementalSyncJobData,
  PGMQ_MAX_DISPATCH_RETRIES,
  PGMQ_PARALLEL_WORKERS,
  PGMQ_POLL_INTERVAL_MS,
  PGMQ_QUEUE_NAMES,
  PGMQ_VT_LONG_SEC,
  PGMQ_VT_SHORT_SEC,
  PgmqJobPayload,
  PgmqMessage,
  PgmqQueueService,
} from '../../queue';
import { PipelineRepository } from '../repositories/pipeline.repository';
import { PythonETLService } from './python-etl.service';

@Injectable()
export class PipelineJobProcessor implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PipelineJobProcessor.name);
  private readonly intervals: NodeJS.Timeout[] = [];
  private readonly activeQueue = new Map<string, boolean>();
  private isShuttingDown = false;
  constructor(
    private readonly pipelineRepository: PipelineRepository,
    private readonly queueService: PgmqQueueService,
    private readonly pythonETLService: PythonETLService,
    private readonly connectionService: ConnectionService,
    private readonly connectionRepository: DataSourceConnectionRepository,
    private readonly activity: ActivityLoggerService,
  ) {}

  async onModuleInit(): Promise<void> {
    // Give PgmqQueueService a moment to finish its init
    setTimeout(() => this.startPolling(), 3_000);
  }

  onModuleDestroy(): void {
    this.isShuttingDown = true;
    for (const interval of this.intervals) clearInterval(interval);
    this.intervals.length = 0;
    this.logger.log('PgMQ job processor stopped');
  }

  // ════════════════════════════════════════════════════════════════
  // POLLING BOOTSTRAP
  // ════════════════════════════════════════════════════════════════

  private startPolling(): void {
    if (!this.queueService.isReady()) {
      this.logger.warn('PgMQ not ready, retrying in 3 s…');
      setTimeout(() => this.startPolling(), 3_000);
      return;
    }
    this.intervals.push(
      setInterval(
        () => void this.poll(PGMQ_QUEUE_NAMES.PIPELINE_JOBS, PGMQ_VT_LONG_SEC),
        PGMQ_POLL_INTERVAL_MS,
      ),
      setInterval(
        () => void this.poll(PGMQ_QUEUE_NAMES.INCREMENTAL_SYNC, PGMQ_VT_LONG_SEC),
        PGMQ_POLL_INTERVAL_MS,
      ),
      setInterval(
        () => void this.poll(PGMQ_QUEUE_NAMES.POLLING_CHECKS, PGMQ_VT_SHORT_SEC),
        PGMQ_POLL_INTERVAL_MS,
      ),
    );
    this.logger.log('PgMQ job processor started — polling 3 queues');
  }

  private async poll(queueName: string, vtSec: number): Promise<void> {
    if (this.isShuttingDown || this.activeQueue.get(queueName)) return;
    this.activeQueue.set(queueName, true);
    try {
      const batchSize = queueName === PGMQ_QUEUE_NAMES.POLLING_CHECKS ? 1 : PGMQ_PARALLEL_WORKERS;
      const messages = await this.queueService.readMessages<PgmqJobPayload>(
        queueName,
        batchSize,
        vtSec,
      );
      if (messages.length === 0) return;
      if (messages.length === 1) {
        await this.dispatch(queueName, messages[0]);
        return;
      }
      await Promise.allSettled(messages.map((msg) => this.dispatch(queueName, msg)));
    } catch (error) {
      this.logger.error(`Error polling "${queueName}": ${error}`);
    } finally {
      this.activeQueue.set(queueName, false);
    }
  }

  private async dispatch(queueName: string, msg: PgmqMessage<PgmqJobPayload>): Promise<void> {
    switch (queueName) {
      case PGMQ_QUEUE_NAMES.PIPELINE_JOBS:
        await this.handleFullSync(msg as PgmqMessage<PgmqJobPayload<FullSyncJobData>>);
        break;
      case PGMQ_QUEUE_NAMES.INCREMENTAL_SYNC:
        await this.handleIncrementalSync(
          msg as PgmqMessage<PgmqJobPayload<IncrementalSyncJobData>>,
        );
        break;
      case PGMQ_QUEUE_NAMES.POLLING_CHECKS:
        await this.handlePollingCheck(msg);
        break;
    }
  }

  // ════════════════════════════════════════════════════════════════
  // FULL SYNC HANDLER
  // ════════════════════════════════════════════════════════════════

  private async handleFullSync(msg: PgmqMessage<PgmqJobPayload<FullSyncJobData>>): Promise<void> {
    const { data, retryCount = 0, maxRetries = PGMQ_MAX_DISPATCH_RETRIES } = msg.message;
    const { pipelineId, runId, organizationId, userId, triggerType } = data;
    this.activity.info('job.full_sync', `Dispatching full sync for pipeline ${pipelineId}`, {
      pipelineId,
      organizationId,
      userId,
      metadata: { triggerType, runId, msgId: msg.msg_id },
    });
    try {
      const pipelineWithSchemas = await this.pipelineRepository.findByIdWithSchemas(pipelineId);
      if (!pipelineWithSchemas) throw new Error(`Pipeline ${pipelineId} not found`);
      const { pipeline, sourceSchema, destinationSchema } = pipelineWithSchemas;

      if (!sourceSchema.dataSourceId || !destinationSchema.dataSourceId) {
        throw new Error(`Pipeline ${pipelineId} missing source or destination data source`);
      }

      const [
        sourceConnectionConfig,
        destConnectionConfig,
        sourceConnectionType,
        destConnectionType,
      ] = await Promise.all([
        this.connectionService.getDecryptedConnection(
          organizationId,
          sourceSchema.dataSourceId,
          userId || 'system',
        ),
        this.connectionService.getDecryptedConnection(
          organizationId,
          destinationSchema.dataSourceId,
          userId || 'system',
        ),
        this.connectionService.getConnectionType(
          organizationId,
          sourceSchema.dataSourceId,
          userId || 'system',
        ),
        this.connectionService.getConnectionType(
          organizationId,
          destinationSchema.dataSourceId,
          userId || 'system',
        ),
      ]);

      await this.pipelineRepository.updateRun(runId, {
        status: 'running',
        jobState: 'running',
        startedAt: new Date(),
      });
      await this.pipelineRepository.update(pipelineId, {
        status: 'running',
        lastRunAt: new Date(),
      });

      const result = await this.pythonETLService.runSync({
        jobId: runId,
        pipelineId,
        organizationId,
        sourceSchema,
        destinationSchema,
        sourceConnectionConfig,
        destConnectionConfig,
        sourceType: resolveSourceConnectorType(sourceConnectionType).registryType,
        destType: resolveDestinationConnectorType(destConnectionType).registryType,
        userId: userId || 'system',
        syncMode: 'full',
        writeMode: (destinationSchema.writeMode as 'append' | 'upsert' | 'replace') || 'append',
        upsertKey: destinationSchema.upsertKey as string[] | undefined,
        hardDelete: false,
      });

      if ('retry' in result && result.retry) {
        this.logger.warn(`[FULL-SYNC] ETL pod at capacity — requeuing pipeline ${pipelineId}`);
        await this.pipelineRepository.updateRun(runId, {
          status: 'pending',
          jobState: 'queued',
        });
        await this.pipelineRepository.update(pipelineId, { status: 'idle' });
        await this.queueService.archiveMessage(PGMQ_QUEUE_NAMES.PIPELINE_JOBS, msg.msg_id);
        if (retryCount < maxRetries) {
          await this.queueService.requeueWithBackoff(
            PGMQ_QUEUE_NAMES.PIPELINE_JOBS,
            msg.message,
            retryCount,
          );
          this.logger.warn(
            `[FULL-SYNC] Requeued pipeline ${pipelineId} (attempt ${retryCount + 1}/${maxRetries})`,
          );
        } else {
          await this.pipelineRepository.updateRun(runId, {
            status: 'failed',
            jobState: 'failed',
            errorMessage: 'Exhausted dispatch retries — all ETL pods at capacity',
            completedAt: new Date(),
          });
          this.logger.error(`[FULL-SYNC] Exhausted dispatch retries for pipeline ${pipelineId}`);
        }
        return;
      }

      this.logger.log(
        `[FULL-SYNC] Dispatched pipeline ${pipelineId} run ${runId} to ETL — callback will finalize`,
      );
      await this.queueService.archiveMessage(PGMQ_QUEUE_NAMES.PIPELINE_JOBS, msg.msg_id);
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error);
      this.logger.error(`[FULL-SYNC] Job failed for ${pipelineId}: ${errorMsg}`);
      if (runId) {
        try {
          await this.pipelineRepository.updateRun(runId, {
            status: 'failed',
            jobState: 'failed',
            errorMessage: errorMsg.substring(0, 1000),
            completedAt: new Date(),
          });
        } catch (_) {
          /* best effort */
        }
      }
      await this.handleFailure(
        PGMQ_QUEUE_NAMES.PIPELINE_JOBS,
        msg,
        retryCount,
        maxRetries,
        pipelineId,
        organizationId,
        error,
        'FULL-SYNC',
      );
    }
  }

  // ════════════════════════════════════════════════════════════════
  // INCREMENTAL SYNC HANDLER
  // ════════════════════════════════════════════════════════════════

  private async handleIncrementalSync(
    msg: PgmqMessage<PgmqJobPayload<IncrementalSyncJobData>>,
  ): Promise<void> {
    const { data, retryCount = 0, maxRetries = PGMQ_MAX_DISPATCH_RETRIES } = msg.message;
    const { pipelineId, runId, organizationId, userId, triggerType } = data;
    this.activity.info(
      'job.incremental_sync',
      `Dispatching LOG_BASED sync for pipeline ${pipelineId}`,
      {
        pipelineId,
        organizationId,
        userId,
        metadata: { triggerType, runId, msgId: msg.msg_id },
      },
    );
    try {
      const pipelineWithSchemas = await this.pipelineRepository.findByIdWithSchemas(pipelineId);
      if (!pipelineWithSchemas) throw new Error(`Pipeline ${pipelineId} not found`);
      const { pipeline, sourceSchema, destinationSchema } = pipelineWithSchemas;

      if (!['listing', 'idle', 'completed', 'failed'].includes(pipeline.status || '')) {
        this.logger.warn(`[LOG_BASED] Pipeline ${pipelineId} is ${pipeline.status} — skipping`);
        await this.queueService.archiveMessage(PGMQ_QUEUE_NAMES.INCREMENTAL_SYNC, msg.msg_id);
        return;
      }

      // Block LOG_BASED dispatch until initial full sync completed
      const isCdcOrLogBased = pipeline.syncMode === 'cdc' || pipeline.syncMode === 'log_based';
      if (isCdcOrLogBased && !areSourceDbMutationsAllowed()) {
        const errorMsg = SOURCE_DB_MUTATION_POLICY_MESSAGE;
        this.logger.warn(`[LOG_BASED] Pipeline ${pipelineId}: ${errorMsg}`);
        await this.pipelineRepository.updateRun(runId, {
          status: 'failed',
          jobState: 'failed',
          errorMessage: errorMsg,
          completedAt: new Date(),
        });
        await this.queueService.archiveMessage(PGMQ_QUEUE_NAMES.INCREMENTAL_SYNC, msg.msg_id);
        return;
      }
      if (isCdcOrLogBased && !pipeline.fullRefreshCompletedAt) {
        const errorMsg =
          "This pipeline requires an initial full sync before log-based sync can run. Click 'Run Initial Sync' to start.";
        this.logger.warn(`[LOG_BASED] Pipeline ${pipelineId}: ${errorMsg}`);
        await this.pipelineRepository.updateRun(runId, {
          status: 'failed',
          jobState: 'failed',
          errorMessage: errorMsg,
          completedAt: new Date(),
        });
        await this.queueService.archiveMessage(PGMQ_QUEUE_NAMES.INCREMENTAL_SYNC, msg.msg_id);
        return;
      }

      if (!sourceSchema.dataSourceId || !destinationSchema.dataSourceId) {
        throw new Error(`Pipeline ${pipelineId} missing source or destination data source`);
      }

      const [
        sourceConnectionConfig,
        destConnectionConfig,
        sourceConnectionType,
        destConnectionType,
        sourceConnection,
      ] = await Promise.all([
        this.connectionService.getDecryptedConnection(
          organizationId,
          sourceSchema.dataSourceId,
          userId || 'system',
        ),
        this.connectionService.getDecryptedConnection(
          organizationId,
          destinationSchema.dataSourceId,
          userId || 'system',
        ),
        this.connectionService.getConnectionType(
          organizationId,
          sourceSchema.dataSourceId,
          userId || 'system',
        ),
        this.connectionService.getConnectionType(
          organizationId,
          destinationSchema.dataSourceId,
          userId || 'system',
        ),
        this.connectionRepository.findByDataSourceId(sourceSchema.dataSourceId),
      ]);

      // Replication slot lives on connection; compute if not yet set
      let replicationSlotName = (sourceConnection?.replicationSlotName as string) || undefined;
      if (!replicationSlotName && sourceConnection?.id) {
        replicationSlotName = `mxf_${sourceConnection.id.replace(/-/g, '').slice(0, 8)}`;
      }

      await this.pipelineRepository.updateRun(runId, {
        status: 'running',
        jobState: 'running',
        startedAt: new Date(),
      });
      await this.pipelineRepository.update(pipelineId, {
        status: 'running',
        lastRunAt: new Date(),
      });

      const result = await this.pythonETLService.runSync({
        jobId: runId,
        pipelineId,
        organizationId,
        sourceSchema,
        destinationSchema,
        sourceConnectionConfig,
        destConnectionConfig,
        sourceType: resolveSourceConnectorType(sourceConnectionType).registryType,
        destType: resolveDestinationConnectorType(destConnectionType).registryType,
        userId: userId || 'system',
        syncMode: 'cdc',
        writeMode: (destinationSchema.writeMode as 'append' | 'upsert' | 'replace') || 'append',
        upsertKey: destinationSchema.upsertKey as string[] | undefined,
        hardDelete: false,
        replicationSlotName,
      });

      if ('retry' in result && result.retry) {
        this.logger.warn(`[LOG_BASED] ETL pod at capacity — requeuing pipeline ${pipelineId}`);
        await this.pipelineRepository.updateRun(runId, {
          status: 'pending',
          jobState: 'queued',
        });
        await this.pipelineRepository.update(pipelineId, { status: 'listing' });
        await this.queueService.archiveMessage(PGMQ_QUEUE_NAMES.INCREMENTAL_SYNC, msg.msg_id);
        if (retryCount < maxRetries) {
          await this.queueService.requeueWithBackoff(
            PGMQ_QUEUE_NAMES.INCREMENTAL_SYNC,
            msg.message,
            retryCount,
          );
          this.logger.warn(
            `[LOG_BASED] Requeued pipeline ${pipelineId} (attempt ${retryCount + 1}/${maxRetries})`,
          );
        } else {
          await this.pipelineRepository.updateRun(runId, {
            status: 'failed',
            jobState: 'failed',
            errorMessage: 'Exhausted dispatch retries — all ETL pods at capacity',
            completedAt: new Date(),
          });
          this.logger.error(`[LOG_BASED] Exhausted dispatch retries for pipeline ${pipelineId}`);
        }
        return;
      }

      this.logger.log(
        `[LOG_BASED] Dispatched pipeline ${pipelineId} run ${runId} to ETL — callback will finalize`,
      );
      await this.queueService.archiveMessage(PGMQ_QUEUE_NAMES.INCREMENTAL_SYNC, msg.msg_id);
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error);
      this.logger.error(`[LOG_BASED] Job failed for ${pipelineId}: ${errorMsg}`);
      if (runId) {
        try {
          await this.pipelineRepository.updateRun(runId, {
            status: 'failed',
            jobState: 'failed',
            errorMessage: errorMsg.substring(0, 1000),
            completedAt: new Date(),
          });
        } catch (_) {
          /* best effort */
        }
      }
      await this.handleFailure(
        PGMQ_QUEUE_NAMES.INCREMENTAL_SYNC,
        msg,
        retryCount,
        maxRetries,
        pipelineId,
        organizationId,
        error,
        'LOG_BASED',
      );
    }
  }

  // ════════════════════════════════════════════════════════════════
  // POLLING CHECKS HANDLER (delta-check + poll-cycle)
  // ════════════════════════════════════════════════════════════════

  private async handlePollingCheck(msg: PgmqMessage<PgmqJobPayload>): Promise<void> {
    const { name } = msg.message;
    try {
      if (name === 'poll-cycle') {
        await this.runPollCycle();
      } else {
        await this.runDeltaCheck(msg.message.data as DeltaCheckJobData);
      }
      await this.queueService.archiveMessage(PGMQ_QUEUE_NAMES.POLLING_CHECKS, msg.msg_id);
    } catch (error) {
      this.logger.error(`[POLLING] Job "${name}" failed: ${error}`);
      await this.queueService.archiveMessage(PGMQ_QUEUE_NAMES.POLLING_CHECKS, msg.msg_id);
    }
  }

  private async runPollCycle(): Promise<void> {
    const activePipelines = await this.pipelineRepository.findActivePipelinesForPolling();
    if (activePipelines.length === 0) return;
    this.logger.log(`[POLLING] ${activePipelines.length} pipeline(s) eligible for delta-check`);
    for (const pipeline of activePipelines) {
      await this.queueService.enqueueDeltaCheck({
        pipelineId: pipeline.id,
        organizationId: pipeline.organizationId,
      });
    }
  }

  private async runDeltaCheck(data: DeltaCheckJobData): Promise<void> {
    const { pipelineId, organizationId } = data;
    this.logger.debug(`[DELTA-CHECK] Checking pipeline ${pipelineId}`);
    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (!pipeline) {
      this.logger.warn(`[DELTA-CHECK] Pipeline ${pipelineId} not found`);
      return;
    }
    if (pipeline.status !== 'listing') {
      this.logger.debug(`[DELTA-CHECK] Pipeline ${pipelineId} skipped (status=${pipeline.status})`);
      return;
    }
    const result = await this.checkForChanges(
      pipelineId,
      pipeline.checkpoint as Record<string, unknown>,
    );
    if (result.checkpoint) {
      await this.pipelineRepository.saveCheckpointStateAtomic(pipelineId, result.checkpoint);
    }
    if (!result.hasChanges) return;
    this.logger.log(`[DELTA-CHECK] Changes detected for pipeline ${pipelineId}`);
    this.activity.info('job.delta_check', `Changes detected for pipeline ${pipelineId}`, {
      pipelineId,
      organizationId,
    });
    const run = await this.pipelineRepository.createRun({
      pipelineId,
      organizationId,
      status: 'pending',
      jobState: 'queued',
      triggerType: 'polling',
      triggeredBy: pipeline.createdBy || undefined,
      startedAt: new Date(),
    });
    await this.queueService.enqueueIncrementalSync({
      pipelineId,
      runId: run.id,
      organizationId,
      userId: pipeline.createdBy || 'system',
      triggerType: 'polling',
      checkpoint: (result.checkpoint || pipeline.checkpoint || {}) as Record<
        string,
        unknown
      > as IncrementalSyncJobData['checkpoint'],
      batchSize: 500,
    });
  }

  private async checkForChanges(
    pipelineId: string,
    _checkpoint: Record<string, unknown> | null,
  ): Promise<{ hasChanges: boolean; checkpoint?: Record<string, unknown> }> {
    try {
      const pipeline = await this.pipelineRepository.findByIdForCDC(pipelineId);
      if (!pipeline?.sourceSchema) return { hasChanges: false };
      // With Singer LOG_BASED CDC, we always assume changes may exist and
      // let the tap determine what's new by reading from the WAL bookmark.
      // The tap handles all change detection internally via replication slot.
      const syncMode = pipeline.syncMode || 'full';
      if (syncMode === 'cdc' || syncMode === 'log_based') {
        return { hasChanges: true };
      }
      return { hasChanges: false };
    } catch (error) {
      this.logger.warn(`[DELTA-CHECK] Failed for ${pipelineId}: ${error}`);
      return { hasChanges: false };
    }
  }

  // ════════════════════════════════════════════════════════════════
  // SHARED HELPERS
  // ════════════════════════════════════════════════════════════════

  private async handleFailure(
    queueName: string,
    msg: PgmqMessage<PgmqJobPayload>,
    retryCount: number,
    maxRetries: number,
    pipelineId: string,
    organizationId: string,
    error: unknown,
    label: string,
  ): Promise<void> {
    const errorMsg = error instanceof Error ? error.message : String(error);
    this.logger.error(`[${label}] Job failed for ${pipelineId}: ${errorMsg}`);
    await this.queueService.publishStatusUpdate({
      pipelineId,
      organizationId,
      status: 'failed',
      error: errorMsg,
      timestamp: new Date().toISOString(),
    });
    // Archive the failed message, then re-enqueue with backoff if retries remain
    await this.queueService.archiveMessage(queueName, msg.msg_id);
    if (retryCount < maxRetries) {
      await this.queueService.requeueWithBackoff(queueName, msg.message, retryCount);
      this.logger.warn(
        `[${label}] Retrying pipeline ${pipelineId} (attempt ${retryCount + 1}/${maxRetries})`,
      );
    } else {
      this.logger.error(`[${label}] Exhausted retries for pipeline ${pipelineId}`);
    }
  }
}
