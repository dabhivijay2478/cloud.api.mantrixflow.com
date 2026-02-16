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

import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { ActivityLoggerService } from '../../../common/logger';
import { PipelineRepository } from '../repositories/pipeline.repository';
import { PipelineService } from './pipeline.service';
import { PythonETLService } from './python-etl.service';
import {
  PgmqQueueService,
  PgmqMessage,
  PgmqJobPayload,
  FullSyncJobData,
  IncrementalSyncJobData,
  DeltaCheckJobData,
} from '../../queue';
import {
  PGMQ_QUEUE_NAMES,
  PGMQ_POLL_INTERVAL_MS,
  PGMQ_VT_LONG_SEC,
  PGMQ_VT_SHORT_SEC,
  PGMQ_PARALLEL_WORKERS,
} from '../../queue';

/** How often to poll the run DB for completion (ms) */
const RUN_POLL_INTERVAL_MS = 2_000;

/** Max time to wait for a pipeline run to complete (ms) */
const RUN_WAIT_TIMEOUT_MS = 4 * 60 * 60 * 1_000; // 4 hours

@Injectable()
export class PipelineJobProcessor implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PipelineJobProcessor.name);
  private readonly intervals: NodeJS.Timeout[] = [];
  private readonly activeQueue = new Map<string, boolean>();
  private isShuttingDown = false;

  constructor(
    private readonly pipelineService: PipelineService,
    private readonly pipelineRepository: PipelineRepository,
    private readonly queueService: PgmqQueueService,
    private readonly pythonETLService: PythonETLService,
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
      const batchSize =
        queueName === PGMQ_QUEUE_NAMES.POLLING_CHECKS ? 1 : PGMQ_PARALLEL_WORKERS;
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
      await Promise.allSettled(
        messages.map((msg) => this.dispatch(queueName, msg)),
      );
    } catch (error) {
      this.logger.error(`Error polling "${queueName}": ${error}`);
    } finally {
      this.activeQueue.set(queueName, false);
    }
  }

  private async dispatch(
    queueName: string,
    msg: PgmqMessage<PgmqJobPayload>,
  ): Promise<void> {
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

  private async handleFullSync(
    msg: PgmqMessage<PgmqJobPayload<FullSyncJobData>>,
  ): Promise<void> {
    const { data, retryCount = 0, maxRetries = 5 } = msg.message;
    const { pipelineId, organizationId, userId, triggerType, batchSize } = data;
    this.activity.info('job.full_sync', `Starting full sync for pipeline ${pipelineId}`, {
      pipelineId,
      organizationId,
      userId,
      metadata: { triggerType, batchSize, msgId: msg.msg_id },
    });
    try {
      const pipeline = await this.pipelineRepository.findById(pipelineId);
      if (!pipeline) throw new Error(`Pipeline ${pipelineId} not found`);
      if (pipeline.status === 'running') {
        this.logger.warn(`[FULL-SYNC] Pipeline ${pipelineId} already running — skipping`);
        await this.queueService.archiveMessage(PGMQ_QUEUE_NAMES.PIPELINE_JOBS, msg.msg_id);
        return;
      }
      const run = await this.pipelineService.runPipeline(
        pipelineId,
        userId || 'system',
        triggerType,
        { batchSize: batchSize || 500 },
      );
      const completedRun = await this.waitForRunCompletion(run.id);
      if (completedRun.status !== 'success') {
        throw new Error(
          completedRun.errorMessage ||
            `Run ${completedRun.id} ended with status ${completedRun.status}`,
        );
      }
      await this.queueService.publishStatusUpdate({
        pipelineId,
        organizationId,
        status: 'completed',
        rowsProcessed: completedRun.rowsWritten || 0,
        timestamp: new Date().toISOString(),
      });
      this.logger.log(`[FULL-SYNC] Completed for pipeline ${pipelineId}`);
      await this.queueService.archiveMessage(PGMQ_QUEUE_NAMES.PIPELINE_JOBS, msg.msg_id);
    } catch (error) {
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
    const { data, retryCount = 0, maxRetries = 5 } = msg.message;
    const { pipelineId, organizationId, userId, triggerType, batchSize } = data;
    this.activity.info(
      'job.incremental_sync',
      `Starting incremental sync for pipeline ${pipelineId}`,
      {
        pipelineId,
        organizationId,
        userId,
        metadata: { triggerType, batchSize, msgId: msg.msg_id },
      },
    );
    try {
      const pipeline = await this.pipelineRepository.findById(pipelineId);
      if (!pipeline) throw new Error(`Pipeline ${pipelineId} not found`);
      if (!['listing', 'idle', 'completed', 'failed'].includes(pipeline.status || '')) {
        this.logger.warn(
          `[INCREMENTAL-SYNC] Pipeline ${pipelineId} is ${pipeline.status} — skipping`,
        );
        await this.queueService.archiveMessage(
          PGMQ_QUEUE_NAMES.INCREMENTAL_SYNC,
          msg.msg_id,
        );
        return;
      }
      const mappedTrigger =
        triggerType === 'polling'
          ? 'polling'
          : triggerType === 'resume'
            ? 'manual'
            : 'scheduled';
      const run = await this.pipelineService.runPipeline(
        pipelineId,
        userId || pipeline.createdBy || 'system',
        mappedTrigger,
        { batchSize: batchSize || 500 },
      );
      const completedRun = await this.waitForRunCompletion(run.id);
      if (completedRun.status !== 'success') {
        throw new Error(
          completedRun.errorMessage ||
            `Run ${completedRun.id} ended with status ${completedRun.status}`,
        );
      }
      await this.queueService.publishStatusUpdate({
        pipelineId,
        organizationId,
        status: 'listing',
        rowsProcessed: completedRun.rowsWritten || 0,
        timestamp: new Date().toISOString(),
      });
      this.logger.log(`[INCREMENTAL-SYNC] Completed for pipeline ${pipelineId}`);
      await this.queueService.archiveMessage(
        PGMQ_QUEUE_NAMES.INCREMENTAL_SYNC,
        msg.msg_id,
      );
    } catch (error) {
      await this.handleFailure(
        PGMQ_QUEUE_NAMES.INCREMENTAL_SYNC,
        msg,
        retryCount,
        maxRetries,
        pipelineId,
        organizationId,
        error,
        'INCREMENTAL-SYNC',
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
    this.logger.log(
      `[POLLING] ${activePipelines.length} pipeline(s) eligible for delta-check`,
    );
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
      this.logger.debug(
        `[DELTA-CHECK] Pipeline ${pipelineId} skipped (status=${pipeline.status})`,
      );
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
    await this.queueService.enqueueIncrementalSync({
      pipelineId,
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
    checkpoint: Record<string, unknown> | null,
  ): Promise<{ hasChanges: boolean; checkpoint?: Record<string, unknown> }> {
    try {
      const pipeline = await this.pipelineRepository.findByIdForCDC(pipelineId);
      if (!pipeline?.sourceSchema) return { hasChanges: false };
      const connectionConfig = await this.pythonETLService.getConnectionConfig(
        pipeline.sourceSchema,
        pipeline.organizationId,
      );
      const result = await this.pythonETLService.deltaCheck({
        sourceSchema: pipeline.sourceSchema,
        connectionConfig,
        checkpoint: checkpoint || {},
      });
      return { hasChanges: result.hasChanges, checkpoint: result.checkpoint };
    } catch (error) {
      this.logger.warn(`[DELTA-CHECK] Failed for ${pipelineId}: ${error}`);
      return { hasChanges: false };
    }
  }

  // ════════════════════════════════════════════════════════════════
  // SHARED HELPERS
  // ════════════════════════════════════════════════════════════════

  private async waitForRunCompletion(runId: string) {
    const startedAt = Date.now();
    while (Date.now() - startedAt <= RUN_WAIT_TIMEOUT_MS) {
      const run = await this.pipelineRepository.findRunById(runId);
      if (!run) throw new Error(`Pipeline run ${runId} not found while waiting`);
      if (['success', 'failed', 'cancelled'].includes(run.status || '')) return run;
      await new Promise((resolve) => setTimeout(resolve, RUN_POLL_INTERVAL_MS));
    }
    throw new Error(`Timed out waiting for run ${runId} completion`);
  }

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
