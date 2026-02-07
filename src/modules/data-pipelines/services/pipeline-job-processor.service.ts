/**
 * BullMQ workers for full sync, incremental sync, and polling checks.
 *
 * Important behavior:
 * - Workers wait for pipeline run completion before marking the BullMQ job done.
 * - Polling checks call Python delta-check and atomically persist returned checkpoint.
 */

import { Processor, WorkerHost, OnWorkerEvent } from '@nestjs/bullmq';
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Job } from 'bullmq';
import { ActivityLoggerService } from '../../../common/logger';
import { PipelineRepository } from '../repositories/pipeline.repository';
import { PipelineService } from './pipeline.service';
import { PythonETLService } from './python-etl.service';
import {
  DeltaCheckJobData,
  FullSyncJobData,
  IncrementalSyncJobData,
  PipelineQueueService,
} from '../../queue/pipeline-queue.service';
import { QUEUE_NAMES } from '../../queue/bullmq.module';

const RUN_POLL_INTERVAL_MS = 2_000;
const RUN_WAIT_TIMEOUT_MS = 4 * 60 * 60 * 1000; // 4 hours

@Injectable()
@Processor(QUEUE_NAMES.PIPELINE_JOBS)
export class PipelineJobsProcessor extends WorkerHost {
  private readonly logger = new Logger(PipelineJobsProcessor.name);

  constructor(
    private readonly pipelineService: PipelineService,
    private readonly pipelineRepository: PipelineRepository,
    private readonly pipelineQueueService: PipelineQueueService,
    private readonly activity: ActivityLoggerService,
  ) {
    super();
  }

  async process(job: Job<FullSyncJobData, unknown, string>): Promise<void> {
    const { pipelineId, organizationId, userId, triggerType, batchSize } = job.data;
    this.activity.info('job.full_sync', `Starting full sync job for pipeline ${pipelineId}`, {
      pipelineId, organizationId, userId, metadata: { triggerType, batchSize, jobId: job.id },
    });

    try {
      const pipeline = await this.pipelineRepository.findById(pipelineId);
      if (!pipeline) {
        throw new Error(`Pipeline ${pipelineId} not found`);
      }
      if (pipeline.status === 'running') {
        this.logger.warn(`[FULL-SYNC] Pipeline ${pipelineId} already running, skipping job`);
        return;
      }

      const run = await this.pipelineService.runPipeline(pipelineId, userId || 'system', triggerType, {
        batchSize: batchSize || 500,
      });
      const completedRun = await this.waitForRunCompletion(run.id);

      if (completedRun.status !== 'success') {
        throw new Error(
          completedRun.errorMessage || `Pipeline run ${completedRun.id} ended with status ${completedRun.status}`,
        );
      }

      await this.pipelineQueueService.publishStatusUpdate({
        pipelineId,
        organizationId,
        status: 'completed',
        rowsProcessed: completedRun.rowsWritten || 0,
        timestamp: new Date().toISOString(),
      });
      this.logger.log(`[FULL-SYNC] Completed job for pipeline ${pipelineId}`);
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error);
      this.logger.error(`[FULL-SYNC] Job failed for ${pipelineId}: ${errorMsg}`);
      await this.pipelineQueueService.publishStatusUpdate({
        pipelineId,
        organizationId,
        status: 'failed',
        error: errorMsg,
        timestamp: new Date().toISOString(),
      });
      throw error;
    }
  }

  private async waitForRunCompletion(runId: string) {
    const startedAt = Date.now();

    while (Date.now() - startedAt <= RUN_WAIT_TIMEOUT_MS) {
      const run = await this.pipelineRepository.findRunById(runId);
      if (!run) {
        throw new Error(`Pipeline run ${runId} not found while waiting for completion`);
      }
      if (['success', 'failed', 'cancelled'].includes(run.status || '')) {
        return run;
      }
      await new Promise((resolve) => setTimeout(resolve, RUN_POLL_INTERVAL_MS));
    }

    throw new Error(`Timed out waiting for run ${runId} completion`);
  }

  @OnWorkerEvent('failed')
  onFailed(job: Job<FullSyncJobData> | undefined, error: Error): void {
    if (job) {
      this.logger.warn(`[FULL-SYNC] Job ${job.id} failed: ${error.message}`);
    }
  }
}

@Injectable()
@Processor(QUEUE_NAMES.INCREMENTAL_SYNC)
export class IncrementalSyncProcessor extends WorkerHost {
  private readonly logger = new Logger(IncrementalSyncProcessor.name);

  constructor(
    private readonly pipelineService: PipelineService,
    private readonly pipelineRepository: PipelineRepository,
    private readonly pipelineQueueService: PipelineQueueService,
    private readonly activity: ActivityLoggerService,
  ) {
    super();
  }

  async process(job: Job<IncrementalSyncJobData, unknown, string>): Promise<void> {
    const { pipelineId, organizationId, userId, triggerType, batchSize } = job.data;
    this.activity.info('job.incremental_sync', `Starting incremental sync job for pipeline ${pipelineId}`, {
      pipelineId, organizationId, userId, metadata: { triggerType, batchSize, jobId: job.id },
    });

    try {
      const pipeline = await this.pipelineRepository.findById(pipelineId);
      if (!pipeline) {
        throw new Error(`Pipeline ${pipelineId} not found`);
      }

      if (!['listing', 'idle', 'completed', 'failed'].includes(pipeline.status || '')) {
        this.logger.warn(
          `[INCREMENTAL-SYNC] Pipeline ${pipelineId} is in ${pipeline.status} status, skipping`,
        );
        return;
      }

      const mappedTriggerType =
        triggerType === 'polling' ? 'polling' : triggerType === 'resume' ? 'manual' : 'scheduled';

      const run = await this.pipelineService.runPipeline(
        pipelineId,
        userId || pipeline.createdBy || 'system',
        mappedTriggerType,
        { batchSize: batchSize || 500 },
      );
      const completedRun = await this.waitForRunCompletion(run.id);

      if (completedRun.status !== 'success') {
        throw new Error(
          completedRun.errorMessage || `Pipeline run ${completedRun.id} ended with status ${completedRun.status}`,
        );
      }

      await this.pipelineQueueService.publishStatusUpdate({
        pipelineId,
        organizationId,
        status: 'listing',
        rowsProcessed: completedRun.rowsWritten || 0,
        timestamp: new Date().toISOString(),
      });
      this.logger.log(`[INCREMENTAL-SYNC] Completed job for pipeline ${pipelineId}`);
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error);
      this.logger.error(`[INCREMENTAL-SYNC] Job failed for ${pipelineId}: ${errorMsg}`);
      await this.pipelineQueueService.publishStatusUpdate({
        pipelineId,
        organizationId,
        status: 'failed',
        error: errorMsg,
        timestamp: new Date().toISOString(),
      });
      throw error;
    }
  }

  private async waitForRunCompletion(runId: string) {
    const startedAt = Date.now();

    while (Date.now() - startedAt <= RUN_WAIT_TIMEOUT_MS) {
      const run = await this.pipelineRepository.findRunById(runId);
      if (!run) {
        throw new Error(`Pipeline run ${runId} not found while waiting for completion`);
      }
      if (['success', 'failed', 'cancelled'].includes(run.status || '')) {
        return run;
      }
      await new Promise((resolve) => setTimeout(resolve, RUN_POLL_INTERVAL_MS));
    }

    throw new Error(`Timed out waiting for run ${runId} completion`);
  }

  @OnWorkerEvent('failed')
  onFailed(job: Job<IncrementalSyncJobData> | undefined, error: Error): void {
    if (job) {
      this.logger.warn(`[INCREMENTAL-SYNC] Job ${job.id} failed: ${error.message}`);
    }
  }
}

@Injectable()
@Processor(QUEUE_NAMES.POLLING_CHECKS)
export class PollingChecksProcessor extends WorkerHost implements OnModuleInit {
  private readonly logger = new Logger(PollingChecksProcessor.name);

  constructor(
    private readonly pipelineRepository: PipelineRepository,
    private readonly pipelineQueueService: PipelineQueueService,
    private readonly pythonETLService: PythonETLService,
    private readonly activity: ActivityLoggerService,
  ) {
    super();
  }

  async onModuleInit(): Promise<void> {
    await this.schedulePollCycle();
  }

  private async schedulePollCycle(): Promise<void> {
    const queue = this.pipelineQueueService.getPollingChecksQueue();
    try {
      await queue.add(
        'poll-cycle',
        {},
        {
          repeat: { pattern: '*/5 * * * *' },
          removeOnComplete: { count: 100 },
        },
      );
      this.logger.log('[POLLING] CDC poll cycle scheduled every 5 minutes');
    } catch (error) {
      this.logger.error(`[POLLING] Failed to schedule poll-cycle: ${error}`);
    }
  }

  async process(job: Job<DeltaCheckJobData | Record<string, never>, unknown, string>): Promise<void> {
    if (job.name === 'poll-cycle') {
      await this.runPollCycle();
      return;
    }

    const { pipelineId, organizationId } = job.data as DeltaCheckJobData;
    this.logger.debug(`[DELTA-CHECK] Checking pipeline ${pipelineId}`);

    try {
      const pipeline = await this.pipelineRepository.findById(pipelineId);
      if (!pipeline) {
        this.logger.warn(`[DELTA-CHECK] Pipeline ${pipelineId} not found`);
        return;
      }
      if (pipeline.status !== 'listing') {
        this.logger.debug(
          `[DELTA-CHECK] Pipeline ${pipelineId} skipped because status=${pipeline.status}`,
        );
        return;
      }

      const result = await this.checkForChanges(pipelineId, pipeline.checkpoint as Record<string, unknown>);

      if (result.checkpoint) {
        await this.pipelineRepository.saveCheckpointStateAtomic(pipelineId, result.checkpoint);
      }

      if (!result.hasChanges) {
        return;
      }

      this.logger.log(`[DELTA-CHECK] Changes detected for pipeline ${pipelineId}`);
      await this.pipelineQueueService.enqueueIncrementalSync({
        pipelineId,
        organizationId,
        userId: pipeline.createdBy || 'system',
        triggerType: 'polling',
        checkpoint: ((result.checkpoint || pipeline.checkpoint || {}) as Record<string, unknown>) as IncrementalSyncJobData['checkpoint'],
        batchSize: 500,
      });
    } catch (error) {
      this.logger.error(`[DELTA-CHECK] Error for pipeline ${pipelineId}: ${error}`);
    }
  }

  private async runPollCycle(): Promise<void> {
    try {
      const activePipelines = await this.pipelineRepository.findActivePipelinesForPolling();
      if (activePipelines.length === 0) {
        return;
      }

      this.logger.log(`[POLLING] ${activePipelines.length} pipeline(s) eligible for delta-check`);
      for (const pipeline of activePipelines) {
        await this.pipelineQueueService.enqueueDeltaCheck({
          pipelineId: pipeline.id,
          organizationId: pipeline.organizationId,
        });
      }
    } catch (error) {
      this.logger.error(`[POLLING] Poll cycle error: ${error}`);
    }
  }

  private async checkForChanges(
    pipelineId: string,
    checkpoint: Record<string, unknown> | null,
  ): Promise<{ hasChanges: boolean; checkpoint?: Record<string, unknown> }> {
    try {
      const pipeline = await this.pipelineRepository.findByIdForCDC(pipelineId);
      if (!pipeline?.sourceSchema) {
        return { hasChanges: false };
      }

      const connectionConfig = await this.pythonETLService.getConnectionConfig(
        pipeline.sourceSchema,
        pipeline.organizationId,
      );

      const result = await this.pythonETLService.deltaCheck({
        sourceSchema: pipeline.sourceSchema,
        connectionConfig,
        checkpoint: checkpoint || {},
      });

      return {
        hasChanges: result.hasChanges,
        checkpoint: result.checkpoint,
      };
    } catch (error) {
      this.logger.warn(`[DELTA-CHECK] Failed delta-check for ${pipelineId}: ${error}`);
      return { hasChanges: false };
    }
  }

  @OnWorkerEvent('failed')
  onFailed(job: Job | undefined, error: Error): void {
    if (job) {
      this.logger.warn(`[POLLING-CHECKS] Job ${job.id} failed: ${error.message}`);
    }
  }
}
