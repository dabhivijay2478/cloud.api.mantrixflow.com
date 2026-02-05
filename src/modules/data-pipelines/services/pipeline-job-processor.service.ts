/**
 * Pipeline Job Processor Service
 * BullMQ workers for pipeline-jobs, incremental-sync, and polling-checks queues.
 * Replaces RabbitMQ job handlers. Uses Redis pub/sub for real-time status (handled by gateway).
 */

import { Processor, WorkerHost, OnWorkerEvent } from '@nestjs/bullmq';
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { Job } from 'bullmq';
import { normalizeEtlBaseUrl } from '../../../common/utils/etl-url';
import { PipelineService } from './pipeline.service';
import { PipelineRepository } from '../repositories/pipeline.repository';
import { PythonETLService } from './python-etl.service';
import {
  PipelineQueueService,
  FullSyncJobData,
  IncrementalSyncJobData,
  DeltaCheckJobData,
} from '../../queue/pipeline-queue.service';
import { QUEUE_NAMES } from '../../queue/bullmq.module';

@Injectable()
@Processor(QUEUE_NAMES.PIPELINE_JOBS)
export class PipelineJobsProcessor extends WorkerHost {
  private readonly logger = new Logger(PipelineJobsProcessor.name);

  constructor(
    private readonly pipelineService: PipelineService,
    private readonly pipelineRepository: PipelineRepository,
    private readonly pipelineQueueService: PipelineQueueService,
  ) {
    super();
  }

  async process(job: Job<FullSyncJobData, unknown, string>): Promise<void> {
    const data = job.data;
    const { pipelineId, organizationId, userId, triggerType, batchSize } = data;

    this.logger.log(`[FULL-SYNC] Starting job for pipeline ${pipelineId}`);

    try {
      const pipeline = await this.pipelineRepository.findById(pipelineId);
      if (!pipeline) throw new Error(`Pipeline ${pipelineId} not found`);
      if (pipeline.status === 'running') {
        this.logger.warn(`[FULL-SYNC] Pipeline ${pipelineId} is already running, skipping`);
        return;
      }

      await this.pipelineService.runPipeline(pipelineId, userId || 'system', triggerType, {
        batchSize: batchSize || 500,
      });

      this.logger.log(`[FULL-SYNC] Completed job for pipeline ${pipelineId}`);
      await this.pipelineQueueService.publishStatusUpdate({
        pipelineId,
        organizationId,
        status: 'completed',
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error);
      this.logger.error(`[FULL-SYNC] Job failed: ${errorMsg}`);
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
  ) {
    super();
  }

  async process(job: Job<IncrementalSyncJobData, unknown, string>): Promise<void> {
    const data = job.data;
    const { pipelineId, organizationId, userId, triggerType, batchSize } = data;

    this.logger.log(`[INCREMENTAL-SYNC] Starting job for pipeline ${pipelineId}`);

    try {
      const pipeline = await this.pipelineRepository.findById(pipelineId);
      if (!pipeline) throw new Error(`Pipeline ${pipelineId} not found`);
      if (!['listing', 'idle', 'completed'].includes(pipeline.status || '')) {
        this.logger.warn(
          `[INCREMENTAL-SYNC] Pipeline ${pipelineId} is in ${pipeline.status} status, skipping`,
        );
        return;
      }

      await this.pipelineService.runPipeline(
        pipelineId,
        userId || pipeline.createdBy || 'system',
        triggerType === 'polling' ? 'polling' : triggerType === 'resume' ? 'manual' : 'scheduled',
        { batchSize: batchSize || 500 },
      );

      this.logger.log(`[INCREMENTAL-SYNC] Completed job for pipeline ${pipelineId}`);
      await this.pipelineQueueService.publishStatusUpdate({
        pipelineId,
        organizationId,
        status: 'listing',
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error);
      this.logger.error(`[INCREMENTAL-SYNC] Job failed: ${errorMsg}`);
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
  private pythonServiceUrl: string;

  constructor(
    private readonly pipelineRepository: PipelineRepository,
    private readonly pipelineQueueService: PipelineQueueService,
    private readonly pythonETLService: PythonETLService,
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
  ) {
    super();
    this.pythonServiceUrl = normalizeEtlBaseUrl(
      this.configService.get<string>('ETL_PYTHON_SERVICE_URL') ??
        this.configService.get<string>('PYTHON_SERVICE_URL'),
    );
    if (!this.pythonServiceUrl) {
      throw new Error(
        'ETL_PYTHON_SERVICE_URL or PYTHON_SERVICE_URL must be set in environment (e.g. in apps/api/.env)',
      );
    }
  }

  async onModuleInit(): Promise<void> {
    await this.schedulePollCycle();
  }

  /**
   * Schedule repeatable job: every 5 min enqueue delta-check jobs for all active pipelines.
   */
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
      this.logger.log('CDC polling scheduled (every 5 minutes)');
    } catch (error) {
      this.logger.error(`Failed to schedule poll-cycle: ${error}`);
    }
  }

  async process(job: Job<DeltaCheckJobData | Record<string, never>, unknown, string>): Promise<void> {
    if (job.name === 'poll-cycle') {
      await this.runPollCycle();
      return;
    }

    const data = job.data as DeltaCheckJobData;
    const { pipelineId, organizationId } = data;

    this.logger.debug(`[DELTA-CHECK] Checking pipeline ${pipelineId} for changes`);

    try {
      const pipeline = await this.pipelineRepository.findById(pipelineId);
      if (!pipeline) {
        this.logger.warn(`[DELTA-CHECK] Pipeline ${pipelineId} not found`);
        return;
      }
      if (pipeline.status !== 'listing') {
        this.logger.debug(
          `[DELTA-CHECK] Pipeline ${pipelineId} not eligible (status: ${pipeline.status})`,
        );
        return;
      }

      const hasChanges = await this.checkForChanges(pipelineId, pipeline);
      if (hasChanges) {
        this.logger.log(
          `[DELTA-CHECK] Changes detected for pipeline ${pipelineId}, enqueuing incremental sync`,
        );
        const checkpoint = (pipeline.checkpoint as Record<string, unknown>) || {};
        await this.pipelineQueueService.enqueueIncrementalSync({
          pipelineId,
          organizationId,
          userId: pipeline.createdBy || 'system',
          triggerType: 'polling',
          checkpoint: checkpoint as IncrementalSyncJobData['checkpoint'],
          batchSize: 500,
        });
      }
    } catch (error) {
      this.logger.error(`[DELTA-CHECK] Error checking pipeline ${pipelineId}: ${error}`);
    }
  }

  private async runPollCycle(): Promise<void> {
    try {
      const activePipelines = await this.pipelineRepository.findActivePipelinesForPolling();
      if (activePipelines.length === 0) return;

      this.logger.log(`[POLLING] Found ${activePipelines.length} pipeline(s) to check`);
      for (const pipeline of activePipelines) {
        await this.pipelineQueueService.enqueueDeltaCheck({
          pipelineId: pipeline.id,
          organizationId: pipeline.organizationId,
        });
      }
    } catch (error) {
      this.logger.error(`[POLLING] Error during poll cycle: ${error}`);
    }
  }

  private async checkForChanges(
    pipelineId: string,
    pipeline: { checkpoint?: unknown },
    pipelineWithSchema?: Awaited<ReturnType<PipelineRepository['findByIdForCDC']>>,
  ): Promise<boolean> {
    try {
      if (!pipelineWithSchema) {
        pipelineWithSchema = await this.pipelineRepository.findByIdForCDC(pipelineId);
      }
      if (!pipelineWithSchema?.sourceSchema) return false;

      const connectionConfig = await this.pythonETLService.getConnectionConfig(
        pipelineWithSchema.sourceSchema,
        pipelineWithSchema.organizationId,
      );
      const checkpoint = (pipeline.checkpoint as Record<string, unknown>) || {};
      const sourceType = this.normalizeSourceType(pipelineWithSchema.sourceSchema.sourceType);

      const response = await firstValueFrom(
        this.httpService.post(
          `${this.pythonServiceUrl}/delta-check/${sourceType}`,
          {
            connection_config: connectionConfig,
            source_config: pipelineWithSchema.sourceSchema.sourceConfig || {},
            table_name: pipelineWithSchema.sourceSchema.sourceTable,
            schema_name: pipelineWithSchema.sourceSchema.sourceSchema,
            checkpoint,
          },
          { timeout: 30000 },
        ),
      );
      return response.data?.has_changes ?? false;
    } catch (error) {
      this.logger.warn(`[DELTA-CHECK] Error checking for changes: ${error}`);
      return false;
    }
  }

  private normalizeSourceType(sourceType: string): string {
    const normalized = sourceType.toLowerCase();
    return normalized === 'postgres' ? 'postgresql' : normalized;
  }

  @OnWorkerEvent('failed')
  onFailed(job: Job | undefined, error: Error): void {
    if (job) {
      this.logger.warn(`[POLLING-CHECKS] Job ${job.id} failed: ${error.message}`);
    }
  }
}
