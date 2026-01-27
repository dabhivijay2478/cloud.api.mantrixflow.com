/**
 * RabbitMQ Job Handler Service
 * Registers and handles all RabbitMQ jobs for pipeline operations
 *
 * Job Types:
 * - full-sync: Complete data sync from source to destination
 * - incremental-sync: Sync only new/changed records (CDC)
 * - delta-check: Poll source for changes and enqueue incremental if needed
 * - status-update: Pub/sub for real-time UI updates
 */

import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { PipelineService } from './pipeline.service';
import { PipelineRepository } from '../repositories/pipeline.repository';
import { PythonETLService } from './python-etl.service';
import { PipelineUpdatesGateway } from '../gateways/pipeline-updates.gateway';
import {
  RabbitMQService,
  QUEUE_NAMES,
  FullSyncJobData,
  IncrementalSyncJobData,
  DeltaCheckJobData,
  StatusUpdateEventData,
} from '../../queue/rabbitmq.service';
import { getEtlServiceUrl } from '../../../common/config/etl-url.util';

@Injectable()
export class RabbitMQJobHandlerService implements OnModuleInit {
  private readonly logger = new Logger(RabbitMQJobHandlerService.name);

  constructor(
    private readonly rabbitmqService: RabbitMQService,
    private readonly pipelineService: PipelineService,
    private readonly pipelineRepository: PipelineRepository,
    private readonly pythonETLService: PythonETLService,
    private readonly pipelineUpdatesGateway: PipelineUpdatesGateway,
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
  ) {
    this.pythonServiceUrl = getEtlServiceUrl(this.configService);
  }

  private readonly pythonServiceUrl: string;

  /**
   * Register all job handlers on module init
   */
  async onModuleInit(): Promise<void> {
    this.logger.log('Registering RabbitMQ job handlers...');

    // Wait for RabbitMQ to be ready
    let retries = 0;
    while (!this.rabbitmqService.isReady() && retries < 10) {
      await new Promise((resolve) => setTimeout(resolve, 1000));
      retries++;
    }

    if (!this.rabbitmqService.isReady()) {
      this.logger.warn('RabbitMQ not ready, job handlers will be registered when ready');
      setTimeout(() => this.registerHandlers(), 5000);
      return;
    }

    await this.registerHandlers();
  }

  /**
   * Register all handlers
   */
  private async registerHandlers(): Promise<void> {
    try {
      // Register queue consumers
      await this.rabbitmqService.consumeQueue<FullSyncJobData>(
        QUEUE_NAMES.FULL_SYNC,
        this.handleFullSync.bind(this),
      );

      await this.rabbitmqService.consumeQueue<IncrementalSyncJobData>(
        QUEUE_NAMES.INCREMENTAL_SYNC,
        this.handleIncrementalSync.bind(this),
      );

      await this.rabbitmqService.consumeQueue<DeltaCheckJobData>(
        QUEUE_NAMES.DELTA_CHECK,
        this.handleDeltaCheck.bind(this),
      );

      // Subscribe to status updates
      await this.rabbitmqService.subscribeToStatusUpdates(
        'pipeline.*.status',
        this.handleStatusUpdate.bind(this),
      );

      // Setup polling consumer (runs every 1-5 minutes)
      await this.setupPollingConsumer();

      this.logger.log('✅ All RabbitMQ job handlers registered successfully');
    } catch (error) {
      this.logger.error(`Failed to register job handlers: ${error}`);
    }
  }

  // ============================================================================
  // JOB HANDLERS
  // ============================================================================

  /**
   * Handle full sync job
   */
  private async handleFullSync(
    data: FullSyncJobData,
    ack: () => void,
    nack: () => void,
  ): Promise<void> {
    const { pipelineId, organizationId, userId, triggerType, batchSize } = data;

    this.logger.log(`[FULL-SYNC] Starting job for pipeline ${pipelineId}`);

    try {
      const pipeline = await this.pipelineRepository.findById(pipelineId);
      if (!pipeline) {
        throw new Error(`Pipeline ${pipelineId} not found`);
      }

      if (pipeline.status === 'running') {
        this.logger.warn(`[FULL-SYNC] Pipeline ${pipelineId} is already running, skipping`);
        ack();
        return;
      }

      await this.pipelineService.runPipeline(pipelineId, userId || 'system', triggerType, {
        batchSize: batchSize || 500,
      });

      this.logger.log(`[FULL-SYNC] Completed job for pipeline ${pipelineId}`);
      ack();

      await this.rabbitmqService.publishStatusUpdate({
        pipelineId,
        organizationId,
        status: 'completed',
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error);
      this.logger.error(`[FULL-SYNC] Job failed: ${errorMsg}`);
      nack(); // Requeue for retry

      await this.rabbitmqService.publishStatusUpdate({
        pipelineId,
        organizationId,
        status: 'failed',
        error: errorMsg,
        timestamp: new Date().toISOString(),
      });
    }
  }

  /**
   * Handle incremental sync job
   */
  private async handleIncrementalSync(
    data: IncrementalSyncJobData,
    ack: () => void,
    nack: () => void,
  ): Promise<void> {
    const { pipelineId, organizationId, userId, triggerType, batchSize } = data;

    this.logger.log(`[INCREMENTAL-SYNC] Starting job for pipeline ${pipelineId}`);

    try {
      const pipeline = await this.pipelineRepository.findById(pipelineId);
      if (!pipeline) {
        throw new Error(`Pipeline ${pipelineId} not found`);
      }

      if (!['listing', 'idle', 'completed'].includes(pipeline.status || '')) {
        this.logger.warn(
          `[INCREMENTAL-SYNC] Pipeline ${pipelineId} is in ${pipeline.status} status, skipping`,
        );
        ack();
        return;
      }

      await this.pipelineService.runPipeline(
        pipelineId,
        userId || pipeline.createdBy || 'system',
        triggerType === 'polling' ? 'polling' : triggerType === 'resume' ? 'manual' : 'scheduled',
        {
          batchSize: batchSize || 500,
        },
      );

      this.logger.log(`[INCREMENTAL-SYNC] Completed job for pipeline ${pipelineId}`);
      ack();

      await this.rabbitmqService.publishStatusUpdate({
        pipelineId,
        organizationId,
        status: 'listing',
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error);
      this.logger.error(`[INCREMENTAL-SYNC] Job failed: ${errorMsg}`);
      nack();

      await this.rabbitmqService.publishStatusUpdate({
        pipelineId,
        organizationId,
        status: 'failed',
        error: errorMsg,
        timestamp: new Date().toISOString(),
      });
    }
  }

  /**
   * Handle delta check job
   * Polls source database for new records and enqueues incremental sync if found
   */
  private async handleDeltaCheck(
    data: DeltaCheckJobData,
    ack: () => void,
    _nack: () => void,
  ): Promise<void> {
    const { pipelineId, organizationId } = data;

    this.logger.debug(`[DELTA-CHECK] Checking pipeline ${pipelineId} for changes`);

    try {
      const pipeline = await this.pipelineRepository.findById(pipelineId);
      if (!pipeline) {
        this.logger.warn(`[DELTA-CHECK] Pipeline ${pipelineId} not found`);
        ack();
        return;
      }

      // Only check pipelines in LISTING status (CDC mode)
      if (pipeline.status !== 'listing') {
        this.logger.debug(
          `[DELTA-CHECK] Pipeline ${pipelineId} not eligible (status: ${pipeline.status})`,
        );
        ack();
        return;
      }

      // Call Python delta-check endpoint - Python handles all CDC/incremental detection
      // Python returns whether changes were detected
      const hasChanges = await this.checkForChanges(pipelineId, pipeline);

      if (hasChanges) {
        this.logger.log(
          `[DELTA-CHECK] Changes detected for pipeline ${pipelineId}, enqueuing incremental sync`,
        );

        // Pass current checkpoint to Python - Python handles all checkpoint management
        const checkpoint = (pipeline.checkpoint as any) || {};
        await this.rabbitmqService.enqueueIncrementalSync({
          pipelineId,
          organizationId,
          userId: pipeline.createdBy || 'system',
          triggerType: 'polling',
          checkpoint: checkpoint, // Pass full checkpoint - Python handles all CDC logic
          batchSize: 500,
        });
      } else {
        this.logger.debug(`[DELTA-CHECK] No changes detected for pipeline ${pipelineId}`);
      }

      ack();
    } catch (error) {
      this.logger.error(`[DELTA-CHECK] Error checking pipeline ${pipelineId}: ${error}`);
      // Don't requeue - delta checks should fail silently and retry on next interval
      ack();
    }
  }

  /**
   * Handle status update pub/sub event
   */
  private async handleStatusUpdate(data: StatusUpdateEventData): Promise<void> {
    this.logger.debug(`[STATUS-UPDATE] Forwarding update for pipeline ${data.pipelineId}`);

    // Forward to WebSocket gateway
    this.pipelineUpdatesGateway.server?.to(`pipeline_${data.pipelineId}`).emit('update', {
      type: 'pipeline',
      pipeline_id: data.pipelineId,
      organization_id: data.organizationId,
      status: data.status,
      total_rows_processed: data.rowsProcessed,
      new_rows_count: data.newRowsCount,
      error: data.error,
      updated_at: data.timestamp,
    });

    if (data.organizationId) {
      this.pipelineUpdatesGateway.server?.to(`org_${data.organizationId}`).emit('pipeline_update', {
        pipeline_id: data.pipelineId,
        status: data.status,
        total_rows_processed: data.rowsProcessed,
        new_rows_count: data.newRowsCount,
        updated_at: data.timestamp,
      });
    }
  }

  /**
   * Setup polling consumer
   * Checks all active pipelines for changes every 1-5 minutes
   */
  private async setupPollingConsumer(): Promise<void> {
    // Poll every 2 minutes
    setInterval(
      async () => {
        try {
          const activePipelines = await this.pipelineRepository.findActivePipelinesForPolling();

          if (activePipelines.length === 0) {
            return;
          }

          this.logger.log(`[POLLING] Found ${activePipelines.length} pipeline(s) to check`);

          for (const pipeline of activePipelines) {
            await this.rabbitmqService.enqueueDeltaCheck({
              pipelineId: pipeline.id,
              organizationId: pipeline.organizationId,
            });
          }
        } catch (error) {
          this.logger.error(`[POLLING] Error during polling: ${error}`);
        }
      },
      2 * 60 * 1000,
    ); // 2 minutes

    this.logger.log('✅ Polling consumer setup (runs every 2 minutes)');
  }

  // ============================================================================
  // HELPER METHODS
  // ============================================================================

  /**
   * Check if source database has changes
   * Uses Python service delta-check endpoint
   */
  private async checkForChanges(
    pipelineId: string,
    pipeline: any,
    pipelineWithSchema?: any,
  ): Promise<boolean> {
    try {
      if (!pipelineWithSchema) {
        pipelineWithSchema = await this.pipelineRepository.findByIdForCDC(pipelineId);
      }

      if (!pipelineWithSchema || !pipelineWithSchema.sourceSchema) {
        return false;
      }

      // Get connection config
      const connectionConfig = await this.pythonETLService.getConnectionConfig(
        pipelineWithSchema.sourceSchema,
        pipelineWithSchema.organizationId,
      );

      const checkpoint = (pipeline.checkpoint as any) || {};

      // Call Python service delta-check endpoint
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
          {
            timeout: 30000,
          },
        ),
      );

      return response.data?.has_changes || false;
    } catch (error) {
      this.logger.warn(`[DELTA-CHECK] Error checking for changes: ${error}`);
      return false;
    }
  }

  /**
   * Normalize source type for Python service
   */
  private normalizeSourceType(sourceType: string): string {
    const normalized = sourceType.toLowerCase();
    if (normalized === 'postgres') {
      return 'postgresql';
    }
    return normalized;
  }
}
