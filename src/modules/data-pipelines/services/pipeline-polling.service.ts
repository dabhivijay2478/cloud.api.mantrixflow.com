/**
 * Pipeline Polling Service
 * Consumes incremental sync jobs from PGMQ queue
 * 
 * Architecture:
 * - pg_cron calls pipeline_polling_function() every 1 minute
 * - Function checks active pipelines and enqueues jobs to PGMQ 'incremental-jobs' queue
 * - This service consumes from 'incremental-jobs' queue and processes them
 * 
 * ROOT FIX: Automated polling after full sync triggers incremental syncs
 */

import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { PGMQService } from './pgmq.service';
import { PipelineService } from './pipeline.service';
import { PipelineRepository } from '../repositories/pipeline.repository';

interface IncrementalJobMessage {
  pipeline_id: string;
  organization_id: string;
  trigger_type: 'polling' | 'manual';
  checkpoint: {
    watermarkField?: string;
    lastSyncValue?: string | number;
    pauseTimestamp?: string;
  };
  watermark_field: string;
  last_sync_value: string | number;
  created_at: string;
}

@Injectable()
export class PipelinePollingService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PipelinePollingService.name);
  private pollingInterval: NodeJS.Timeout | null = null;
  private isProcessing = false;
  private readonly POLL_INTERVAL_MS = 5000; // Poll every 5 seconds
  private readonly VISIBILITY_TIMEOUT = 300; // 5 minutes visibility timeout

  constructor(
    private readonly pgmqService: PGMQService,
    private readonly pipelineService: PipelineService,
    private readonly pipelineRepository: PipelineRepository,
  ) {}

  async onModuleInit() {
    this.logger.log('Starting Pipeline Polling Service...');
    
    // Initialize the incremental-jobs queue (will be created on first send if it doesn't exist)
    try {
      await this.pgmqService.initializeQueue('incremental-jobs');
      this.logger.log('PGMQ queue initialized: incremental-jobs');
    } catch (error) {
      this.logger.warn(`Failed to initialize PGMQ queue: ${error}`);
      // Continue anyway - queue will be created on first send
    }
    
    // Start polling for incremental sync jobs
    this.startPolling();
    
    this.logger.log('Pipeline Polling Service started');
  }

  async onModuleDestroy() {
    this.logger.log('Stopping Pipeline Polling Service...');
    
    if (this.pollingInterval) {
      clearInterval(this.pollingInterval);
      this.pollingInterval = null;
    }
    
    this.logger.log('Pipeline Polling Service stopped');
  }

  /**
   * Start polling for incremental sync jobs from PGMQ
   */
  private startPolling() {
    this.pollingInterval = setInterval(async () => {
      if (this.isProcessing) {
        return; // Skip if already processing
      }

      await this.processJobs();
    }, this.POLL_INTERVAL_MS);

    this.logger.log(`Started polling for incremental sync jobs (interval: ${this.POLL_INTERVAL_MS}ms)`);
  }

  /**
   * Process jobs from the incremental-jobs queue
   */
  private async processJobs() {
    if (this.isProcessing) {
      return;
    }

    this.isProcessing = true;

    try {
      // Read messages from queue (non-destructive)
      const messages = await this.pgmqService.read<IncrementalJobMessage>(
        'incremental-jobs',
        this.VISIBILITY_TIMEOUT,
        10, // Process up to 10 jobs at a time
      );

      if (messages.length === 0) {
        return; // No jobs to process
      }

      this.logger.log(`Processing ${messages.length} incremental sync job(s)`);

      // Process each job
      for (const msg of messages) {
        try {
          await this.processIncrementalJob(msg.message);
          
          // Archive message on success
          await this.pgmqService.archive('incremental-jobs', msg.msg_id);
          
          this.logger.log(`Successfully processed incremental sync job for pipeline ${msg.message.pipeline_id}`);
        } catch (error) {
          this.logger.error(
            `Failed to process incremental sync job for pipeline ${msg.message.pipeline_id}: ${error}`,
          );
          
          // Don't archive on error - let it retry (visibility timeout will make it available again)
          // Or archive to dead letter queue if retry limit exceeded
        }
      }
    } catch (error) {
      this.logger.error(`Error in job processing loop: ${error}`);
    } finally {
      this.isProcessing = false;
    }
  }

  /**
   * Process a single incremental sync job
   */
  private async processIncrementalJob(job: IncrementalJobMessage) {
    const { pipeline_id, organization_id, checkpoint, watermark_field, last_sync_value } = job;

    // Verify pipeline exists and is in LISTING status
    const pipeline = await this.pipelineRepository.findById(pipeline_id);
    if (!pipeline) {
      throw new Error(`Pipeline ${pipeline_id} not found`);
    }

    if (pipeline.status !== 'listing') {
      this.logger.warn(
        `Pipeline ${pipeline_id} is not in LISTING status (current: ${pipeline.status}). Skipping.`,
      );
      return;
    }

    if (pipeline.organizationId !== organization_id) {
      throw new Error(`Pipeline ${pipeline_id} does not belong to organization ${organization_id}`);
    }

    // Run the pipeline with incremental sync
    // Use a system user ID or the pipeline creator
    const systemUserId = pipeline.createdBy || 'system';

    this.logger.log(
      `Running incremental sync for pipeline ${pipeline_id} (watermark: ${watermark_field} > ${last_sync_value})`,
    );

    // Trigger pipeline run - it will use collectIncremental automatically
    await this.pipelineService.runPipeline(
      pipeline_id,
      systemUserId,
      'polling', // triggerType
    );
  }

  /**
   * Manually trigger polling check (for testing)
   */
  async triggerPollingCheck(): Promise<void> {
    this.logger.log('Manually triggering polling check');
    await this.processJobs();
  }
}
