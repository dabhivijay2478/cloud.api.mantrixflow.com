/**
 * PostgreSQL Pipeline Job Processor
 * Handles scheduled and background pipeline executions using BullMQ
 */

import { OnWorkerEvent, Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import { PostgresPipelineService } from '../../postgres-pipeline.service';

export interface PipelineJobData {
  pipelineId: string;
  triggeredBy?: string;
  triggerType: 'manual' | 'scheduled' | 'webhook';
  metadata?: Record<string, any>;
}

@Processor('postgres-pipeline')
export class PostgresPipelineProcessor extends WorkerHost {
  private readonly logger = new Logger(PostgresPipelineProcessor.name);

  constructor(private readonly pipelineService: PostgresPipelineService) {
    super();
  }

  /**
   * Process pipeline execution job
   */
  async process(job: Job<PipelineJobData>): Promise<any> {
    const { pipelineId, triggeredBy, triggerType, metadata } = job.data;

    this.logger.log(`Processing pipeline job ${job.id} for pipeline ${pipelineId}`);

    try {
      // Update job progress
      await job.updateProgress(10);

      // Execute the pipeline
      const result = await this.pipelineService.executePipeline(pipelineId);

      // Update job progress
      await job.updateProgress(100);

      this.logger.log(
        `Pipeline ${pipelineId} executed successfully. Rows written: ${result.rowsWritten}`,
      );

      return {
        success: true,
        pipelineId,
        runId: result.runId,
        rowsWritten: result.rowsWritten,
        status: result.status,
        completedAt: new Date(),
      };
    } catch (error) {
      this.logger.error(`Pipeline ${pipelineId} execution failed: ${error.message}`, error.stack);

      throw error; // BullMQ will handle retries
    }
  }

  /**
   * Handle job completion
   */
  @OnWorkerEvent('completed')
  onCompleted(job: Job<PipelineJobData>, _result: any) {
    this.logger.log(
      `Pipeline job ${job.id} completed successfully for pipeline ${job.data.pipelineId}`,
    );
  }

  /**
   * Handle job failure
   */
  @OnWorkerEvent('failed')
  onFailed(job: Job<PipelineJobData>, error: Error) {
    this.logger.error(
      `Pipeline job ${job.id} failed for pipeline ${job.data.pipelineId}: ${error.message}`,
    );
  }

  /**
   * Handle job progress
   */
  @OnWorkerEvent('progress')
  onProgress(job: Job<PipelineJobData>, progress: number) {
    this.logger.debug(
      `Pipeline job ${job.id} progress: ${progress}% for pipeline ${job.data.pipelineId}`,
    );
  }

  /**
   * Handle job active
   */
  @OnWorkerEvent('active')
  onActive(job: Job<PipelineJobData>) {
    this.logger.log(`Pipeline job ${job.id} started for pipeline ${job.data.pipelineId}`);
  }
}
