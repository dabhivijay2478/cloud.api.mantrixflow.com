/**
 * Pipeline Queue Processor
 * Example processor for handling pipeline execution jobs
 */

import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { PgBossService } from '../pgboss.service';
import { QUEUE_NAMES } from '../pgboss.constants';
import type { PipelineJobData, JobData } from '../pgboss.interfaces';
import type { Job } from 'pg-boss';

@Injectable()
export class PipelineQueueProcessor implements OnModuleInit {
  private readonly logger = new Logger(PipelineQueueProcessor.name);

  constructor(
    private readonly pgBossService: PgBossService,
  ) {}

  /**
   * Register workers when module initializes
   */
  async onModuleInit(): Promise<void> {
    // Register pipeline execution worker
    await this.pgBossService.work<PipelineJobData>(
      QUEUE_NAMES.PIPELINE_EXECUTION,
      async (job) => this.processPipelineExecution(job),
      {
        batchSize: 1,
        includeMetadata: true,
      },
    );

    // Register scheduled pipeline worker
    await this.pgBossService.work<PipelineJobData>(
      QUEUE_NAMES.PIPELINE_SCHEDULED,
      async (job) => this.processScheduledPipeline(job),
      {
        batchSize: 1,
        includeMetadata: true,
      },
    );

    this.logger.log('Pipeline queue processors registered');
  }

  /**
   * Process pipeline execution job
   */
  private async processPipelineExecution(
    job: Job<JobData<PipelineJobData>>,
  ): Promise<{ success: boolean }> {
    const { payload } = job.data;
    const { pipelineId, triggerType } = payload;

    this.logger.log(`Processing pipeline: ${pipelineId} (trigger: ${triggerType})`);

    try {
      // Execute the pipeline
      // await this.pipelineService.executePipeline({
      //   pipelineId,
      //   organizationId,
      //   userId,
      //   options,
      // });

      this.logger.log(`Pipeline ${pipelineId} executed successfully`);

      return { success: true };
    } catch (error) {
      this.logger.error(
        `Pipeline ${pipelineId} execution failed: ${error instanceof Error ? error.message : error}`,
        error instanceof Error ? error.stack : undefined,
      );
      throw error;
    }
  }

  /**
   * Process scheduled pipeline job
   */
  private async processScheduledPipeline(
    job: Job<JobData<PipelineJobData>>,
  ): Promise<{ success: boolean }> {
    const { payload } = job.data;
    const { pipelineId } = payload;

    this.logger.log(`Processing scheduled pipeline: ${pipelineId}`);

    try {
      return { success: true };
    } catch (error) {
      this.logger.error(`Scheduled pipeline ${pipelineId} failed`, error);
      throw error;
    }
  }

  /**
   * Schedule a pipeline for immediate execution
   */
  async schedulePipelineExecution(data: PipelineJobData): Promise<string | null> {
    return this.pgBossService.sendWithContext(
      QUEUE_NAMES.PIPELINE_EXECUTION,
      data,
      {
        organizationId: data.organizationId,
        userId: data.userId,
      },
    );
  }

  /**
   * Schedule a pipeline with delay
   */
  async schedulePipelineDelayed(
    data: PipelineJobData,
    delaySeconds: number,
  ): Promise<string | null> {
    return this.pgBossService.sendDelayed(
      QUEUE_NAMES.PIPELINE_EXECUTION,
      data,
      delaySeconds,
    );
  }

  /**
   * Schedule a recurring pipeline using cron expression
   */
  async schedulePipelineCron(
    pipelineId: string,
    cronExpression: string,
    organizationId: string,
    userId: string,
  ): Promise<void> {
    const scheduleName = `pipeline:${pipelineId}`;
    const jobData: PipelineJobData = {
      pipelineId,
      organizationId,
      userId,
      triggerType: 'scheduled',
    };

    await this.pgBossService.schedule(scheduleName, {
      cron: cronExpression,
      timezone: 'UTC',
      data: jobData as unknown as Record<string, unknown>,
    });

    // Also register a worker for this specific schedule
    await this.pgBossService.work<PipelineJobData>(
      scheduleName,
      async (job) => this.processPipelineExecution(job),
      { batchSize: 1 },
    );
  }

  /**
   * Cancel a scheduled pipeline
   */
  async cancelScheduledPipeline(pipelineId: string): Promise<void> {
    await this.pgBossService.unschedule(`pipeline:${pipelineId}`);
  }

  /**
   * Schedule a singleton pipeline (prevent duplicates)
   */
  async schedulePipelineSingleton(data: PipelineJobData): Promise<string | null> {
    return this.pgBossService.sendSingleton(
      QUEUE_NAMES.PIPELINE_EXECUTION,
      data,
      `pipeline:${data.pipelineId}`,
      { singletonSeconds: 300 },
    );
  }
}
