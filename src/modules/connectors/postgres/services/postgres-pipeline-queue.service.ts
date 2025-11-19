/**
 * PostgreSQL Pipeline Queue Service
 * Manages job queue for scheduled pipeline executions
 */

import { Injectable, Logger } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import { Cron, CronExpression } from '@nestjs/schedule';
import { PostgresPipelineRepository } from '../repositories/postgres-pipeline.repository';
import { PipelineJobData } from '../jobs/postgres-pipeline.processor';

@Injectable()
export class PostgresPipelineQueueService {
    private readonly logger = new Logger(PostgresPipelineQueueService.name);

    constructor(
        @InjectQueue('postgres-pipeline')
        private readonly pipelineQueue: Queue<PipelineJobData>,
        private readonly pipelineRepository: PostgresPipelineRepository,
    ) { }

    /**
     * Add pipeline execution job to queue
     */
    async addPipelineJob(
        pipelineId: string,
        triggeredBy?: string,
        triggerType: 'manual' | 'scheduled' | 'webhook' = 'manual',
        metadata?: Record<string, any>,
    ): Promise<string> {
        const job = await this.pipelineQueue.add(
            'execute-pipeline',
            {
                pipelineId,
                triggeredBy,
                triggerType,
                metadata,
            },
            {
                attempts: 3,
                backoff: {
                    type: 'exponential',
                    delay: 5000,
                },
                removeOnComplete: 100, // Keep last 100 completed jobs
                removeOnFail: 500, // Keep last 500 failed jobs
            },
        );

        this.logger.log(
            `Added pipeline job ${job.id} for pipeline ${pipelineId}`,
        );

        return job.id!;
    }

    /**
     * Schedule recurring pipeline job
     */
    async schedulePipeline(
        pipelineId: string,
        frequency: string,
    ): Promise<void> {
        const cronPattern = this.frequencyToCron(frequency);

        if (!cronPattern) {
            this.logger.warn(
                `Invalid frequency ${frequency} for pipeline ${pipelineId}`,
            );
            return;
        }

        await this.pipelineQueue.add(
            'execute-pipeline',
            {
                pipelineId,
                triggerType: 'scheduled',
            },
            {
                repeat: {
                    pattern: cronPattern,
                },
                jobId: `scheduled-${pipelineId}`, // Unique ID to prevent duplicates
            },
        );

        this.logger.log(
            `Scheduled pipeline ${pipelineId} with frequency ${frequency}`,
        );
    }

    /**
     * Remove scheduled pipeline job
     */
    async unschedulePipeline(pipelineId: string): Promise<void> {
        const jobId = `scheduled-${pipelineId}`;
        const job = await this.pipelineQueue.getJob(jobId);

        if (job) {
            await job.remove();
            this.logger.log(`Removed scheduled job for pipeline ${pipelineId}`);
        }
    }

    /**
     * Get job status
     */
    async getJobStatus(jobId: string): Promise<any> {
        const job = await this.pipelineQueue.getJob(jobId);

        if (!job) {
            return null;
        }

        const state = await job.getState();
        const progress = job.progress;

        return {
            id: job.id,
            state,
            progress,
            data: job.data,
            attemptsMade: job.attemptsMade,
            finishedOn: job.finishedOn,
            processedOn: job.processedOn,
        };
    }

    /**
     * Cron job to check and schedule pipelines
     * Runs every 5 minutes
     */
    @Cron(CronExpression.EVERY_5_MINUTES)
    async checkScheduledPipelines(): Promise<void> {
        this.logger.debug('Checking for pipelines that need scheduling...');

        // This would query pipelines that have syncFrequency set
        // and ensure they are scheduled in the queue
        // Implementation depends on your specific requirements
    }

    /**
     * Convert frequency string to cron pattern
     */
    private frequencyToCron(frequency: string): string | null {
        const frequencyMap: Record<string, string> = {
            '5min': '*/5 * * * *',
            '15min': '*/15 * * * *',
            '30min': '*/30 * * * *',
            '1hour': '0 * * * *',
            '6hours': '0 */6 * * *',
            '12hours': '0 */12 * * *',
            '24hours': '0 0 * * *',
            daily: '0 0 * * *',
            weekly: '0 0 * * 0',
            monthly: '0 0 1 * *',
        };

        return frequencyMap[frequency] || null;
    }

    /**
     * Get queue metrics
     */
    async getQueueMetrics(): Promise<{
        waiting: number;
        active: number;
        completed: number;
        failed: number;
        delayed: number;
    }> {
        const [waiting, active, completed, failed, delayed] = await Promise.all([
            this.pipelineQueue.getWaitingCount(),
            this.pipelineQueue.getActiveCount(),
            this.pipelineQueue.getCompletedCount(),
            this.pipelineQueue.getFailedCount(),
            this.pipelineQueue.getDelayedCount(),
        ]);

        return {
            waiting,
            active,
            completed,
            failed,
            delayed,
        };
    }

    /**
     * Clean old jobs
     */
    async cleanOldJobs(
        grace: number = 24 * 60 * 60 * 1000,
    ): Promise<void> {
        await this.pipelineQueue.clean(grace, 100, 'completed');
        await this.pipelineQueue.clean(grace, 100, 'failed');

        this.logger.log('Cleaned old jobs from queue');
    }
}
