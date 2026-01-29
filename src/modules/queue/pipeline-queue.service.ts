/**
 * Pipeline Queue Service
 * Enqueues pipeline jobs via BullMQ and publishes real-time status via Redis pub/sub.
 */

import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import Redis from 'ioredis';
import { QUEUE_NAMES, REDIS_PUBSUB_CHANNEL } from './bullmq.module';

export interface FullSyncJobData {
  pipelineId: string;
  organizationId: string;
  userId: string;
  triggerType: 'manual' | 'scheduled';
  batchSize?: number;
}

export interface IncrementalSyncJobData {
  pipelineId: string;
  organizationId: string;
  userId: string;
  triggerType: 'polling' | 'manual' | 'resume';
  checkpoint: {
    watermarkField?: string;
    lastValue?: string | number;
    walPosition?: string;
    lsn?: string;
    slotName?: string;
    publicationName?: string;
    pauseTimestamp?: string;
  };
  batchSize?: number;
}

export interface DeltaCheckJobData {
  pipelineId: string;
  organizationId: string;
}

export interface StatusUpdateEventData {
  pipelineId: string;
  organizationId: string;
  status: string;
  rowsProcessed?: number;
  newRowsCount?: number;
  error?: string;
  timestamp: string;
}

@Injectable()
export class PipelineQueueService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PipelineQueueService.name);
  private redisPub: Redis | null = null;

  constructor(
    private readonly configService: ConfigService,
    @InjectQueue(QUEUE_NAMES.PIPELINE_JOBS) private readonly pipelineJobsQueue: Queue,
    @InjectQueue(QUEUE_NAMES.INCREMENTAL_SYNC) private readonly incrementalSyncQueue: Queue,
    @InjectQueue(QUEUE_NAMES.POLLING_CHECKS) private readonly pollingChecksQueue: Queue,
  ) {}

  async onModuleInit(): Promise<void> {
    const redisUrl = this.configService.get<string>('REDIS_URL') || 'redis://localhost:6379';
    try {
      this.redisPub = new Redis(redisUrl, { maxRetriesPerRequest: null });
      this.redisPub.on('error', (err) => this.logger.error(`Redis pub error: ${err.message}`));
      this.logger.log('Pipeline queue service (BullMQ + Redis pub/sub) ready');
    } catch (error) {
      this.logger.error(`Failed to connect Redis for pub/sub: ${error}`);
    }
  }

  async onModuleDestroy(): Promise<void> {
    if (this.redisPub) {
      await this.redisPub.quit();
      this.redisPub = null;
    }
  }

  isReady(): boolean {
    return this.redisPub?.status === 'ready';
  }

  async enqueueFullSync(data: FullSyncJobData): Promise<void> {
    await this.pipelineJobsQueue.add('full-sync', data, {
      priority: data.triggerType === 'manual' ? 1 : 5,
      attempts: 3,
      backoff: { type: 'exponential', delay: 2000 },
    });
    this.logger.debug(`Enqueued full sync job for pipeline ${data.pipelineId}`);
  }

  async enqueueIncrementalSync(data: IncrementalSyncJobData): Promise<void> {
    await this.incrementalSyncQueue.add('incremental-sync', data, {
      priority: data.triggerType === 'manual' ? 1 : 5,
      attempts: 3,
      backoff: { type: 'exponential', delay: 2000 },
    });
    this.logger.debug(`Enqueued incremental sync job for pipeline ${data.pipelineId}`);
  }

  async enqueueDeltaCheck(data: DeltaCheckJobData): Promise<void> {
    await this.pollingChecksQueue.add('delta-check', data, {
      attempts: 1,
      removeOnComplete: { count: 1000 },
    });
    this.logger.debug(`Enqueued delta check job for pipeline ${data.pipelineId}`);
  }

  /**
   * Publish status update to Redis channel for Socket.io gateway to forward to clients.
   */
  async publishStatusUpdate(data: StatusUpdateEventData): Promise<void> {
    if (!this.redisPub || this.redisPub.status !== 'ready') return;
    try {
      await this.redisPub.publish(REDIS_PUBSUB_CHANNEL, JSON.stringify(data));
      this.logger.debug(`Published status update for pipeline ${data.pipelineId}`);
    } catch (error) {
      this.logger.warn(`Failed to publish status update: ${error}`);
    }
  }

  /**
   * Schedule a delayed delta check (e.g. for next poll).
   */
  async scheduleDelayedDeltaCheck(data: DeltaCheckJobData, delayMs: number): Promise<void> {
    await this.pollingChecksQueue.add('delta-check', data, {
      delay: delayMs,
      attempts: 1,
      removeOnComplete: { count: 1000 },
    });
  }

  getPipelineJobsQueue(): Queue {
    return this.pipelineJobsQueue;
  }

  getIncrementalSyncQueue(): Queue {
    return this.incrementalSyncQueue;
  }

  getPollingChecksQueue(): Queue {
    return this.pollingChecksQueue;
  }
}
