/**
 * BullMQ Module
 * Provides BullMQ + Redis queue integration for pipeline jobs and real-time events.
 * Uses REDIS_URL from environment for connection.
 */

import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { BullModule } from '@nestjs/bullmq';
import { PipelineQueueService } from './pipeline-queue.service';

export const QUEUE_NAMES = {
  PIPELINE_JOBS: 'pipeline-jobs',
  INCREMENTAL_SYNC: 'incremental-sync',
  POLLING_CHECKS: 'polling-checks',
} as const;

export const REDIS_PUBSUB_CHANNEL = 'pipeline-updates';

@Module({
  imports: [
    BullModule.forRootAsync({
      imports: [ConfigModule],
      useFactory: (configService: ConfigService) => {
        const redisUrl = configService.get<string>('REDIS_URL') || 'redis://localhost:6379';
        try {
          const url = new URL(redisUrl);
          return {
            connection: {
              host: url.hostname,
              port: parseInt(url.port || '6379', 10),
              username: url.username || undefined,
              password: url.password || undefined,
            },
          };
        } catch {
          return { connection: { host: 'localhost', port: 6379 } };
        }
      },
      inject: [ConfigService],
    }),
    BullModule.registerQueue(
      { name: QUEUE_NAMES.PIPELINE_JOBS },
      { name: QUEUE_NAMES.INCREMENTAL_SYNC },
      { name: QUEUE_NAMES.POLLING_CHECKS },
    ),
  ],
  providers: [PipelineQueueService],
  exports: [BullModule, PipelineQueueService],
})
export class BullmqModule {}
