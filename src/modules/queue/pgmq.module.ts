/**
 * PgMQ Module
 * Provides Supabase-native pgmq queue integration for pipeline jobs and real-time events.
 * Replaces BullMQ + Redis with pgmq (durable Postgres message queue) and pg_cron (scheduled jobs).
 *
 * @see https://github.com/tembo-io/pgmq
 * @see https://github.com/citusdata/pg_cron
 */

import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { PgmqQueueService } from './pgmq-queue.service';

@Module({
  imports: [ConfigModule],
  providers: [PgmqQueueService],
  exports: [PgmqQueueService],
})
export class PgmqModule {}
