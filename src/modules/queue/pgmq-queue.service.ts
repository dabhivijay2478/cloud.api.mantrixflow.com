/**
 * PgMQ Queue Service
 * Enqueues pipeline jobs via Supabase pgmq extension and publishes
 * real-time status updates via Postgres NOTIFY.
 *
 * Replaces BullMQ + Redis with Supabase-native pgmq (durable message queue)
 * and pg_cron (scheduled jobs).
 *
 * @see https://github.com/tembo-io/pgmq
 * @see https://github.com/citusdata/pg_cron
 */

import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Pool } from 'pg';
import {
  PGMQ_QUEUE_NAMES,
  PGCRON_CDC_POLL_JOB,
  PGCRON_CDC_POLL_SCHEDULE,
  PG_NOTIFY_PIPELINE_STATUS,
  type FullSyncJobData,
  type IncrementalSyncJobData,
  type DeltaCheckJobData,
} from './pgmq.constants';

export interface StatusUpdateEventData {
  pipelineId: string;
  organizationId: string;
  status: string;
  rowsProcessed?: number;
  newRowsCount?: number;
  error?: string;
  timestamp: string;
}

/** Payload wrapper stored in each pgmq message body */
export interface PgmqJobPayload<T = unknown> {
  name: string;
  data: T;
  retryCount: number;
  maxRetries: number;
}

/** Row returned by pgmq.read() */
export interface PgmqMessage<T = unknown> {
  msg_id: string;
  read_ct: number;
  enqueued_at: Date;
  vt: Date;
  message: T;
}

@Injectable()
export class PgmqQueueService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PgmqQueueService.name);
  private pool: Pool | null = null;
  private initialized = false;

  constructor(private readonly configService: ConfigService) {}

  async onModuleInit(): Promise<void> {
    // PgMQ/pg_cron need session mode. Prefer DATABASE_DIRECT_URL; else derive from DATABASE_URL (6543 → 5432).
    const explicitDirect = this.configService.get<string>('DATABASE_DIRECT_URL');
    const poolerUrl = this.configService.get<string>('DATABASE_URL');
    const databaseUrl =
      explicitDirect ||
      (poolerUrl?.includes(':6543') ? poolerUrl.replace(':6543', ':5432') : poolerUrl) ||
      null;
    if (!databaseUrl) {
      this.logger.error('DATABASE_URL (or DATABASE_DIRECT_URL) not set — pgmq queue service cannot start');
      return;
    }
    if (!explicitDirect && poolerUrl?.includes(':6543')) {
      this.logger.log(
        'Using session-mode URL derived from DATABASE_URL (port 5432). Set DATABASE_DIRECT_URL in .env to override.',
      );
    }
    try {
      this.pool = new Pool({ connectionString: databaseUrl, max: 3 });
      this.pool.on('error', (err) => this.logger.error(`PG pool error: ${err.message}`));
      this.logger.log('Ensuring pgmq extension...');
      await this.ensureExtension();
      this.logger.log('Ensuring queues exist...');
      await this.ensureQueuesExist();
      this.logger.log('Setting up pg_cron (optional)...');
      await this.setupCdcPollCron();
      this.initialized = true;
      this.logger.log('PgMQ queue service (pgmq + pg_cron) ready');
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error));
      this.logger.error(
        `Failed to initialise PgMQ queue service: ${err.message}. ` +
          'Ensure pgmq is enabled in Supabase Dashboard → Database → Extensions, and use a session-mode connection (DATABASE_DIRECT_URL, port 5432) for queue operations.',
      );
      if (err.stack) this.logger.debug(err.stack);
    }
  }

  async onModuleDestroy(): Promise<void> {
    if (this.pool) {
      await this.pool.end();
      this.pool = null;
    }
    this.initialized = false;
  }

  /** Whether the service is ready to accept queue operations. */
  isReady(): boolean {
    return this.initialized && this.pool !== null;
  }

  // ════════════════════════════════════════════════════════════════
  // EXTENSION & QUEUE BOOTSTRAP
  // ════════════════════════════════════════════════════════════════

  private async ensureExtension(): Promise<void> {
    try {
      await this.sql('CREATE EXTENSION IF NOT EXISTS pgmq');
      this.logger.log('pgmq extension enabled');
    } catch (error) {
      // Non-fatal: extension may already be enabled via Supabase Dashboard (Database → Extensions).
      this.logger.warn(
        `Could not run CREATE EXTENSION pgmq (continuing anyway): ${error}. ` +
          'If queues fail, enable pgmq in Supabase Dashboard → Database → Extensions.',
      );
    }
  }

  private async ensureQueuesExist(): Promise<void> {
    for (const name of Object.values(PGMQ_QUEUE_NAMES)) {
      try {
        await this.sql('SELECT pgmq.create($1)', [name]);
        this.logger.log(`pgmq queue "${name}" created`);
      } catch (error: any) {
        const msg = String(error?.message ?? error).toLowerCase();
        const alreadyExists =
          msg.includes('already exists') ||
          msg.includes('duplicate') ||
          (msg.includes('relation') && msg.includes('exists')) ||
          msg.includes('already a member of extension');
        if (alreadyExists) {
          this.logger.log(`pgmq queue "${name}" already exists (skipped)`);
        } else {
          throw error;
        }
      }
    }
  }

  // ════════════════════════════════════════════════════════════════
  // pg_cron: CDC POLL CYCLE (every 5 min)
  // ════════════════════════════════════════════════════════════════

  /**
   * Set up a pg_cron job that inserts a poll-cycle message into the
   * polling_checks queue every 5 minutes.
   */
  private async setupCdcPollCron(): Promise<void> {
    try {
      // Remove existing job (idempotent — ignore if not found)
      try {
        await this.sql('SELECT cron.unschedule($1)', [PGCRON_CDC_POLL_JOB]);
      } catch {
        /* job may not exist yet */
      }
      const cronBody =
        `SELECT pgmq.send('${PGMQ_QUEUE_NAMES.POLLING_CHECKS}', ` +
        `'{"name":"poll-cycle","data":{},"retryCount":0,"maxRetries":1}'::jsonb)`;
      await this.sql('SELECT cron.schedule($1, $2, $3)', [
        PGCRON_CDC_POLL_JOB,
        PGCRON_CDC_POLL_SCHEDULE,
        cronBody,
      ]);
      this.logger.log(
        `pg_cron job "${PGCRON_CDC_POLL_JOB}" scheduled: ${PGCRON_CDC_POLL_SCHEDULE}`,
      );
    } catch (error) {
      this.logger.warn(
        `Could not setup pg_cron for CDC polling (pg_cron may not be enabled): ${error}`,
      );
    }
  }

  // ════════════════════════════════════════════════════════════════
  // STATUS UPDATES (replaces Redis pub/sub)
  // ════════════════════════════════════════════════════════════════

  /**
   * Publish a transient pipeline status update via Postgres NOTIFY.
   * The Socket.io gateway LISTENs on this channel and forwards to clients.
   */
  async publishStatusUpdate(data: StatusUpdateEventData): Promise<void> {
    if (!this.isReady()) return;
    try {
      const payload = JSON.stringify(data);
      // Postgres NOTIFY payload limit is ~8 000 bytes
      const safePayload =
        payload.length > 7_900
          ? JSON.stringify({ ...data, error: data.error?.substring(0, 500) })
          : payload;
      await this.sql('SELECT pg_notify($1, $2)', [PG_NOTIFY_PIPELINE_STATUS, safePayload]);
      this.logger.debug(`Published status update for pipeline ${data.pipelineId}`);
    } catch (error) {
      this.logger.warn(`Failed to publish status update: ${error}`);
    }
  }

  // ════════════════════════════════════════════════════════════════
  // READ / ARCHIVE / RETRY — used by job processors
  // ════════════════════════════════════════════════════════════════

  /** Read up to `qty` messages from a pgmq queue with a given visibility timeout. */
  async readMessages<T = unknown>(
    queueName: string,
    qty: number = 1,
    vtSec: number = 300,
  ): Promise<PgmqMessage<T>[]> {
    if (!this.isReady()) return [];
    try {
      const result = await this.sql('SELECT * FROM pgmq.read($1, $2, $3)', [
        queueName,
        vtSec,
        qty,
      ]);
      return result.rows as PgmqMessage<T>[];
    } catch (error) {
      this.logger.error(`Failed to read from pgmq queue "${queueName}": ${error}`);
      return [];
    }
  }

  /** Archive a processed message (moved to the archive table for auditability). */
  async archiveMessage(queueName: string, msgId: string): Promise<void> {
    try {
      await this.sql('SELECT pgmq.archive($1, $2::bigint)', [queueName, msgId]);
    } catch (error) {
      this.logger.error(`Failed to archive msg ${msgId} from "${queueName}": ${error}`);
    }
  }

  /** Delete a message from the queue permanently. */
  async deleteMessage(queueName: string, msgId: string): Promise<void> {
    try {
      await this.sql('SELECT pgmq.delete($1, $2::bigint)', [queueName, msgId]);
    } catch (error) {
      this.logger.error(`Failed to delete msg ${msgId} from "${queueName}": ${error}`);
    }
  }

  /**
   * Re-enqueue a failed message with exponential backoff delay.
   * The original message should be archived before calling this.
   */
  async requeueWithBackoff(
    queueName: string,
    payload: PgmqJobPayload,
    currentRetry: number,
  ): Promise<void> {
    const delaySec = Math.min(2 ** currentRetry * 2, 300); // 2 s, 4 s, 8 s … max 5 min
    const retryPayload: PgmqJobPayload = { ...payload, retryCount: currentRetry + 1 };
    await this.sendWithDelay(queueName, retryPayload, delaySec);
    this.logger.debug(
      `Re-enqueued to "${queueName}" with ${delaySec}s delay (retry ${currentRetry + 1})`,
    );
  }

  // ════════════════════════════════════════════════════════════════
  // PIPELINE JOB ENQUEUE HELPERS
  // ════════════════════════════════════════════════════════════════

  async enqueueFullSync(data: FullSyncJobData): Promise<string> {
    const payload: PgmqJobPayload<FullSyncJobData> = {
      name: 'full-sync',
      data,
      retryCount: 0,
      maxRetries: 5,
    };
    return this.send(PGMQ_QUEUE_NAMES.PIPELINE_JOBS, payload);
  }

  async enqueueIncrementalSync(data: IncrementalSyncJobData): Promise<string> {
    const payload: PgmqJobPayload<IncrementalSyncJobData> = {
      name: 'incremental-sync',
      data,
      retryCount: 0,
      maxRetries: 5,
    };
    return this.send(PGMQ_QUEUE_NAMES.INCREMENTAL_SYNC, payload);
  }

  async enqueueDeltaCheck(data: DeltaCheckJobData): Promise<string> {
    const payload: PgmqJobPayload<DeltaCheckJobData> = {
      name: 'delta-check',
      data,
      retryCount: 0,
      maxRetries: 1,
    };
    return this.send(PGMQ_QUEUE_NAMES.POLLING_CHECKS, payload);
  }

  // ════════════════════════════════════════════════════════════════
  // LOW-LEVEL pgmq SQL HELPERS
  // ════════════════════════════════════════════════════════════════

  private async send(queueName: string, payload: PgmqJobPayload): Promise<string> {
    const result = await this.sql('SELECT * FROM pgmq.send($1, $2::jsonb)', [
      queueName,
      JSON.stringify(payload),
    ]);
    return result.rows[0]?.send;
  }

  private async sendWithDelay(
    queueName: string,
    payload: PgmqJobPayload,
    delaySec: number,
  ): Promise<string> {
    // pgmq.send(queue_name, msg, delay) — delay is third param (seconds)
    const result = await this.sql('SELECT * FROM pgmq.send($1, $2::jsonb, $3::integer)', [
      queueName,
      JSON.stringify(payload),
      delaySec,
    ]);
    return String(result.rows[0]?.send ?? '');
  }

  private async sql(text: string, params?: unknown[]) {
    if (!this.pool) throw new Error('PG pool not initialised');
    return this.pool.query(text, params);
  }
}
