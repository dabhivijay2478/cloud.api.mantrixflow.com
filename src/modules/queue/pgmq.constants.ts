/**
 * PgMQ queue constants shared by PgmqModule and PgmqQueueService.
 * Replaces BullMQ/Redis with Supabase-native pgmq (Postgres message queue) and pg_cron.
 *
 * @see https://github.com/tembo-io/pgmq
 * @see https://github.com/citusdata/pg_cron
 */

/** Queue names for pgmq (underscores required for valid Postgres identifiers) */
export const PGMQ_QUEUE_NAMES = {
  PIPELINE_JOBS: 'pipeline_jobs',
  INCREMENTAL_SYNC: 'incremental_sync',
  POLLING_CHECKS: 'polling_checks',
} as const;

/** How often NestJS polls pgmq queues for new messages (ms) */
export const PGMQ_POLL_INTERVAL_MS = 2_000;

/** Max number of pipeline jobs to process in parallel per poll cycle.
 * Override via PGMQ_PARALLEL_WORKERS env. Default 50 for K8s scale (200-500 concurrent). */
export const PGMQ_PARALLEL_WORKERS =
  (typeof process !== 'undefined' && process.env?.PGMQ_PARALLEL_WORKERS
    ? parseInt(process.env.PGMQ_PARALLEL_WORKERS, 10)
    : NaN) || 50;

/** Base backoff delay for requeuing when ETL pod returns 503 (ms) */
export const PGMQ_REQUEUE_BACKOFF_BASE_MS = 5_000;

/** Maximum number of dispatch retries before marking run as failed */
export const PGMQ_MAX_DISPATCH_RETRIES = 10;

/** Visibility timeout for long-running pipeline / sync jobs (seconds) */
export const PGMQ_VT_LONG_SEC = 14_400; // 4 hours

/** Visibility timeout for short-lived polling-check jobs (seconds) */
export const PGMQ_VT_SHORT_SEC = 300; // 5 minutes

/** Maximum retry attempts for job processing */
export const PGMQ_MAX_RETRIES = 5;

/** pg_cron job name for the CDC poll cycle */
export const PGCRON_CDC_POLL_JOB = 'pgmq_cdc_poll_cycle';

/** pg_cron schedule for CDC poll cycle (every 5 minutes) */
export const PGCRON_CDC_POLL_SCHEDULE = '*/5 * * * *';

/** Postgres NOTIFY channel for transient pipeline job-status updates */
export const PG_NOTIFY_PIPELINE_STATUS = 'pipeline_job_status';

/** Job payload types for pgmq messages */
export interface FullSyncJobData {
  pipelineId: string;
  runId: string;
  organizationId: string;
  userId: string;
  triggerType: string;
  batchSize?: number;
}

export interface IncrementalSyncJobData {
  pipelineId: string;
  runId: string;
  organizationId: string;
  userId: string;
  triggerType: string;
  checkpoint?: Record<string, unknown>;
  batchSize?: number;
}

export interface DeltaCheckJobData {
  pipelineId: string;
  organizationId: string;
}
