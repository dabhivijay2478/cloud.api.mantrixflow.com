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
  ETL_JOBS: 'etl_jobs',
  ETL_JOBS_DLQ: 'etl_jobs_dlq',
} as const;

/** How often NestJS polls pgmq queues for new messages (ms) */
export const PGMQ_POLL_INTERVAL_MS = 2_000;

/** Max number of pipeline jobs to process in parallel per poll cycle (real-time parallel execution) */
export const PGMQ_PARALLEL_WORKERS = 5;

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
