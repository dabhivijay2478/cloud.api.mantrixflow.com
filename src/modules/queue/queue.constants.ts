/**
 * Queue constants shared by BullmqModule and PipelineQueueService.
 * Kept in a separate file to avoid circular dependency (bullmq.module ↔ pipeline-queue.service).
 */

export const QUEUE_NAMES = {
  PIPELINE_JOBS: 'pipeline-jobs',
  INCREMENTAL_SYNC: 'incremental-sync',
  POLLING_CHECKS: 'polling-checks',
} as const;

export const REDIS_PUBSUB_CHANNEL = 'pipeline-updates';
