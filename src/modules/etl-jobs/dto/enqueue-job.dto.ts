/**
 * DTO for enqueueing an ETL job
 */

export interface EnqueueJobDto {
  pipelineId: string;
  orgId: string;
  userId: string;
  syncMode?: 'full' | 'incremental';
  triggerType?: 'manual' | 'scheduled' | 'api' | 'polling';
}
