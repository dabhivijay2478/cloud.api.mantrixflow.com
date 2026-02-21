/**
 * DTO for ETL callback (FastAPI POSTs when meltano run completes)
 */

export interface EtlCallbackDto {
  jobId: string;
  pgmqMsgId: number;
  status: 'completed' | 'failed';
  rowsSynced?: number;
  stateId?: string;
  errorMessage?: string;
  userMessage?: string;
}
