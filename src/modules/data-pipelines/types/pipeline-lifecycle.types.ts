/**
 * Pipeline Lifecycle Types
 * Defines the pipeline status lifecycle and checkpoint types
 */

/**
 * Pipeline execution status
 * Represents the current state of a pipeline in its lifecycle
 */
export enum PipelineStatus {
  /** Pipeline is idle, no active runs */
  IDLE = 'idle',

  /** Pipeline is initializing (connecting to sources, validating config) */
  INITIALIZING = 'initializing',

  /** Active full or incremental migration in progress */
  RUNNING = 'running',

  /** Finished migration, now polling/listing for changes (polling mode) */
  LISTING = 'listing',

  /** Real-time listening mode (CDC, webhooks, triggers) for new changes */
  LISTENING = 'listening',

  /** Pipeline is paused by user */
  PAUSED = 'paused',

  /** Pipeline encountered an error */
  FAILED = 'failed',

  /** One-time run finished successfully */
  COMPLETED = 'completed',
}

/**
 * Sync mode for the pipeline
 */
export enum SyncMode {
  /** Full sync - replaces all data */
  FULL = 'full',

  /** Incremental sync - only new/changed data since last checkpoint */
  INCREMENTAL = 'incremental',

  /** CDC mode - real-time change data capture */
  CDC = 'cdc',
}

/**
 * Checkpoint data for resumable syncs
 * Stores source-specific state for incremental processing
 */
export interface PipelineCheckpoint {
  /** Last sync timestamp */
  lastSyncAt?: string;

  /** Last synced value (for incremental column) */
  lastSyncValue?: string | number;

  /** Watermark field name (incremental column) */
  watermarkField?: string;

  /** Timestamp when pipeline was paused (for delta calculation on resume) */
  pauseTimestamp?: string;

  /** For cursor-based pagination */
  cursor?: string;

  /** For offset-based pagination */
  offset?: number;

  /** For PostgreSQL WAL-based CDC */
  walPosition?: string;
  
  /** LSN (Log Sequence Number) - alias for walPosition */
  lsn?: string;
  
  /** Replication slot name for WAL CDC */
  slotName?: string;
  
  /** Publication name for WAL CDC */
  publicationName?: string;

  /** For MongoDB change streams */
  changeStreamToken?: string;

  /** For MySQL binlog */
  binlogPosition?: {
    file: string;
    position: number;
  };

  /** For API pagination */
  pageToken?: string;

  /** Last processed ID */
  lastId?: string;

  /** Total rows processed in last run */
  rowsProcessed?: number;

  /** Total rows in source (if known) */
  totalRows?: number;

  /** Current batch number */
  currentBatch?: number;

  /** Total batches expected */
  totalBatches?: number;

  /** Custom source-specific data */
  metadata?: Record<string, unknown>;
}

/**
 * Pipeline run progress info
 */
export interface PipelineProgress {
  /** Current phase of execution */
  phase: 'initializing' | 'collecting' | 'transforming' | 'emitting' | 'finalizing' | 'complete';

  /** Current batch being processed */
  currentBatch: number;

  /** Total batches expected (if known) */
  totalBatches?: number;

  /** Rows processed so far */
  rowsProcessed: number;

  /** Total rows expected (if known) */
  rowsTotal?: number;

  /** Progress percentage (0-100) */
  percentage?: number;

  /** Human-readable status message */
  message: string;

  /** Time elapsed in seconds */
  elapsedSeconds?: number;

  /** Estimated time remaining in seconds */
  estimatedRemainingSeconds?: number;
}

/**
 * Result of a pipeline lifecycle transition
 */
export interface StatusTransitionResult {
  success: boolean;
  previousStatus: PipelineStatus;
  newStatus: PipelineStatus;
  message: string;
  checkpoint?: PipelineCheckpoint;
}

/**
 * Polling configuration for LISTING mode
 */
export interface PollingConfig {
  /** Interval in seconds between polls */
  intervalSeconds: number;

  /** Maximum records to fetch per poll */
  batchSize: number;

  /** Whether to process immediately or queue */
  processImmediately: boolean;

  /** Backoff multiplier on empty polls */
  emptyPollBackoffMultiplier?: number;

  /** Maximum backoff interval in seconds */
  maxBackoffSeconds?: number;
}

/**
 * CDC listener configuration for LISTENING mode
 */
export interface CDCListenerConfig {
  /** Type of CDC listener */
  type: 'postgres_wal' | 'mysql_binlog' | 'mongodb_change_stream' | 'webhook' | 'trigger';

  /** Whether to batch changes before processing */
  batchChanges: boolean;

  /** Batch window in milliseconds */
  batchWindowMs?: number;

  /** Maximum batch size before flush */
  maxBatchSize?: number;

  /** Whether to acknowledge changes immediately */
  autoAcknowledge: boolean;

  /** Custom listener-specific configuration */
  config?: Record<string, unknown>;
}

/**
 * Valid status transitions
 * Defines which status changes are allowed
 */
export const VALID_STATUS_TRANSITIONS: Record<PipelineStatus, PipelineStatus[]> = {
  [PipelineStatus.IDLE]: [
    PipelineStatus.INITIALIZING,
    PipelineStatus.RUNNING,
    PipelineStatus.PAUSED,
  ],
  [PipelineStatus.INITIALIZING]: [
    PipelineStatus.RUNNING,
    PipelineStatus.FAILED,
    PipelineStatus.PAUSED,
    PipelineStatus.IDLE,
  ],
  [PipelineStatus.RUNNING]: [
    PipelineStatus.LISTING,
    PipelineStatus.LISTENING,
    PipelineStatus.COMPLETED,
    PipelineStatus.FAILED,
    PipelineStatus.PAUSED,
    PipelineStatus.IDLE,
  ],
  [PipelineStatus.LISTING]: [
    PipelineStatus.RUNNING,
    PipelineStatus.LISTENING,
    PipelineStatus.PAUSED,
    PipelineStatus.FAILED,
    PipelineStatus.IDLE,
    PipelineStatus.COMPLETED,
  ],
  [PipelineStatus.LISTENING]: [
    PipelineStatus.RUNNING,
    PipelineStatus.PAUSED,
    PipelineStatus.FAILED,
    PipelineStatus.IDLE,
  ],
  [PipelineStatus.PAUSED]: [
    PipelineStatus.RUNNING,
    PipelineStatus.LISTING,
    PipelineStatus.LISTENING,
    PipelineStatus.IDLE,
  ],
  [PipelineStatus.FAILED]: [
    PipelineStatus.IDLE,
    PipelineStatus.INITIALIZING,
    PipelineStatus.RUNNING,
    PipelineStatus.PAUSED,
  ],
  [PipelineStatus.COMPLETED]: [
    PipelineStatus.IDLE,
    PipelineStatus.INITIALIZING,
    PipelineStatus.RUNNING,
  ],
};

/**
 * Check if a status transition is valid
 */
export function isValidStatusTransition(from: PipelineStatus, to: PipelineStatus): boolean {
  return VALID_STATUS_TRANSITIONS[from]?.includes(to) ?? false;
}

/**
 * Get human-readable status description
 */
export function getStatusDescription(status: PipelineStatus): string {
  const descriptions: Record<PipelineStatus, string> = {
    [PipelineStatus.IDLE]: 'Pipeline is idle and ready to run',
    [PipelineStatus.INITIALIZING]: 'Pipeline is initializing and validating configuration',
    [PipelineStatus.RUNNING]: 'Pipeline is actively syncing data',
    [PipelineStatus.LISTING]: 'Pipeline is polling for new changes',
    [PipelineStatus.LISTENING]: 'Pipeline is listening for real-time changes',
    [PipelineStatus.PAUSED]: 'Pipeline is paused',
    [PipelineStatus.FAILED]: 'Pipeline encountered an error',
    [PipelineStatus.COMPLETED]: 'Pipeline run completed successfully',
  };
  return descriptions[status];
}
