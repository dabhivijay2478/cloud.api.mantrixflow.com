/**
 * Pipeline Lifecycle Service
 * Manages pipeline status transitions, checkpoints, and execution phases
 */

import { Injectable, Logger, NotFoundException, BadRequestException } from '@nestjs/common';
import { PipelineRepository } from '../repositories/pipeline.repository';
import { ActivityLogService } from '../../activity-logs/activity-log.service';
import { PIPELINE_ACTIONS } from '../../activity-logs/constants/activity-log-types';
import {
  PipelineStatus,
  PipelineCheckpoint,
  PipelineProgress,
  StatusTransitionResult,
  isValidStatusTransition,
  getStatusDescription,
} from '../types/pipeline-lifecycle.types';

/**
 * Lifecycle log entry for detailed tracking
 */

@Injectable()
export class PipelineLifecycleService {
  private readonly logger = new Logger(PipelineLifecycleService.name);

  constructor(
    private readonly pipelineRepository: PipelineRepository,
    private readonly activityLogService: ActivityLogService,
  ) {}

  // ============================================================================
  // STATUS TRANSITIONS
  // ============================================================================

  /**
   * Transition pipeline to a new status with validation and logging
   * @param force - If true, skip transition validation (use for run starts where we need to override stuck states)
   */
  async transitionStatus(
    pipelineId: string,
    newStatus: PipelineStatus,
    userId: string | null,
    message?: string,
    metadata?: Record<string, unknown>,
    force: boolean = false,
  ): Promise<StatusTransitionResult> {
    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    const currentStatus = (pipeline.status as PipelineStatus) || PipelineStatus.IDLE;

    // Skip validation if force mode or if status is already the target (no-op)
    if (!force && currentStatus !== newStatus) {
      if (!isValidStatusTransition(currentStatus, newStatus)) {
        throw new BadRequestException(
          `Invalid status transition from ${currentStatus} to ${newStatus}`,
        );
      }
    }

    // If already in target status and not forcing, just return success without update
    if (currentStatus === newStatus && !force) {
      this.logger.log(`Pipeline ${pipelineId} already in ${newStatus} status, skipping update`);
      return {
        success: true,
        previousStatus: currentStatus,
        newStatus,
        message: message || getStatusDescription(newStatus),
        checkpoint: pipeline.checkpoint as PipelineCheckpoint | undefined,
      };
    }

    // Update status
    await this.pipelineRepository.update(pipelineId, {
      status: newStatus,
      ...(newStatus === PipelineStatus.FAILED ? { lastError: message } : {}),
    });

    // Log to console
    const logMessage = `🔄 Pipeline ${pipeline.name} (${pipelineId}): ${currentStatus} → ${newStatus}${message ? ` - ${message}` : ''}`;
    this.logger.log(logMessage);
    console.log(logMessage);

    // Log to activity
    await this.activityLogService.logPipelineAction(
      pipeline.organizationId,
      userId,
      PIPELINE_ACTIONS.STATUS_CHANGED,
      pipelineId,
      pipeline.name,
      {
        previousStatus: currentStatus,
        newStatus,
        description: getStatusDescription(newStatus),
        message,
        forced: force,
        ...metadata,
      },
    );

    return {
      success: true,
      previousStatus: currentStatus,
      newStatus,
      message: message || getStatusDescription(newStatus),
      checkpoint: pipeline.checkpoint as PipelineCheckpoint | undefined,
    };
  }

  /**
   * Set pipeline to INITIALIZING state
   */
  async initializePipeline(pipelineId: string, userId: string): Promise<StatusTransitionResult> {
    const message = 'Pipeline is initializing, validating configuration...';
    console.log(`\n${'='.repeat(60)}`);
    console.log(`🚀 INITIALIZING PIPELINE`);
    console.log(`${'='.repeat(60)}`);

    return this.transitionStatus(pipelineId, PipelineStatus.INITIALIZING, userId, message);
  }

  /**
   * Set pipeline to RUNNING state
   */
  async startRunning(
    pipelineId: string,
    userId: string | null,
    isFullSync: boolean,
    totalRows?: number,
  ): Promise<StatusTransitionResult> {
    const syncType = isFullSync ? 'full sync' : 'incremental sync';
    const message = `Starting ${syncType}${totalRows ? ` (${totalRows.toLocaleString()} rows expected)` : ''}`;

    console.log(`\n📊 ${message.toUpperCase()}`);

    const result = await this.transitionStatus(
      pipelineId,
      PipelineStatus.RUNNING,
      userId,
      message,
      { syncType: isFullSync ? 'full' : 'incremental', totalRows },
      true, // Force transition for run starts
    );

    // Log specific sync start
    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (pipeline) {
      await this.activityLogService.logPipelineAction(
        pipeline.organizationId,
        userId,
        isFullSync ? PIPELINE_ACTIONS.FULL_SYNC_STARTED : PIPELINE_ACTIONS.INCREMENTAL_SYNC_STARTED,
        pipelineId,
        pipeline.name,
        { totalRows },
      );
    }

    return result;
  }

  /**
   * Set pipeline to LISTING (polling) mode
   */
  async enterListingMode(
    pipelineId: string,
    userId: string | null,
    pollingIntervalSeconds: number = 300,
  ): Promise<StatusTransitionResult> {
    const message = `Entering LISTING mode, polling every ${pollingIntervalSeconds}s for changes`;

    console.log(`\n👁️ ${message}`);

    await this.pipelineRepository.update(pipelineId, {
      pollingIntervalSeconds,
    });

    const result = await this.transitionStatus(
      pipelineId,
      PipelineStatus.LISTING,
      userId,
      message,
      { pollingIntervalSeconds },
    );

    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (pipeline) {
      await this.activityLogService.logPipelineAction(
        pipeline.organizationId,
        userId,
        PIPELINE_ACTIONS.LISTING_STARTED,
        pipelineId,
        pipeline.name,
        { pollingIntervalSeconds },
      );
    }

    return result;
  }

  /**
   * Set pipeline to LISTENING (CDC) mode
   */
  async enterListeningMode(
    pipelineId: string,
    userId: string | null,
    cdcType: string,
  ): Promise<StatusTransitionResult> {
    const message = `Entering LISTENING mode with ${cdcType} CDC`;

    console.log(`\n📡 ${message}`);

    const result = await this.transitionStatus(
      pipelineId,
      PipelineStatus.LISTENING,
      userId,
      message,
      { cdcType },
    );

    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (pipeline) {
      await this.activityLogService.logPipelineAction(
        pipeline.organizationId,
        userId,
        PIPELINE_ACTIONS.LISTENING_STARTED,
        pipelineId,
        pipeline.name,
        { cdcType },
      );
    }

    return result;
  }

  /**
   * Mark pipeline as completed
   */
  async markCompleted(
    pipelineId: string,
    userId: string | null,
    stats: { rowsProcessed: number; durationSeconds: number },
  ): Promise<StatusTransitionResult> {
    const message = `Sync completed: ${stats.rowsProcessed.toLocaleString()} rows in ${stats.durationSeconds}s`;

    console.log(`\n✅ ${message}`);
    console.log(`${'='.repeat(60)}\n`);

    return this.transitionStatus(pipelineId, PipelineStatus.COMPLETED, userId, message, stats);
  }

  /**
   * Mark pipeline as failed
   */
  async markFailed(
    pipelineId: string,
    userId: string | null,
    error: Error | string,
    partialStats?: { rowsProcessed?: number },
  ): Promise<StatusTransitionResult> {
    const errorMessage = error instanceof Error ? error.message : error;
    const message = `Pipeline failed: ${errorMessage}${partialStats?.rowsProcessed ? ` (${partialStats.rowsProcessed} rows processed before failure)` : ''}`;

    console.error(`\n❌ ${message}`);
    console.log(`${'='.repeat(60)}\n`);

    return this.transitionStatus(pipelineId, PipelineStatus.FAILED, userId, message, {
      error: errorMessage,
      ...partialStats,
    });
  }

  // ============================================================================
  // CHECKPOINT MANAGEMENT
  // ============================================================================

  /**
   * Save checkpoint for resumable syncs
   */
  async saveCheckpoint(
    pipelineId: string,
    checkpoint: PipelineCheckpoint,
    userId: string | null,
  ): Promise<void> {
    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    await this.pipelineRepository.update(pipelineId, {
      checkpoint,
      lastSyncAt: new Date(),
      lastSyncValue: checkpoint.lastSyncValue?.toString(),
    });

    const logMessage = `💾 Checkpoint saved: ${checkpoint.rowsProcessed?.toLocaleString() || 0} rows processed`;
    this.logger.log(logMessage);
    console.log(logMessage);

    await this.activityLogService.logPipelineAction(
      pipeline.organizationId,
      userId,
      PIPELINE_ACTIONS.CHECKPOINT_SAVED,
      pipelineId,
      pipeline.name,
      {
        rowsProcessed: checkpoint.rowsProcessed,
        lastSyncValue: checkpoint.lastSyncValue,
        cursor: checkpoint.cursor,
      },
    );
  }

  /**
   * Get current checkpoint
   */
  async getCheckpoint(pipelineId: string): Promise<PipelineCheckpoint | null> {
    const pipeline = await this.pipelineRepository.findById(pipelineId);
    return (pipeline?.checkpoint as PipelineCheckpoint) || null;
  }

  /**
   * Clear checkpoint (for fresh full sync)
   */
  async clearCheckpoint(pipelineId: string, userId: string | null): Promise<void> {
    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    await this.pipelineRepository.update(pipelineId, {
      checkpoint: null,
      lastSyncValue: null,
    });

    this.logger.log(`Checkpoint cleared for pipeline ${pipelineId}`);

    await this.activityLogService.logPipelineAction(
      pipeline.organizationId,
      userId,
      PIPELINE_ACTIONS.CHECKPOINT_SAVED,
      pipelineId,
      pipeline.name,
      { action: 'cleared' },
    );
  }

  // ============================================================================
  // PROGRESS TRACKING
  // ============================================================================

  /**
   * Log batch progress with console output
   */
  async logBatchProgress(
    pipelineId: string,
    runId: string,
    progress: PipelineProgress,
    userId: string | null,
  ): Promise<void> {
    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (!pipeline) return;

    // Calculate progress bar
    const progressBar = this.createProgressBar(progress.percentage || 0);
    const statsLine = `Batch ${progress.currentBatch}${progress.totalBatches ? `/${progress.totalBatches}` : ''} | ${progress.rowsProcessed.toLocaleString()}${progress.rowsTotal ? `/${progress.rowsTotal.toLocaleString()}` : ''} rows`;

    // Console output
    console.log(`📦 ${progressBar} ${statsLine}`);
    console.log(`   └─ ${progress.message}`);

    // Log to activity for significant milestones (every 10% or every 5000 rows)
    const shouldLogActivity =
      (progress.percentage && progress.percentage % 10 === 0) ||
      progress.rowsProcessed % 5000 === 0;

    if (shouldLogActivity) {
      await this.activityLogService.logPipelineRunAction(
        pipeline.organizationId,
        userId,
        PIPELINE_ACTIONS.BATCH_COMPLETED,
        runId,
        pipelineId,
        pipeline.name,
        {
          phase: progress.phase,
          currentBatch: progress.currentBatch,
          totalBatches: progress.totalBatches,
          rowsProcessed: progress.rowsProcessed,
          rowsTotal: progress.rowsTotal,
          percentage: progress.percentage,
          message: progress.message,
        },
      );
    }

    // Update run with progress
    await this.pipelineRepository.updateRun(runId, {
      rowsRead: progress.rowsProcessed,
    });
  }

  /**
   * Create a visual progress bar
   */
  private createProgressBar(percentage: number, width: number = 20): string {
    const filled = Math.round((percentage / 100) * width);
    const empty = width - filled;
    const bar = '█'.repeat(filled) + '░'.repeat(empty);
    return `[${bar}] ${percentage.toFixed(1)}%`;
  }

  /**
   * Log sync completion summary
   */
  async logSyncSummary(
    pipelineId: string,
    runId: string,
    userId: string | null,
    stats: {
      syncType: 'full' | 'incremental';
      rowsRead: number;
      rowsWritten: number;
      rowsSkipped: number;
      rowsFailed: number;
      durationSeconds: number;
      batchCount: number;
    },
  ): Promise<void> {
    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (!pipeline) return;

    // Console summary
    console.log(`\n${'─'.repeat(50)}`);
    console.log(`📊 SYNC SUMMARY - ${stats.syncType.toUpperCase()}`);
    console.log(`${'─'.repeat(50)}`);
    console.log(`   Pipeline: ${pipeline.name}`);
    console.log(`   Rows Read:    ${stats.rowsRead.toLocaleString()}`);
    console.log(`   Rows Written: ${stats.rowsWritten.toLocaleString()}`);
    console.log(`   Rows Skipped: ${stats.rowsSkipped.toLocaleString()}`);
    console.log(`   Rows Failed:  ${stats.rowsFailed.toLocaleString()}`);
    console.log(`   Batches:      ${stats.batchCount}`);
    console.log(`   Duration:     ${stats.durationSeconds}s`);
    console.log(
      `   Rate:         ${(stats.rowsWritten / Math.max(stats.durationSeconds, 1)).toFixed(0)} rows/sec`,
    );
    console.log(`${'─'.repeat(50)}\n`);

    // Log to activity
    await this.activityLogService.logPipelineRunAction(
      pipeline.organizationId,
      userId,
      stats.syncType === 'full'
        ? PIPELINE_ACTIONS.FULL_SYNC_COMPLETED
        : PIPELINE_ACTIONS.INCREMENTAL_SYNC_COMPLETED,
      runId,
      pipelineId,
      pipeline.name,
      {
        ...stats,
        rate: stats.rowsWritten / Math.max(stats.durationSeconds, 1),
      },
    );
  }

  // ============================================================================
  // POLLING & CHANGE DETECTION
  // ============================================================================

  /**
   * Log a poll execution
   */
  async logPollExecution(
    pipelineId: string,
    userId: string | null,
    result: {
      changesDetected: number;
      nextPollAt: Date;
    },
  ): Promise<void> {
    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (!pipeline) return;

    if (result.changesDetected > 0) {
      console.log(
        `🔍 Poll executed: ${result.changesDetected} changes detected, triggering sync...`,
      );
    } else {
      console.log(
        `🔍 Poll executed: No changes detected, next poll at ${result.nextPollAt.toISOString()}`,
      );
    }

    await this.activityLogService.logPipelineAction(
      pipeline.organizationId,
      userId,
      PIPELINE_ACTIONS.POLL_EXECUTED,
      pipelineId,
      pipeline.name,
      result,
    );
  }

  /**
   * Log CDC change detection
   */
  async logChangeDetected(
    pipelineId: string,
    userId: string | null,
    changeInfo: {
      changeType: 'insert' | 'update' | 'delete';
      recordCount: number;
      source: string;
    },
  ): Promise<void> {
    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (!pipeline) return;

    console.log(
      `📡 CDC: ${changeInfo.recordCount} ${changeInfo.changeType}(s) detected from ${changeInfo.source}`,
    );

    await this.activityLogService.logPipelineAction(
      pipeline.organizationId,
      userId,
      PIPELINE_ACTIONS.CHANGE_DETECTED,
      pipelineId,
      pipeline.name,
      changeInfo,
    );
  }

  // ============================================================================
  // UTILITY METHODS
  // ============================================================================

  /**
   * Determine if pipeline should use CDC based on source type
   */
  isCDCSupported(sourceType: string): boolean {
    const cdcSupportedTypes = ['postgres', 'mysql', 'mongodb'];
    return cdcSupportedTypes.includes(sourceType.toLowerCase());
  }

  /**
   * Get recommended post-sync mode based on source type and sync mode
   */
  getRecommendedPostSyncMode(
    sourceType: string,
    syncMode: string,
  ): PipelineStatus.LISTING | PipelineStatus.LISTENING | PipelineStatus.COMPLETED {
    // For manual/one-time syncs, just complete
    if (syncMode === 'full') {
      return PipelineStatus.COMPLETED;
    }

    // For CDC-capable sources with incremental mode, use listening
    if (this.isCDCSupported(sourceType) && syncMode === 'cdc') {
      return PipelineStatus.LISTENING;
    }

    // For incremental without CDC, use polling
    if (syncMode === 'incremental') {
      return PipelineStatus.LISTING;
    }

    return PipelineStatus.COMPLETED;
  }
}
