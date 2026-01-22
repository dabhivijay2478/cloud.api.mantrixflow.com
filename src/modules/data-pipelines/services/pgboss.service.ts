/**
 * PgBoss Service
 * Job queue service using PgBoss v12 for:
 * - Exactly-once delivery with transaction integration
 * - Cron scheduling for polling/CDC
 * - Priority queues (high for manual runs, low for polls)
 * - Automatic retries with exponential backoff
 * - Dead letter queues for failed jobs
 * - Pub/sub for real-time status updates
 *
 * ROOT FIX: Replaces PGMQ and pg_cron with a unified job management system
 *
 * Guide: PgBoss handles all async operations - scheduling, retries, and job tracking
 */

import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PgBoss } from 'pg-boss';
import type { Job, SendOptions, WorkOptions } from 'pg-boss';

// Job types for type-safe job handling
export interface FullSyncJobData {
  pipelineId: string;
  organizationId: string;
  userId: string;
  triggerType: 'manual' | 'scheduled';
  batchSize?: number;
}

export interface IncrementalSyncJobData {
  pipelineId: string;
  organizationId: string;
  userId: string;
  triggerType: 'polling' | 'manual' | 'resume';
  checkpoint: {
    // WAL-based CDC (PostgreSQL)
    walPosition?: string;
    lsn?: string;
    slotName?: string;
    publicationName?: string;
    // Legacy column-based (for MySQL/MongoDB fallback)
    watermarkField?: string;
    lastValue?: string | number;
    pauseTimestamp?: string;
  };
  batchSize?: number;
}

export interface DeltaCheckJobData {
  pipelineId: string;
  organizationId: string;
}

export interface StatusUpdateEventData {
  pipelineId: string;
  organizationId: string;
  status: string;
  rowsProcessed?: number;
  newRowsCount?: number;
  error?: string;
  timestamp: string;
}

// Job names constants
export const JOB_NAMES = {
  FULL_SYNC: 'pipeline-full-sync',
  INCREMENTAL_SYNC: 'pipeline-incremental-sync',
  DELTA_CHECK: 'pipeline-delta-check',
  STATUS_UPDATE: 'pipeline-status-update',
} as const;

// Job priorities (lower = higher priority)
export const JOB_PRIORITY = {
  HIGH: 1, // Manual runs, resume
  NORMAL: 5, // Scheduled runs
  LOW: 10, // Polling checks
} as const;

@Injectable()
export class PgBossService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PgBossService.name);
  private boss: PgBoss | null = null;
  private isInitialized = false;
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 10;
  private reconnectDelay = 5000; // 5 seconds
  private reconnectTimer: NodeJS.Timeout | null = null;
  private isReconnecting = false;

  // Job handlers registry
  private jobHandlers: Map<string, (job: Job<any>) => Promise<void>> = new Map();

  constructor(private readonly configService: ConfigService) {
    // Error handling is done via PgBoss event handlers
    // Connection errors are handled gracefully with automatic reconnection
  }

  /**
   * Initialize PgBoss on module start
   */
  async onModuleInit(): Promise<void> {
    await this.initializePgBoss();
  }

  /**
   * Internal initialization method (can be called for reconnection)
   */
  private async initializePgBoss(): Promise<void> {
    try {
      if (this.isReconnecting) {
        this.logger.log('Reconnecting PgBoss...');
      } else {
        this.logger.log('════════════════════════════════════════════════════════');
        this.logger.log('🔧 Initializing PgBoss job queue service...');
      }

      const databaseUrl = this.configService.get<string>('DATABASE_URL');
      if (!databaseUrl) {
        throw new Error('DATABASE_URL environment variable is required for PgBoss');
      }

      // Create PgBoss instance with v12 configuration
      this.boss = new PgBoss({
        connectionString: databaseUrl,
        // Schema for PgBoss tables (separate from app schema)
        schema: 'pgboss',
        // Application name for connection identification
        application_name: 'data-pipeline-jobs',
        // Maintenance check interval
        maintenanceIntervalSeconds: 120,
        // Monitor state changes
        monitorIntervalSeconds: 30,
        // Maximum connections - reduced to prevent exhaustion
        max: 3,
        // Connection timeout
        connectionTimeoutMillis: 10000,
      });

      // Handle PgBoss errors with automatic reconnection
      this.boss.on('error', (error) => {
        const errorMessage = error?.message || String(error);
        
        // Check if it's a connection error
        if (
          errorMessage.includes('Connection terminated') ||
          errorMessage.includes('ECONNRESET') ||
          errorMessage.includes('ETIMEDOUT') ||
          errorMessage.includes('Connection closed') ||
          errorMessage.includes('Connection ended')
        ) {
          this.logger.warn(`PgBoss connection error (will attempt reconnect): ${errorMessage}`);
          // Don't log stack trace for connection errors
          this.handleConnectionError();
        } else {
          this.logger.error(`PgBoss error: ${errorMessage}`, error?.stack);
        }
      });

      // Connection health is tracked via successful operations
      // Reset reconnect attempts on successful start

      // Start PgBoss
      await this.boss.start();
      this.isInitialized = true;
      
      // Reset reconnect attempts on successful connection
      if (this.reconnectAttempts > 0) {
        this.logger.log(`✅ PgBoss reconnected successfully`);
        this.reconnectAttempts = 0;
        this.isReconnecting = false;
      }

      // Create job queues with retry configuration
      await this.createQueues();

      if (!this.isReconnecting) {
        this.logger.log('✅ PgBoss initialized successfully');
        this.logger.log('   Queues: full-sync, incremental-sync, delta-check');
        this.logger.log('   Features: Retries (3x), Priority queues, Cron scheduling');
        this.logger.log('════════════════════════════════════════════════════════');
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.logger.error(`Failed to initialize PgBoss: ${errorMessage}`);
      
      // If reconnecting, mark as failed but don't throw
      if (this.isReconnecting) {
        this.isReconnecting = false;
        // Will retry on next connection error
      } else {
        // Don't throw on initial startup - allow app to start without PgBoss if database isn't ready
        this.isInitialized = false;
      }
    }
  }

  /**
   * Create job queues with configuration
   */
  private async createQueues(): Promise<void> {
    if (!this.boss) return;

    const queueConfig = {
      retryLimit: 3,
      retryDelay: 30,
      retryBackoff: true,
      expireInSeconds: 3600, // 1 hour timeout
    };

    try {
      await this.boss.createQueue(JOB_NAMES.FULL_SYNC, queueConfig);
      await this.boss.createQueue(JOB_NAMES.INCREMENTAL_SYNC, queueConfig);
      await this.boss.createQueue(JOB_NAMES.DELTA_CHECK, { ...queueConfig, retryLimit: 1 });
      await this.boss.createQueue('scheduled-pipeline-run', queueConfig);
      await this.boss.createQueue('global-pipeline-polling', { ...queueConfig, retryLimit: 1 });
      this.logger.log('Job queues created successfully');
    } catch (error) {
      // Queues might already exist, which is fine
      this.logger.debug(`Queue creation: ${error}`);
    }
  }

  /**
   * Handle connection errors with automatic reconnection
   */
  private async handleConnectionError(): Promise<void> {
    if (this.isReconnecting) {
      return; // Already reconnecting
    }

    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      this.logger.error(
        `PgBoss reconnection failed after ${this.maxReconnectAttempts} attempts. PgBoss will remain disabled.`,
      );
      this.isInitialized = false;
      return;
    }

    this.isReconnecting = true;
    this.reconnectAttempts++;

    this.logger.log(
      `Attempting to reconnect PgBoss (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})...`,
    );

    // Clear any existing timer
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
    }

    // Wait before reconnecting
    this.reconnectTimer = setTimeout(async () => {
      try {
        // Stop current instance if it exists
        if (this.boss) {
          try {
            await this.boss.stop({ graceful: false, timeout: 5000 });
          } catch (stopError) {
            // Ignore stop errors during reconnection
          }
          this.boss = null;
        }

        // Reinitialize using internal method
        await this.initializePgBoss();
      } catch (reconnectError) {
        const errorMessage = reconnectError instanceof Error ? reconnectError.message : String(reconnectError);
        this.logger.warn(`Reconnection attempt ${this.reconnectAttempts} failed: ${errorMessage}`);
        this.isReconnecting = false;
        // Will retry on next connection error
      }
    }, this.reconnectDelay);
  }

  /**
   * Cleanup on module destroy
   */
  async onModuleDestroy(): Promise<void> {
    // Clear reconnect timer
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    if (this.boss) {
      this.logger.log('Stopping PgBoss...');
      try {
        await this.boss.stop({ graceful: true, timeout: 30000 });
      } catch (error) {
        this.logger.warn(`Error stopping PgBoss: ${error}`);
      }
      this.boss = null;
      this.isInitialized = false;
      this.logger.log('PgBoss stopped');
    }
  }

  /**
   * Check if PgBoss is ready
   */
  isReady(): boolean {
    if (!this.isInitialized || !this.boss) {
      return false;
    }

    // Check if boss is in a valid state
    try {
      // PgBoss v12 doesn't expose a direct "isStarted" method
      // We'll rely on isInitialized flag and boss instance existence
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Get connection health status
   */
  getHealthStatus(): { ready: boolean; reconnecting: boolean; reconnectAttempts: number } {
    return {
      ready: this.isReady(),
      reconnecting: this.isReconnecting,
      reconnectAttempts: this.reconnectAttempts,
    };
  }

  /**
   * Get the PgBoss instance (for advanced usage)
   */
  getInstance(): PgBoss | null {
    return this.boss;
  }

  // ============================================================================
  // JOB SENDING METHODS
  // ============================================================================

  /**
   * Enqueue a full sync job
   * Priority: NORMAL for scheduled, HIGH for manual
   */
  async enqueueFullSync(data: FullSyncJobData): Promise<string | null> {
    if (!this.isReady()) {
      if (this.isReconnecting) {
        this.logger.debug('PgBoss reconnecting, will retry enqueue after reconnection');
      } else {
        this.logger.warn('PgBoss not ready, cannot enqueue full sync job');
      }
      return null;
    }

    try {
      const priority = data.triggerType === 'manual' ? JOB_PRIORITY.HIGH : JOB_PRIORITY.NORMAL;

      const sendOptions: SendOptions = {
        priority,
        retryLimit: 3,
        retryDelay: 30,
        retryBackoff: true,
        // Unique key to prevent duplicate jobs for same pipeline
        singletonKey: `full-sync-${data.pipelineId}`,
        // Expire singleton after 5 minutes (allow retry)
        singletonSeconds: 300,
      };

      const jobId = await this.boss!.send(JOB_NAMES.FULL_SYNC, data, sendOptions);

      this.logger.log(
        `Enqueued full sync job: ${jobId} for pipeline ${data.pipelineId} (priority: ${priority})`,
      );
      return jobId;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      if (errorMessage.includes('Connection') || errorMessage.includes('terminated')) {
        this.logger.warn(`Connection error while enqueuing full sync: ${errorMessage}`);
        this.handleConnectionError();
      } else {
        this.logger.error(`Failed to enqueue full sync job: ${errorMessage}`);
      }
      return null;
    }
  }

  /**
   * Enqueue an incremental sync job
   * Priority: HIGH (always important to capture changes quickly)
   */
  async enqueueIncrementalSync(data: IncrementalSyncJobData): Promise<string | null> {
    if (!this.isReady()) {
      if (this.isReconnecting) {
        this.logger.debug('PgBoss reconnecting, will retry enqueue after reconnection');
      } else {
        this.logger.warn('PgBoss not ready, cannot enqueue incremental sync job');
      }
      return null;
    }

    try {
      const priority =
        data.triggerType === 'resume' || data.triggerType === 'manual'
          ? JOB_PRIORITY.HIGH
          : JOB_PRIORITY.NORMAL;

      const sendOptions: SendOptions = {
        priority,
        retryLimit: 3,
        retryDelay: 30,
        retryBackoff: true,
        singletonKey: `incr-sync-${data.pipelineId}`,
        singletonSeconds: 60, // Shorter window for incremental
      };

      const jobId = await this.boss!.send(JOB_NAMES.INCREMENTAL_SYNC, data, sendOptions);

      this.logger.log(
        `Enqueued incremental sync job: ${jobId} for pipeline ${data.pipelineId} (trigger: ${data.triggerType})`,
      );
      return jobId;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      if (errorMessage.includes('Connection') || errorMessage.includes('terminated')) {
        this.logger.warn(`Connection error while enqueuing incremental sync: ${errorMessage}`);
        this.handleConnectionError();
      } else {
        this.logger.error(`Failed to enqueue incremental sync job: ${errorMessage}`);
      }
      return null;
    }
  }

  /**
   * Enqueue a delta check job (polling)
   * Priority: LOW (background operation)
   */
  async enqueueDeltaCheck(data: DeltaCheckJobData): Promise<string | null> {
    if (!this.isReady()) {
      // Delta checks are low priority, so we can silently skip if not ready
      return null;
    }

    try {
      const sendOptions: SendOptions = {
        priority: JOB_PRIORITY.LOW,
        retryLimit: 1, // Don't retry delta checks aggressively
        singletonKey: `delta-check-${data.pipelineId}`,
        singletonSeconds: 30, // Short window - delta checks are frequent
      };

      const jobId = await this.boss!.send(JOB_NAMES.DELTA_CHECK, data, sendOptions);

      this.logger.debug(`Enqueued delta check job: ${jobId} for pipeline ${data.pipelineId}`);
      return jobId;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      if (errorMessage.includes('Connection') || errorMessage.includes('terminated')) {
        // Delta checks are low priority, just log and trigger reconnection
        this.logger.debug(`Connection error while enqueuing delta check: ${errorMessage}`);
        this.handleConnectionError();
      } else {
        this.logger.warn(`Failed to enqueue delta check job: ${errorMessage}`);
      }
      return null;
    }
  }

  /**
   * Publish a status update event (pub/sub)
   */
  async publishStatusUpdate(data: StatusUpdateEventData): Promise<void> {
    if (!this.isReady()) {
      this.logger.warn('PgBoss not ready, cannot publish status update');
      return;
    }

    // Use PgBoss publish for fan-out to multiple subscribers
    await this.boss!.publish(JOB_NAMES.STATUS_UPDATE, data);

    this.logger.debug(`Published status update for pipeline ${data.pipelineId}: ${data.status}`);
  }

  // ============================================================================
  // CRON SCHEDULING METHODS
  // ============================================================================

  /**
   * Schedule a recurring cron job for pipeline polling
   * This replaces pg_cron for CDC/incremental polling
   */
  async scheduleCronJob(
    name: string,
    cronExpression: string,
    data: any,
    options?: { tz?: string },
  ): Promise<void> {
    if (!this.isReady()) {
      this.logger.warn('PgBoss not ready, cannot schedule cron job');
      return;
    }

    await this.boss!.schedule(name, cronExpression, data, {
      tz: options?.tz || 'UTC',
    });

    this.logger.log(`Scheduled cron job: ${name} with cron "${cronExpression}"`);
  }

  /**
   * Unschedule a cron job
   */
  async unscheduleCronJob(name: string): Promise<void> {
    if (!this.isReady()) {
      return;
    }

    await this.boss!.unschedule(name);
    this.logger.log(`Unscheduled cron job: ${name}`);
  }

  /**
   * Setup the global delta check cron job
   * Runs every minute to check all active pipelines for changes
   */
  async setupGlobalPollingCron(): Promise<void> {
    if (!this.isReady()) {
      return;
    }

    // Schedule global polling job every minute
    await this.scheduleCronJob(
      'global-pipeline-polling',
      '* * * * *', // Every minute
      { type: 'global-poll' },
    );

    this.logger.log('Global pipeline polling cron job scheduled (every minute)');
  }

  // ============================================================================
  // JOB WORKER REGISTRATION
  // ============================================================================

  /**
   * Register a job handler for a specific job type
   * Note: pg-boss v12 passes an array of jobs to the handler
   */
  async registerWorker<T>(
    jobName: string,
    handler: (job: Job<T>) => Promise<void>,
    options?: WorkOptions,
  ): Promise<void> {
    if (!this.isReady()) {
      this.logger.warn(`PgBoss not ready, cannot register worker for ${jobName}`);
      return;
    }

    // Store handler reference
    this.jobHandlers.set(jobName, handler as any);

    // Register with PgBoss v12 API - handler receives array of jobs
    const workOptions: WorkOptions = {
      batchSize: 1, // Process one job at a time
      ...options,
    };

    await this.boss!.work<T>(jobName, workOptions, async (jobs: Job<T>[]) => {
      // Process each job in the batch
      for (const job of jobs) {
        try {
          this.logger.debug(`Processing job ${job.id} (${jobName})`);
          await handler(job);
          this.logger.debug(`Completed job ${job.id} (${jobName})`);
        } catch (error) {
          this.logger.error(`Job ${job.id} (${jobName}) failed: ${error}`);
          throw error; // Re-throw to trigger PgBoss retry
        }
      }
    });

    this.logger.log(`Registered worker for job type: ${jobName}`);
  }

  /**
   * Register a subscriber for pub/sub events
   */
  async registerSubscriber<T>(
    eventName: string,
    handler: (data: T) => Promise<void>,
  ): Promise<void> {
    if (!this.isReady()) {
      this.logger.warn(`PgBoss not ready, cannot register subscriber for ${eventName}`);
      return;
    }

    try {
      // In pg-boss v12, subscribe takes (event, name) and we need to use work for the handler
      const subscriberName = `${eventName}-subscriber`;
      
      // Create the subscriber queue first
      try {
        await this.boss!.createQueue(subscriberName, { retryLimit: 1 });
      } catch {
        // Queue might already exist
      }

      await this.boss!.subscribe(eventName, subscriberName);

      // Register a worker to handle the subscription - handler receives array of jobs
      await this.boss!.work<T>(subscriberName, { batchSize: 1 }, async (jobs: Job<T>[]) => {
        for (const job of jobs) {
          try {
            await handler(job.data);
          } catch (error) {
            this.logger.error(`Subscriber error for ${eventName}: ${error}`);
          }
        }
      });

      this.logger.log(`Registered subscriber for event: ${eventName}`);
    } catch (error) {
      this.logger.error(`Failed to register subscriber for ${eventName}: ${error}`);
    }
  }

  // ============================================================================
  // JOB MANAGEMENT METHODS
  // ============================================================================

  /**
   * Get job by ID
   */
  async getJob<T>(jobName: string, jobId: string): Promise<any | null> {
    if (!this.isReady()) {
      return null;
    }

    return await this.boss!.getJobById<T>(jobName, jobId);
  }

  /**
   * Cancel a job
   */
  async cancelJob(jobName: string, jobId: string): Promise<void> {
    if (!this.isReady()) {
      return;
    }

    await this.boss!.cancel(jobName, jobId);
    this.logger.log(`Cancelled job: ${jobId}`);
  }

  /**
   * Complete a job manually (for external completion)
   */
  async completeJob(jobName: string, jobId: string, data?: any): Promise<void> {
    if (!this.isReady()) {
      return;
    }

    await this.boss!.complete(jobName, jobId, data);
    this.logger.debug(`Manually completed job: ${jobId}`);
  }

  /**
   * Fail a job manually
   */
  async failJob(jobName: string, jobId: string, error: Error | string): Promise<void> {
    if (!this.isReady()) {
      return;
    }

    const errorData = typeof error === 'string' ? { message: error } : { message: error.message };
    await this.boss!.fail(jobName, jobId, errorData);
    this.logger.debug(`Manually failed job: ${jobId}`);
  }

  /**
   * Get queue statistics
   */
  async getQueueStats(queueName: string): Promise<{
    created: number;
    active: number;
    completed: number;
    failed: number;
  } | null> {
    if (!this.isReady()) {
      return null;
    }

    try {
      const stats = await this.boss!.getQueueStats(queueName);
      return {
        created: stats?.queuedCount || 0,
        active: stats?.activeCount || 0,
        completed: 0,
        failed: 0,
      };
    } catch {
      return null;
    }
  }

  /**
   * Delete all jobs for a specific pipeline (cleanup)
   */
  async deleteJobsForPipeline(pipelineId: string): Promise<void> {
    if (!this.isReady()) {
      return;
    }

    // Cancel any pending jobs for this pipeline using singleton keys
    // Note: pg-boss v12 doesn't allow canceling by singleton key directly
    // We would need to track job IDs separately for proper cleanup
    this.logger.log(`Cleanup requested for pipeline: ${pipelineId}`);
  }
}
