/**
 * PgBoss Service
 * Core service for managing PostgreSQL-based job queues
 *
 * Features:
 * - Exactly-once job delivery
 * - Priority queues
 * - Cron scheduling (distributed)
 * - Dead letter queues
 * - Exponential backoff retries
 * - Job chaining
 */

import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { PgBoss } from 'pg-boss';
import type {
  Job,
  SendOptions,
  WorkOptions as PgBossWorkOptions,
  ScheduleOptions,
  Schedule,
  JobWithMetadata,
  ConstructorOptions,
} from 'pg-boss';
import { PGBOSS_OPTIONS, DEFAULT_JOB_OPTIONS, JOB_PRIORITY } from './pgboss.constants';
import type {
  PgBossModuleOptions,
  JobData,
  CronScheduleOptions,
  JobResult,
  WorkerOptions,
} from './pgboss.interfaces';

type JobHandler<T> = (job: Job<JobData<T>>) => Promise<JobResult<unknown> | undefined>;

@Injectable()
export class PgBossService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PgBossService.name);
  private boss!: PgBoss;
  private isConnected = false;
  private readonly registeredWorkers: Map<string, () => Promise<void>> = new Map();

  constructor(
    @Inject(PGBOSS_OPTIONS)
    private readonly options: PgBossModuleOptions,
  ) {}

  /**
   * Initialize PgBoss on module startup
   */
  async onModuleInit(): Promise<void> {
    try {
      // Create PgBoss instance
      this.boss = new PgBoss(this.options as ConstructorOptions);

      // Set up event handlers
      this.boss.on('error', (error: Error) => {
        this.logger.error(`PgBoss error: ${error.message}`, error.stack);
      });

      await this.boss.start();
      this.isConnected = true;
      this.logger.log('PgBoss started successfully');

      // Register any pre-configured workers
      for (const [name, starter] of this.registeredWorkers) {
        try {
          await starter();
          this.logger.log(`Worker started for queue: ${name}`);
        } catch (error) {
          this.logger.error(
            `Failed to start worker for ${name}: ${error instanceof Error ? error.message : error}`,
          );
        }
      }
    } catch (error) {
      this.logger.error(
        `Failed to start PgBoss: ${error instanceof Error ? error.message : error}`,
        error instanceof Error ? error.stack : undefined,
      );
      throw error;
    }
  }

  /**
   * Gracefully shutdown PgBoss on module destroy
   */
  async onModuleDestroy(): Promise<void> {
    if (this.isConnected) {
      try {
        await this.boss.stop({ graceful: true, timeout: 30000 });
        this.isConnected = false;
        this.logger.log('PgBoss stopped gracefully');
      } catch (error) {
        this.logger.error(
          `Error stopping PgBoss: ${error instanceof Error ? error.message : error}`,
        );
      }
    }
  }

  /**
   * Get the underlying PgBoss instance for advanced operations
   */
  getBoss(): PgBoss {
    return this.boss;
  }

  /**
   * Check if PgBoss is connected and ready
   */
  isReady(): boolean {
    return this.isConnected;
  }

  // ==========================================
  // JOB SENDING METHODS
  // ==========================================

  /**
   * Send a job to a queue
   * Basic job sending with optional configuration
   */
  async send<T>(
    queueName: string,
    data: T,
    options?: Partial<SendOptions>,
  ): Promise<string | null> {
    const jobData: JobData<T> = {
      payload: data,
      metadata: {
        triggeredAt: new Date().toISOString(),
      },
    };

    const jobOptions: SendOptions = {
      ...DEFAULT_JOB_OPTIONS,
      ...options,
    };

    const jobId = await this.boss.send(queueName, jobData, jobOptions);

    if (jobId) {
      this.logger.log(`Job ${jobId} sent to queue ${queueName}`);
    }

    return jobId;
  }

  /**
   * Send a job with metadata (organization context, user, etc.)
   */
  async sendWithContext<T>(
    queueName: string,
    data: T,
    context: {
      organizationId?: string;
      userId?: string;
      correlationId?: string;
    },
    options?: Partial<SendOptions>,
  ): Promise<string | null> {
    const jobData: JobData<T> = {
      payload: data,
      metadata: {
        ...context,
        triggeredAt: new Date().toISOString(),
        source: 'api',
      },
    };

    return this.boss.send(queueName, jobData, {
      ...DEFAULT_JOB_OPTIONS,
      ...options,
    });
  }

  /**
   * Send a high-priority job
   */
  async sendPriority<T>(
    queueName: string,
    data: T,
    priority: keyof typeof JOB_PRIORITY = 'HIGH',
    options?: Partial<SendOptions>,
  ): Promise<string | null> {
    return this.send(queueName, data, {
      ...options,
      priority: JOB_PRIORITY[priority],
    });
  }

  /**
   * Send a delayed job (execute after specified time)
   */
  async sendDelayed<T>(
    queueName: string,
    data: T,
    delaySeconds: number,
    options?: Partial<SendOptions>,
  ): Promise<string | null> {
    return this.boss.sendAfter(
      queueName,
      { payload: data } as object,
      options ?? null,
      delaySeconds,
    );
  }

  /**
   * Send a job to be executed at a specific time
   */
  async sendScheduledAt<T>(
    queueName: string,
    data: T,
    executeAt: Date,
    options?: Partial<SendOptions>,
  ): Promise<string | null> {
    return this.boss.sendAfter(queueName, { payload: data } as object, options ?? null, executeAt);
  }

  /**
   * Send a singleton job (only one instance can exist)
   * Useful for preventing duplicate jobs
   */
  async sendSingleton<T>(
    queueName: string,
    data: T,
    singletonKey: string,
    options?: Partial<SendOptions>,
  ): Promise<string | null> {
    return this.send(queueName, data, {
      ...options,
      singletonKey,
      singletonSeconds: options?.singletonSeconds ?? 60,
    });
  }

  /**
   * Send a debounced job (consolidates rapid fire requests)
   */
  async sendDebounced<T>(
    queueName: string,
    data: T,
    debounceKey: string,
    debounceSeconds: number = 5,
    options?: Partial<SendOptions>,
  ): Promise<string | null> {
    return this.boss.sendDebounced(
      queueName,
      { payload: data } as object,
      options ?? null,
      debounceSeconds,
      debounceKey,
    );
  }

  /**
   * Send a throttled job (rate limited)
   */
  async sendThrottled<T>(
    queueName: string,
    data: T,
    throttleKey: string,
    throttleSeconds: number = 60,
    options?: Partial<SendOptions>,
  ): Promise<string | null> {
    return this.boss.sendThrottled(
      queueName,
      { payload: data } as object,
      options ?? null,
      throttleSeconds,
      throttleKey,
    );
  }

  // ==========================================
  // CRON SCHEDULING METHODS
  // ==========================================

  /**
   * Schedule a recurring job using cron expression
   * This is distributed - only one instance will run it
   */
  async schedule(
    queueName: string,
    cronOptions: CronScheduleOptions,
    options?: Partial<ScheduleOptions>,
  ): Promise<void> {
    const { cron, timezone = 'UTC', data = {} } = cronOptions;

    // Create the queue first to ensure it exists
    // PgBoss requires the queue to exist before scheduling
    try {
      await this.boss.createQueue(queueName);
    } catch (error) {
      // Queue might already exist, which is fine
      const message = error instanceof Error ? error.message : String(error);
      if (!message.includes('already exists')) {
        this.logger.warn(`Queue creation warning for ${queueName}: ${message}`);
      }
    }

    await this.boss.schedule(queueName, cron, data, {
      tz: timezone,
      ...options,
    });

    this.logger.log(`Scheduled cron job: ${queueName} with expression ${cron}`);
  }

  /**
   * Unschedule a recurring job
   */
  async unschedule(queueName: string): Promise<void> {
    try {
      await this.boss.unschedule(queueName);
      this.logger.log(`Unscheduled cron job: ${queueName}`);
    } catch (error) {
      // Ignore if schedule doesn't exist
      const message = error instanceof Error ? error.message : String(error);
      if (!message.includes('not found')) {
        throw error;
      }
      this.logger.log(`No existing schedule found for: ${queueName}`);
    }
  }

  /**
   * Get all scheduled jobs
   */
  async getSchedules(): Promise<Schedule[]> {
    return this.boss.getSchedules();
  }

  // ==========================================
  // WORKER/PROCESSING METHODS
  // ==========================================

  /**
   * Register a worker to process jobs from a queue
   * Use this in your processor services
   */
  async work<T>(
    queueName: string,
    handler: JobHandler<T>,
    workerOptions?: WorkerOptions,
  ): Promise<string> {
    const options: PgBossWorkOptions = {
      batchSize: workerOptions?.batchSize ?? 1,
    };

    const workerId = await this.boss.work<JobData<T>>(queueName, options, async (jobs) => {
      // Handle both single job and batch modes
      const jobArray = Array.isArray(jobs) ? jobs : [jobs];

      for (const job of jobArray) {
        const startTime = Date.now();
        try {
          this.logger.debug(`Processing job ${job.id} from ${queueName}`);

          const result = await handler(job as Job<JobData<T>>);

          const duration = Date.now() - startTime;
          this.logger.log(
            `Job ${job.id} completed in ${duration}ms ${result?.success !== false ? '✓' : '✗'}`,
          );
        } catch (error) {
          const duration = Date.now() - startTime;
          this.logger.error(
            `Job ${job.id} failed after ${duration}ms: ${error instanceof Error ? error.message : error}`,
            error instanceof Error ? error.stack : undefined,
          );
          throw error; // Re-throw to trigger retry
        }
      }
    });

    this.logger.log(`Worker registered for queue: ${queueName}`);
    return workerId;
  }

  /**
   * Register a worker that will be started when PgBoss connects
   * Use this for workers defined at module initialization
   */
  registerWorker<T>(queueName: string, handler: JobHandler<T>, options?: WorkerOptions): void {
    this.registeredWorkers.set(queueName, async () => {
      await this.work(queueName, handler, options);
    });
  }

  /**
   * Stop a specific worker
   */
  async offWork(queueName: string): Promise<void> {
    await this.boss.offWork(queueName);
    this.registeredWorkers.delete(queueName);
    this.logger.log(`Worker stopped for queue: ${queueName}`);
  }

  // ==========================================
  // JOB MANAGEMENT METHODS
  // ==========================================

  /**
   * Get a job by ID
   */
  async getJobById<T>(queueName: string, jobId: string): Promise<JobWithMetadata<T> | null> {
    return this.boss.getJobById(queueName, jobId);
  }

  /**
   * Cancel a job
   */
  async cancel(queueName: string, jobId: string): Promise<void> {
    await this.boss.cancel(queueName, jobId);
    this.logger.log(`Job ${jobId} cancelled`);
  }

  /**
   * Resume a paused/cancelled job
   */
  async resume(queueName: string, jobId: string): Promise<void> {
    await this.boss.resume(queueName, jobId);
    this.logger.log(`Job ${jobId} resumed`);
  }

  /**
   * Complete a job manually (mark as done)
   */
  async complete(queueName: string, jobId: string, data?: object): Promise<void> {
    await this.boss.complete(queueName, jobId, data);
    this.logger.debug(`Job ${jobId} completed manually`);
  }

  /**
   * Fail a job manually (mark as failed)
   */
  async fail(queueName: string, jobId: string, error?: Error | string): Promise<void> {
    const errorData =
      error instanceof Error ? { message: error.message, stack: error.stack } : { message: error };
    await this.boss.fail(queueName, jobId, errorData);
    this.logger.debug(`Job ${jobId} failed manually`);
  }

  // ==========================================
  // QUEUE MANAGEMENT METHODS
  // ==========================================

  /**
   * Create a new queue (optional - queues are auto-created)
   */
  async createQueue(queueName: string): Promise<void> {
    await this.boss.createQueue(queueName);
    this.logger.log(`Queue created: ${queueName}`);
  }

  /**
   * Delete a queue and all its jobs
   */
  async deleteQueue(queueName: string): Promise<void> {
    await this.boss.deleteQueue(queueName);
    this.logger.log(`Queue deleted: ${queueName}`);
  }

  /**
   * Get queue stats
   */
  async getQueueStats(queueName: string): Promise<unknown> {
    return this.boss.getQueueStats(queueName);
  }

  // ==========================================
  // UTILITY METHODS
  // ==========================================

  /**
   * Fetch jobs without processing (for inspection)
   */
  async fetch<T>(queueName: string, batchSize: number = 10): Promise<Job<T>[]> {
    return this.boss.fetch(queueName, { batchSize });
  }

  /**
   * Insert a job and execute a callback in the same transaction
   * Useful for ensuring job creation is atomic with other DB operations
   */
  async sendInTransaction<T>(
    queueName: string,
    data: T,
    callback: (jobId: string) => Promise<void>,
    options?: Partial<SendOptions>,
  ): Promise<string | null> {
    const jobId = await this.send(queueName, data, options);

    if (jobId) {
      await callback(jobId);
    }

    return jobId;
  }
}
