/**
 * PgBoss Interfaces
 * Type definitions for PgBoss module configuration
 */

import type { ModuleMetadata, Type } from '@nestjs/common';
import type { ConstructorOptions } from 'pg-boss';

/**
 * PgBoss module configuration options
 * Extends pg-boss ConstructorOptions with additional metadata
 */
export interface PgBossModuleOptions extends ConstructorOptions {
  // Additional options can be added here
}

/**
 * Factory interface for creating PgBoss options
 */
export interface PgBossOptionsFactory {
  createPgBossOptions(): Promise<PgBossModuleOptions> | PgBossModuleOptions;
}

/**
 * Async module options for PgBoss
 * Supports useFactory, useClass, and useExisting patterns
 */
export interface PgBossModuleAsyncOptions extends Pick<ModuleMetadata, 'imports'> {
  /**
   * Factory function to create options
   */
  useFactory?: (...args: any[]) => Promise<PgBossModuleOptions> | PgBossModuleOptions;

  /**
   * Dependencies to inject into factory
   */
  inject?: any[];

  /**
   * Class that implements PgBossOptionsFactory
   */
  useClass?: Type<PgBossOptionsFactory>;

  /**
   * Existing provider to use
   */
  useExisting?: Type<PgBossOptionsFactory>;
}

/**
 * Job data wrapper with metadata
 */
export interface JobData<T = unknown> {
  /**
   * The actual payload data
   */
  payload: T;

  /**
   * Job metadata
   */
  metadata?: {
    organizationId?: string;
    userId?: string;
    correlationId?: string;
    triggeredAt?: string;
    source?: string;
  };
}

/**
 * Cron schedule options
 */
export interface CronScheduleOptions {
  /**
   * Cron expression (e.g., '0 * * * *' for every hour)
   */
  cron: string;

  /**
   * Timezone (e.g., 'UTC', 'America/New_York')
   */
  timezone?: string;

  /**
   * Optional job data to include with each run
   */
  data?: Record<string, unknown>;
}

/**
 * Job completion handler result
 */
export interface JobResult<T = unknown> {
  success: boolean;
  data?: T;
  error?: string;
  duration?: number;
}

/**
 * Worker options for job processing
 */
export interface WorkerOptions {
  /**
   * Number of concurrent jobs to process
   */
  teamSize?: number;

  /**
   * How many jobs to fetch per batch
   */
  batchSize?: number;

  /**
   * Include job metadata in handler
   */
  includeMetadata?: boolean;

  /**
   * Priority level for this worker
   */
  priority?: boolean;
}

/**
 * Pipeline job data structure
 */
export interface PipelineJobData {
  pipelineId: string;
  organizationId: string;
  userId: string;
  triggerType: 'manual' | 'scheduled' | 'webhook';
  options?: {
    batchSize?: number;
    retryAttempts?: number;
  };
}

/**
 * Notification job data structure
 */
export interface NotificationJobData {
  type: 'email' | 'push' | 'in-app';
  recipientId: string;
  template: string;
  data: Record<string, unknown>;
  organizationId?: string;
}
