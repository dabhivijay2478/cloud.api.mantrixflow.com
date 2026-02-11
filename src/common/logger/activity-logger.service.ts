/**
 * Activity Logger Service
 * Reusable structured logger for all activity tracking across the NestJS backend.
 *
 * Wraps Pino (via nestjs-pino) to log structured JSON with consistent fields:
 *   timestamp, level, action, message, userId, pipelineId, organizationId, metadata
 *
 * Usage:
 *   constructor(private readonly activity: ActivityLoggerService) {}
 *   this.activity.log({ action: 'pipeline.started', message: '...', pipelineId, userId });
 */

import { Injectable } from '@nestjs/common';
import { PinoLogger } from 'nestjs-pino';

/** Supported log levels */
export type ActivityLevel = 'info' | 'warn' | 'error' | 'debug';

/** All recognised action types (extend as needed) */
export type ActivityAction =
  // Pipeline lifecycle
  | 'pipeline.created'
  | 'pipeline.updated'
  | 'pipeline.deleted'
  | 'pipeline.started'
  | 'pipeline.completed'
  | 'pipeline.failed'
  | 'pipeline.paused'
  | 'pipeline.resumed'
  | 'pipeline.cancelled'
  | 'pipeline.batch_completed'
  | 'pipeline.scheduled'
  // Sync
  | 'sync.full_started'
  | 'sync.incremental_started'
  | 'sync.collect'
  | 'sync.transform'
  | 'sync.emit'
  | 'sync.checkpoint_saved'
  | 'sync.retry'
  | 'sync.progress'
  // Jobs
  | 'job.full_sync'
  | 'job.incremental_sync'
  | 'job.delta_check'
  | 'job.poll_cycle'
  | 'job.failed'
  // WebSocket
  | 'ws.client_connected'
  | 'ws.client_disconnected'
  | 'ws.room_joined'
  | 'ws.room_left'
  | 'ws.update_broadcast'
  | 'ws.notify_message'
  // Data sources
  | 'datasource.created'
  | 'datasource.deleted'
  | 'datasource.connection_tested'
  | 'datasource.schema_discovered'
  // Auth / request
  | 'request.handled'
  | 'request.error'
  // Generic
  | string;

export interface ActivityLogParams {
  /** Log level (default: 'info') */
  level?: ActivityLevel;
  /** Structured action type */
  action: ActivityAction;
  /** Human-readable message */
  message: string;
  /** User who triggered the action */
  userId?: string;
  /** Related pipeline ID */
  pipelineId?: string;
  /** Related pipeline run ID */
  runId?: string;
  /** Organisation scope */
  organizationId?: string;
  /** Additional structured data */
  metadata?: Record<string, unknown>;
}

@Injectable()
export class ActivityLoggerService {
  constructor(private readonly pino: PinoLogger) {
    this.pino.setContext('ActivityLogger');
  }

  /**
   * Log a structured activity event.
   * All fields are attached as top-level Pino child bindings so they appear
   * in the JSON output and are searchable by any log aggregator.
   */
  log(params: ActivityLogParams): void {
    const {
      level = 'info',
      action,
      message,
      userId,
      pipelineId,
      runId,
      organizationId,
      metadata,
    } = params;

    const ctx: Record<string, unknown> = {
      action,
      ...(userId && { userId }),
      ...(pipelineId && { pipelineId }),
      ...(runId && { runId }),
      ...(organizationId && { organizationId }),
      ...(metadata && { metadata }),
    };

    switch (level) {
      case 'error':
        this.pino.error(ctx, message);
        break;
      case 'warn':
        this.pino.warn(ctx, message);
        break;
      case 'debug':
        this.pino.debug(ctx, message);
        break;
      default:
        this.pino.info(ctx, message);
        break;
    }
  }

  /** Convenience: info-level log */
  info(
    action: ActivityAction,
    message: string,
    ctx?: Omit<ActivityLogParams, 'action' | 'message' | 'level'>,
  ): void {
    this.log({ level: 'info', action, message, ...ctx });
  }

  /** Convenience: warn-level log */
  warn(
    action: ActivityAction,
    message: string,
    ctx?: Omit<ActivityLogParams, 'action' | 'message' | 'level'>,
  ): void {
    this.log({ level: 'warn', action, message, ...ctx });
  }

  /** Convenience: error-level log */
  error(
    action: ActivityAction,
    message: string,
    ctx?: Omit<ActivityLogParams, 'action' | 'message' | 'level'>,
  ): void {
    this.log({ level: 'error', action, message, ...ctx });
  }

  /** Convenience: debug-level log */
  debug(
    action: ActivityAction,
    message: string,
    ctx?: Omit<ActivityLogParams, 'action' | 'message' | 'level'>,
  ): void {
    this.log({ level: 'debug', action, message, ...ctx });
  }
}
