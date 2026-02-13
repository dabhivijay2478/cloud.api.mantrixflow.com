/**
 * Pipeline Updates WebSocket Gateway
 * Handles real-time updates via Socket.io
 *
 * Architecture:
 * - Listens to Postgres NOTIFY events for transient job status updates
 *   ('pipeline_updates', 'pipeline_run_updates', 'pipeline_job_status')
 * - Subscribes to Supabase Realtime for table-level changes on pipelines / pipeline_runs
 * - Forwards all notifications to Socket.io clients
 * - Clients join room: pipeline_{pipelineId} to receive updates
 */

import {
  WebSocketGateway,
  WebSocketServer,
  OnGatewayConnection,
  OnGatewayDisconnect,
  SubscribeMessage,
  MessageBody,
  ConnectedSocket,
} from '@nestjs/websockets';
import { Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { Server, Socket } from 'socket.io';
import { ConfigService } from '@nestjs/config';
import { createClient, SupabaseClient, RealtimeChannel } from '@supabase/supabase-js';
import { ActivityLoggerService } from '../../../common/logger';
import { PG_NOTIFY_PIPELINE_STATUS } from '../../queue/pgmq.constants';

interface PipelineUpdatePayload {
  pipeline_id: string;
  organization_id: string;
  status?: string;
  last_run_status?: string;
  last_run_at?: string;
  total_rows_processed?: number;
  last_sync_at?: string;
  checkpoint?: any;
  updated_at?: string;
}

interface PipelineRunUpdatePayload {
  run_id: string;
  pipeline_id: string;
  organization_id: string;
  status?: string;
  rows_read?: number;
  rows_written?: number;
  rows_skipped?: number;
  rows_failed?: number;
  duration_seconds?: number;
  error_message?: string;
  updated_at?: string;
}

@WebSocketGateway({
  cors: {
    origin: (() => {
      // Only from environment (apps/api/.env): ALLOWED_ORIGINS, FRONTEND_URL
      const allowedOriginsEnv = process.env.ALLOWED_ORIGINS ?? '';
      const frontendUrl = process.env.FRONTEND_URL ?? '';
      const allowedOrigins: string[] = [];
      if (allowedOriginsEnv) {
        allowedOrigins.push(
          ...allowedOriginsEnv
            .split(',')
            .map((o) => o.trim())
            .filter(Boolean),
        );
      }
      if (frontendUrl && !allowedOrigins.includes(frontendUrl)) {
        allowedOrigins.push(frontendUrl);
      }
      return allowedOrigins;
    })(),
    credentials: true,
  },
  namespace: '/pipelines',
})
export class PipelineUpdatesGateway
  implements OnGatewayConnection, OnGatewayDisconnect, OnModuleInit, OnModuleDestroy
{
  @WebSocketServer()
  server: Server;

  private readonly logger = new Logger(PipelineUpdatesGateway.name);
  private pgClient: any = null;
  private supabase: SupabaseClient | null = null;
  private realtimeChannel: RealtimeChannel | null = null;
  private notifyListeners: Array<{ channel: string; handler: (payload: string) => void }> =
    [];

  constructor(
    private readonly configService: ConfigService,
    private readonly activity: ActivityLoggerService,
  ) {}

  async onModuleInit() {
    this.logger.log('Initializing Pipeline Updates Gateway...');
    await this.setupPostgresListeners();
    await this.setupSupabaseRealtime();
    this.logger.log('Pipeline Updates Gateway initialized');
  }

  async onModuleDestroy() {
    this.logger.log('Shutting down Pipeline Updates Gateway...');
    // Clean up Postgres NOTIFY client
    if (this.pgClient) {
      try {
        for (const listener of this.notifyListeners) {
          await this.pgClient.query(`UNLISTEN ${listener.channel}`).catch(() => {});
        }
        await this.pgClient.end();
      } catch {
        /* ignore cleanup errors */
      }
      this.pgClient = null;
    }
    // Clean up Supabase Realtime
    if (this.realtimeChannel && this.supabase) {
      this.supabase.removeChannel(this.realtimeChannel);
      this.realtimeChannel = null;
    }
    if (this.supabase) {
      await this.supabase.removeAllChannels();
      this.supabase = null;
    }
    this.logger.log('Pipeline Updates Gateway shut down');
  }

  // ════════════════════════════════════════════════════════════════
  // SUPABASE REALTIME (table-level changes on pipelines / pipeline_runs)
  // ════════════════════════════════════════════════════════════════

  /**
   * Subscribe to Supabase Realtime for table-level change events.
   * Catches DB-level UPDATEs on the pipelines and pipeline_runs tables,
   * complementing the NOTIFY channel for transient job status updates.
   */
  private async setupSupabaseRealtime(): Promise<void> {
    const supabaseUrl = this.configService.get<string>('SUPABASE_URL');
    const supabaseKey =
      this.configService.get<string>('SUPABASE_SERVICE_ROLE_KEY') ||
      this.configService.get<string>('SUPABASE_ANON_KEY');
    if (!supabaseUrl || !supabaseKey) {
      this.logger.warn(
        'SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set — Supabase Realtime disabled',
      );
      return;
    }
    try {
      this.supabase = createClient(supabaseUrl, supabaseKey, {
        realtime: { params: { eventsPerSecond: 10 } },
      });
      this.realtimeChannel = this.supabase
        .channel('pipeline-realtime')
        .on(
          'postgres_changes',
          { event: 'UPDATE', schema: 'public', table: 'pipelines' },
          (payload) => this.handleSupabasePipelineChange(payload),
        )
        .on(
          'postgres_changes',
          { event: '*', schema: 'public', table: 'pipeline_runs' },
          (payload) => this.handleSupabaseRunChange(payload),
        )
        .subscribe((status) => {
          this.logger.log(`Supabase Realtime subscription status: ${status}`);
        });
      this.logger.log(
        'Supabase Realtime subscription active for pipelines & pipeline_runs',
      );
    } catch (error) {
      this.logger.error(`Failed to setup Supabase Realtime: ${error}`);
    }
  }

  /** Forward Supabase Realtime pipeline UPDATE to Socket.io rooms. */
  private handleSupabasePipelineChange(payload: any): void {
    try {
      const row = payload.new;
      if (!row?.id || !row?.organization_id) return;
      const update: PipelineUpdatePayload = {
        pipeline_id: row.id,
        organization_id: row.organization_id,
        status: row.status,
        last_run_status: row.last_run_status,
        last_run_at: row.last_run_at,
        total_rows_processed: row.total_rows_processed,
        last_sync_at: row.last_sync_at,
        updated_at: row.updated_at,
      };
      this.server
        .to(`pipeline_${update.pipeline_id}`)
        .emit('update', { type: 'pipeline', ...update });
      this.server.to(`org_${update.organization_id}`).emit('pipeline_update', update);
      this.logger.debug(`Supabase Realtime: pipeline ${update.pipeline_id} updated`);
    } catch (error) {
      this.logger.error(`Error handling Supabase pipeline change: ${error}`);
    }
  }

  /** Forward Supabase Realtime pipeline_runs change to Socket.io rooms. */
  private handleSupabaseRunChange(payload: any): void {
    try {
      const row = payload.new;
      if (!row?.id || !row?.pipeline_id) return;
      const update: PipelineRunUpdatePayload = {
        run_id: row.id,
        pipeline_id: row.pipeline_id,
        organization_id: row.organization_id,
        status: row.status,
        rows_read: row.rows_read,
        rows_written: row.rows_written,
        rows_skipped: row.rows_skipped,
        rows_failed: row.rows_failed,
        duration_seconds: row.duration_seconds,
        error_message: row.error_message,
        updated_at: row.updated_at,
      };
      this.server
        .to(`pipeline_${update.pipeline_id}`)
        .emit('run_update', { type: 'run', ...update });
      this.server.to(`run_${update.run_id}`).emit('update', { type: 'run', ...update });
      this.logger.debug(`Supabase Realtime: run ${update.run_id} updated`);
    } catch (error) {
      this.logger.error(`Error handling Supabase run change: ${error}`);
    }
  }

  // ════════════════════════════════════════════════════════════════
  // POSTGRES NOTIFY (transient status updates + DB triggers)
  // ════════════════════════════════════════════════════════════════

  /**
   * Set up Postgres LISTEN for NOTIFY events.
   * Uses DATABASE_DIRECT_URL (session-mode pooler) if available, otherwise DATABASE_URL.
   */
  private async setupPostgresListeners() {
    try {
      const databaseUrl =
        this.configService.get<string>('DATABASE_DIRECT_URL') ||
        this.configService.get<string>('DATABASE_URL');
      if (!databaseUrl) {
        this.logger.warn('DATABASE_URL not found — NOTIFY listeners disabled');
        return;
      }
      await this.connectPgNotify(databaseUrl);
      // Store listeners for cleanup
      this.notifyListeners = [
        {
          channel: 'pipeline_updates',
          handler: (payload) => this.handlePipelineUpdate(payload),
        },
        {
          channel: 'pipeline_run_updates',
          handler: (payload) => this.handlePipelineRunUpdate(payload),
        },
        {
          channel: PG_NOTIFY_PIPELINE_STATUS,
          handler: (payload) => this.handleJobStatusUpdate(payload),
        },
      ];
    } catch (error) {
      this.logger.error(`Failed to setup Postgres listeners: ${error}`);
    }
  }

  /**
   * Create a dedicated PG connection for LISTEN/NOTIFY with auto-reconnect.
   * Handles ECONNRESET gracefully instead of crashing the process.
   */
  private async connectPgNotify(databaseUrl: string): Promise<void> {
    try {
      if (this.pgClient) {
        await this.pgClient.end().catch(() => {});
        this.pgClient = null;
      }
      const { Client } = await import('pg');
      this.pgClient = new Client({ connectionString: databaseUrl });
      // Attach error handler BEFORE connect to prevent unhandled 'error' crash
      this.pgClient.on('error', (err: Error) => {
        this.logger.warn(`Postgres NOTIFY connection lost: ${err.message}. Reconnecting…`);
        this.pgClient = null;
        setTimeout(() => void this.connectPgNotify(databaseUrl), 3_000);
      });
      await this.pgClient.connect();
      await this.pgClient.query('LISTEN pipeline_updates');
      await this.pgClient.query('LISTEN pipeline_run_updates');
      await this.pgClient.query(`LISTEN ${PG_NOTIFY_PIPELINE_STATUS}`);
      this.pgClient.on('notification', (msg: any) => {
        this.handlePostgresNotification(msg.channel, msg.payload);
      });
      this.logger.log(
        `Postgres NOTIFY listeners: pipeline_updates, pipeline_run_updates, ${PG_NOTIFY_PIPELINE_STATUS}`,
      );
    } catch (error) {
      this.logger.error(`Postgres NOTIFY connect failed: ${error}. Retrying in 5 s…`);
      this.pgClient = null;
      setTimeout(() => void this.connectPgNotify(databaseUrl), 5_000);
    }
  }

  /** Route Postgres NOTIFY to the appropriate handler. */
  private handlePostgresNotification(channel: string, payload: string) {
    try {
      if (channel === 'pipeline_updates') {
        this.handlePipelineUpdate(payload);
      } else if (channel === 'pipeline_run_updates') {
        this.handlePipelineRunUpdate(payload);
      } else if (channel === PG_NOTIFY_PIPELINE_STATUS) {
        this.handleJobStatusUpdate(payload);
      }
    } catch (error) {
      this.logger.error(`Error handling NOTIFY from ${channel}: ${error}`);
    }
  }

  /** Handle pipeline update from DB trigger NOTIFY (snake_case). */
  private handlePipelineUpdate(payload: string) {
    try {
      const update: PipelineUpdatePayload = JSON.parse(payload);
      const { pipeline_id, organization_id } = update;
      this.server
        .to(`pipeline_${pipeline_id}`)
        .emit('update', { type: 'pipeline', ...update });
      this.server.to(`org_${organization_id}`).emit('pipeline_update', update);
      this.logger.debug(`Broadcasted pipeline update for ${pipeline_id}`);
    } catch (error) {
      this.logger.error(`Error parsing pipeline update: ${error}`);
    }
  }

  /** Handle pipeline run update from DB trigger NOTIFY (snake_case). */
  private handlePipelineRunUpdate(payload: string) {
    try {
      const update: PipelineRunUpdatePayload = JSON.parse(payload);
      const { pipeline_id, run_id } = update;
      this.server
        .to(`pipeline_${pipeline_id}`)
        .emit('run_update', { type: 'run', ...update });
      this.server.to(`run_${run_id}`).emit('update', { type: 'run', ...update });
      this.logger.debug(`Broadcasted run update for ${run_id} (pipeline: ${pipeline_id})`);
    } catch (error) {
      this.logger.error(`Error parsing run update: ${error}`);
    }
  }

  /**
   * Handle transient job status update from PgmqQueueService (via NOTIFY).
   * Payload uses camelCase (pipelineId, organizationId, status, …).
   */
  private handleJobStatusUpdate(payload: string): void {
    try {
      const data = JSON.parse(payload) as {
        pipelineId: string;
        organizationId: string;
        status: string;
        rowsProcessed?: number;
        newRowsCount?: number;
        error?: string;
        timestamp: string;
      };
      const {
        pipelineId,
        organizationId,
        status,
        rowsProcessed,
        newRowsCount,
        error,
        timestamp,
      } = data;
      this.server.to(`pipeline_${pipelineId}`).emit('update', {
        type: 'pipeline',
        pipeline_id: pipelineId,
        organization_id: organizationId,
        status,
        total_rows_processed: rowsProcessed,
        new_rows_count: newRowsCount,
        error,
        updated_at: timestamp,
      });
      if (organizationId) {
        this.server.to(`org_${organizationId}`).emit('pipeline_update', {
          pipeline_id: pipelineId,
          status,
          total_rows_processed: rowsProcessed,
          new_rows_count: newRowsCount,
          updated_at: timestamp,
        });
      }
      this.logger.debug(`Broadcasted job status update for pipeline ${pipelineId}`);
    } catch (error) {
      this.logger.error(`Error parsing job status update: ${error}`);
    }
  }

  // ════════════════════════════════════════════════════════════════
  // CLIENT CONNECTION HANDLERS
  // ════════════════════════════════════════════════════════════════

  /** Handle client connection */
  handleConnection(client: Socket) {
    this.activity.info('ws.client_connected', `Client connected: ${client.id}`, {
      metadata: { clientId: client.id },
    });
  }

  /** Handle client disconnection */
  handleDisconnect(client: Socket) {
    this.activity.info('ws.client_disconnected', `Client disconnected: ${client.id}`, {
      metadata: { clientId: client.id },
    });
  }

  /** Join pipeline room to receive updates */
  @SubscribeMessage('join_pipeline')
  handleJoinPipeline(
    @MessageBody() data: { pipelineId: string; organizationId?: string },
    @ConnectedSocket() client: Socket,
  ) {
    const { pipelineId, organizationId } = data;
    if (!pipelineId) {
      client.emit('error', { message: 'pipelineId is required' });
      return;
    }
    client.join(`pipeline_${pipelineId}`);
    this.activity.info('ws.room_joined', `Client ${client.id} joined pipeline_${pipelineId}`, {
      pipelineId,
      organizationId,
      metadata: { clientId: client.id, room: `pipeline_${pipelineId}` },
    });
    if (organizationId) {
      client.join(`org_${organizationId}`);
    }
    client.emit('joined', { pipelineId, organizationId });
  }

  /** Leave pipeline room */
  @SubscribeMessage('leave_pipeline')
  handleLeavePipeline(
    @MessageBody() data: { pipelineId: string },
    @ConnectedSocket() client: Socket,
  ) {
    const { pipelineId } = data;
    if (pipelineId) {
      client.leave(`pipeline_${pipelineId}`);
      this.activity.info('ws.room_left', `Client ${client.id} left pipeline_${pipelineId}`, {
        pipelineId,
        metadata: { clientId: client.id },
      });
    }
    client.emit('left', { pipelineId });
  }

  /** Join pipeline run room */
  @SubscribeMessage('join_run')
  handleJoinRun(
    @MessageBody() data: { runId: string },
    @ConnectedSocket() client: Socket,
  ) {
    const { runId } = data;
    if (runId) {
      client.join(`run_${runId}`);
      this.activity.debug('ws.room_joined', `Client ${client.id} joined run_${runId}`, {
        runId,
        metadata: { clientId: client.id },
      });
      client.emit('joined_run', { runId });
    }
  }
}
