/**
 * Pipeline Updates WebSocket Gateway
 * Handles real-time updates via Socket.io
 *
 * Architecture:
 * - Listens to Postgres NOTIFY events ('pipeline_updates', 'pipeline_run_updates')
 * - Forwards notifications to Socket.io clients
 * - Clients join room: pipeline_{pipelineId} to receive updates
 *
 * ROOT FIX: Real-time updates for status, row counts, progress
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
      const allowedOriginsEnv = process.env.ALLOWED_ORIGINS || '';
      const frontendUrl = process.env.FRONTEND_URL || '';
      const allowLocalhost = process.env.ALLOW_LOCALHOST === 'true';

      const allowedOrigins: string[] = [];
      if (allowedOriginsEnv) {
        allowedOrigins.push(...allowedOriginsEnv.split(',').map((o) => o.trim()));
      }
      if (frontendUrl) {
        allowedOrigins.push(frontendUrl);
      }
      allowedOrigins.push(
        'https://cloud.mantrixflow.com',
        'https://cloud.api.mantrixflow.com',
        'https://cloud.api.etl.server.mantrixflow.com',
      );
      if (allowLocalhost) {
        allowedOrigins.push('http://localhost:3000', 'http://localhost:3001');
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
  private notifyListeners: Array<{ channel: string; handler: (payload: string) => void }> = [];

  constructor(private readonly configService: ConfigService) {}

  async onModuleInit() {
    this.logger.log('Initializing Pipeline Updates Gateway...');

    // Connect to Postgres to listen for NOTIFY events
    await this.setupPostgresListeners();

    this.logger.log('Pipeline Updates Gateway initialized');
  }

  async onModuleDestroy() {
    this.logger.log('Shutting down Pipeline Updates Gateway...');

    // Remove listeners
    for (const listener of this.notifyListeners) {
      if (this.pgClient) {
        await this.pgClient.query(`UNLISTEN ${listener.channel}`);
      }
    }

    // Close Postgres connection
    if (this.pgClient) {
      await this.pgClient.end();
      this.pgClient = null;
    }

    this.logger.log('Pipeline Updates Gateway shut down');
  }

  /**
   * Setup Postgres LISTEN for NOTIFY events
   */
  private async setupPostgresListeners() {
    try {
      const databaseUrl = this.configService.get<string>('DATABASE_URL');
      if (!databaseUrl) {
        this.logger.warn('DATABASE_URL not found. Real-time updates via NOTIFY will not work.');
        return;
      }

      // Create a dedicated connection for listening
      const { Pool } = await import('pg');
      const pool = new Pool({ connectionString: databaseUrl });
      this.pgClient = await pool.connect();

      // Listen for pipeline updates
      await this.pgClient.query('LISTEN pipeline_updates');
      await this.pgClient.query('LISTEN pipeline_run_updates');

      // Handle notifications
      this.pgClient.on('notification', (msg: any) => {
        this.handlePostgresNotification(msg.channel, msg.payload);
      });

      this.logger.log(
        'Postgres NOTIFY listeners set up for pipeline_updates and pipeline_run_updates',
      );

      // Store listeners for cleanup
      this.notifyListeners = [
        { channel: 'pipeline_updates', handler: (payload) => this.handlePipelineUpdate(payload) },
        {
          channel: 'pipeline_run_updates',
          handler: (payload) => this.handlePipelineRunUpdate(payload),
        },
      ];
    } catch (error) {
      this.logger.error(`Failed to setup Postgres listeners: ${error}`);
    }
  }

  /**
   * Handle Postgres NOTIFY notification
   */
  private handlePostgresNotification(channel: string, payload: string) {
    try {
      if (channel === 'pipeline_updates') {
        this.handlePipelineUpdate(payload);
      } else if (channel === 'pipeline_run_updates') {
        this.handlePipelineRunUpdate(payload);
      }
    } catch (error) {
      this.logger.error(`Error handling notification from ${channel}: ${error}`);
    }
  }

  /**
   * Handle pipeline update notification
   */
  private handlePipelineUpdate(payload: string) {
    try {
      const update: PipelineUpdatePayload = JSON.parse(payload);
      const { pipeline_id, organization_id } = update;

      // Broadcast to pipeline room
      this.server.to(`pipeline_${pipeline_id}`).emit('update', {
        type: 'pipeline',
        ...update,
      });

      // Also broadcast to organization room (for dashboard views)
      this.server.to(`org_${organization_id}`).emit('pipeline_update', {
        ...update,
      });

      this.logger.debug(`Broadcasted pipeline update for ${pipeline_id}`);
    } catch (error) {
      this.logger.error(`Error parsing pipeline update: ${error}`);
    }
  }

  /**
   * Handle pipeline run update notification
   */
  private handlePipelineRunUpdate(payload: string) {
    try {
      const update: PipelineRunUpdatePayload = JSON.parse(payload);
      const { pipeline_id, run_id } = update;

      // Broadcast to pipeline room
      this.server.to(`pipeline_${pipeline_id}`).emit('run_update', {
        type: 'run',
        ...update,
      });

      // Broadcast to run-specific room
      this.server.to(`run_${run_id}`).emit('update', {
        type: 'run',
        ...update,
      });

      this.logger.debug(`Broadcasted run update for ${run_id} (pipeline: ${pipeline_id})`);
    } catch (error) {
      this.logger.error(`Error parsing run update: ${error}`);
    }
  }

  /**
   * Handle client connection
   */
  handleConnection(client: Socket) {
    this.logger.log(`Client connected: ${client.id}`);
  }

  /**
   * Handle client disconnection
   */
  handleDisconnect(client: Socket) {
    this.logger.log(`Client disconnected: ${client.id}`);
  }

  /**
   * Join pipeline room to receive updates
   */
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

    // Join pipeline room
    client.join(`pipeline_${pipelineId}`);
    this.logger.log(`Client ${client.id} joined pipeline_${pipelineId}`);

    // Also join organization room if provided
    if (organizationId) {
      client.join(`org_${organizationId}`);
      this.logger.log(`Client ${client.id} joined org_${organizationId}`);
    }

    client.emit('joined', { pipelineId, organizationId });
  }

  /**
   * Leave pipeline room
   */
  @SubscribeMessage('leave_pipeline')
  handleLeavePipeline(
    @MessageBody() data: { pipelineId: string },
    @ConnectedSocket() client: Socket,
  ) {
    const { pipelineId } = data;

    if (pipelineId) {
      client.leave(`pipeline_${pipelineId}`);
      this.logger.log(`Client ${client.id} left pipeline_${pipelineId}`);
    }

    client.emit('left', { pipelineId });
  }

  /**
   * Join pipeline run room
   */
  @SubscribeMessage('join_run')
  handleJoinRun(@MessageBody() data: { runId: string }, @ConnectedSocket() client: Socket) {
    const { runId } = data;

    if (runId) {
      client.join(`run_${runId}`);
      this.logger.log(`Client ${client.id} joined run_${runId}`);
      client.emit('joined_run', { runId });
    }
  }
}
