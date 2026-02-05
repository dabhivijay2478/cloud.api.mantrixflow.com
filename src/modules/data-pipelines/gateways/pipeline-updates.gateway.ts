/**
 * Pipeline Updates WebSocket Gateway
 * Handles real-time updates via Socket.io
 *
 * Architecture:
 * - Listens to Postgres NOTIFY events ('pipeline_updates', 'pipeline_run_updates')
 * - Subscribes to Redis channel 'pipeline-updates' for job status/progress from BullMQ
 * - Forwards notifications to Socket.io clients
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
import Redis from 'ioredis';
import { REDIS_PUBSUB_CHANNEL } from '../../queue/bullmq.module';

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
        allowedOrigins.push(...allowedOriginsEnv.split(',').map((o) => o.trim()).filter(Boolean));
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
  private redisSub: Redis | null = null;
  private notifyListeners: Array<{ channel: string; handler: (payload: string) => void }> = [];

  constructor(private readonly configService: ConfigService) {}

  async onModuleInit() {
    this.logger.log('Initializing Pipeline Updates Gateway...');

    await this.setupPostgresListeners();
    await this.setupRedisSubscriber();

    this.logger.log('Pipeline Updates Gateway initialized');
  }

  async onModuleDestroy() {
    this.logger.log('Shutting down Pipeline Updates Gateway...');

    for (const listener of this.notifyListeners) {
      if (this.pgClient) {
        await this.pgClient.query(`UNLISTEN ${listener.channel}`);
      }
    }
    if (this.pgClient) {
      await this.pgClient.end();
      this.pgClient = null;
    }
    if (this.redisSub) {
      await this.redisSub.unsubscribe(REDIS_PUBSUB_CHANNEL);
      await this.redisSub.quit();
      this.redisSub = null;
    }

    this.logger.log('Pipeline Updates Gateway shut down');
  }

  /**
   * Subscribe to Redis channel for real-time status/progress from BullMQ job handlers.
   */
  private async setupRedisSubscriber(): Promise<void> {
    const redisUrl = this.configService.get<string>('REDIS_URL');
    if (!redisUrl) {
      this.logger.warn('REDIS_URL not set. Redis pub/sub updates will not be received.');
      return;
    }
    try {
      this.redisSub = new Redis(redisUrl, { maxRetriesPerRequest: null });
      this.redisSub.on('error', (err) => this.logger.error(`Redis subscriber error: ${err.message}`));
      this.redisSub.subscribe(REDIS_PUBSUB_CHANNEL, (err) => {
        if (err) this.logger.error(`Redis subscribe error: ${err.message}`);
      });
      this.redisSub.on('message', (channel: string, message: string) => {
        if (channel === REDIS_PUBSUB_CHANNEL) {
          this.handleRedisPipelineUpdate(message);
        }
      });
      this.logger.log(`Subscribed to Redis channel: ${REDIS_PUBSUB_CHANNEL}`);
    } catch (error) {
      this.logger.error(`Failed to setup Redis subscriber: ${error}`);
    }
  }

  /**
   * Handle status update from Redis (published by PipelineQueueService / job processors).
   */
  private handleRedisPipelineUpdate(message: string): void {
    try {
      const data = JSON.parse(message) as {
        pipelineId: string;
        organizationId: string;
        status: string;
        rowsProcessed?: number;
        newRowsCount?: number;
        error?: string;
        timestamp: string;
      };
      const { pipelineId, organizationId, status, rowsProcessed, newRowsCount, error, timestamp } =
        data;

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
      this.logger.debug(`Broadcasted Redis pipeline update for ${pipelineId}`);
    } catch (error) {
      this.logger.error(`Error parsing Redis pipeline update: ${error}`);
    }
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
