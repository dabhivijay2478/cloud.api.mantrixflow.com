/**
 * Internal Controller — Endpoints called by ETL server and pg_cron
 *
 * - POST /internal/etl-callback — ETL server POSTs when a pipeline run finishes
 * - POST /internal/connections/resolve — ETL fetches decrypted configs by connection IDs
 * - POST /internal/process-etl-jobs — pg_cron trigger (optional, NestJS also polls pgmq)
 */

import {
  Body,
  Controller,
  Headers,
  Inject,
  Logger,
  Post,
  UnauthorizedException,
} from '@nestjs/common';
import { eq, sql } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import { pipelineRuns, pipelines } from '../../database/schemas';
import {
  resolveDestinationConnectorType,
  resolveSourceConnectorType,
} from '../connectors/utils/connector-resolver';
import { ConnectionService } from '../data-sources/connection.service';
import { DataSourceRepository } from '../data-sources/repositories/data-source.repository';

export class ResolveConnectionsDto {
  organization_id!: string;
  source_conn_id!: string;
  dest_conn_id!: string;
}

export class EtlCallbackDto {
  jobId!: string;
  pgmqMsgId?: string;
  status!: 'completed' | 'failed';
  rowsSynced?: number;
  errorMessage?: string;
  new_cursor?: string;
  newState?: Record<string, unknown>;
  cdc_position?: Record<string, unknown>;
  sync_mode?: string;
}

@Controller('internal')
export class InternalController {
  private readonly logger = new Logger(InternalController.name);

  constructor(
    @Inject('DRIZZLE_DB') private readonly db: NodePgDatabase<any>,
    private readonly connectionService: ConnectionService,
    private readonly dataSourceRepository: DataSourceRepository,
  ) {}

  /**
   * Resolve connection IDs to decrypted configs.
   * ETL server calls this instead of receiving credentials in the sync request.
   */
  @Post('connections/resolve')
  async resolveConnections(
    @Headers('x-internal-token') token: string,
    @Body() body: ResolveConnectionsDto,
  ) {
    if (token !== process.env.INTERNAL_TOKEN) {
      throw new UnauthorizedException('Invalid internal token');
    }

    const { organization_id, source_conn_id, dest_conn_id } = body;

    const [sourceDataSource, destDataSource, sourceConfig, destConfig] = await Promise.all([
      this.dataSourceRepository.findById(source_conn_id),
      this.dataSourceRepository.findById(dest_conn_id),
      this.connectionService.getDecryptedConnection(organization_id, source_conn_id, 'system'),
      this.connectionService.getDecryptedConnection(organization_id, dest_conn_id, 'system'),
    ]);

    const sourceType = resolveSourceConnectorType(sourceDataSource?.sourceType).registryType;
    const destType = resolveDestinationConnectorType(destDataSource?.sourceType).registryType;

    return {
      source: { type: sourceType, config: sourceConfig },
      dest: { type: destType, config: destConfig },
    };
  }

  @Post('etl-callback')
  async etlCallback(@Headers('x-internal-token') token: string, @Body() body: EtlCallbackDto) {
    if (token !== process.env.INTERNAL_TOKEN) {
      throw new UnauthorizedException('Invalid internal token');
    }

    const {
      jobId,
      pgmqMsgId,
      status,
      rowsSynced,
      errorMessage,
      new_cursor,
      newState,
      cdc_position,
      sync_mode,
    } = body;

    // Find run by jobId (we use run.id as job_id when dispatching)
    const [run] = await this.db
      .select()
      .from(pipelineRuns)
      .where(eq(pipelineRuns.id, jobId))
      .limit(1);

    if (!run) {
      this.logger.warn(`etl-callback: no run found for jobId ${jobId}`);
      return { received: true };
    }

    const runStatus = status === 'completed' ? 'success' : 'failed';
    const existingMeta = (run.runMetadata as Record<string, unknown>) ?? {};
    const runMetadata = {
      batchSize: (existingMeta.batchSize as number) ?? 500,
      ...existingMeta,
      cdc_position: cdc_position ?? existingMeta.cdc_position,
      sync_mode: sync_mode ?? existingMeta.sync_mode,
    };

    await this.db
      .update(pipelineRuns)
      .set({
        status: runStatus,
        jobState: status === 'completed' ? 'completed' : 'failed',
        rowsWritten: rowsSynced ?? 0,
        runMetadata: runMetadata as any,
        errorMessage: errorMessage ?? undefined,
        completedAt: new Date(),
        jobStateUpdatedAt: new Date(),
      })
      .where(eq(pipelineRuns.id, run.id));

    // Update pipeline on successful completion
    if (status === 'completed' && run.pipelineId) {
      const [pipeline] = await this.db
        .select()
        .from(pipelines)
        .where(eq(pipelines.id, run.pipelineId))
        .limit(1);
      if (pipeline) {
        const isFullSync =
          sync_mode?.toLowerCase() === 'full' || sync_mode === 'FULL_TABLE';
        const updates: Record<string, unknown> = {
          lastRunAt: new Date(),
          lastRunStatus: runStatus,
          lastError: errorMessage ?? undefined,
          updatedAt: new Date(),
        };
        if (isFullSync) {
          updates.fullRefreshCompletedAt = new Date();
          updates.syncMode = 'log_based';
        }
        if (newState) {
          const checkpoint = {
            ...(pipeline.checkpoint as Record<string, unknown>),
            ...newState,
            cursor_value: new_cursor ?? (newState as any).cursor_value,
            lsn: (newState as any).lsn,
            binlog_file: (newState as any).binlog_file,
            binlog_position: (newState as any).binlog_position,
            state_blob: (newState as any).state_blob,
          };
          updates.checkpoint = checkpoint;
          updates.lastSyncValue = new_cursor ?? undefined;
        }
        await this.db
          .update(pipelines)
          .set(updates as any)
          .where(eq(pipelines.id, run.pipelineId));
      }
    }

    // ACK pgmq message if provided
    if (pgmqMsgId) {
      try {
        await this.db.execute(sql`
          SELECT pgmq.delete('pipeline_jobs', ${pgmqMsgId}::bigint)
        `);
      } catch (e) {
        this.logger.warn(`pgmq.delete failed for msg ${pgmqMsgId}: ${e}`);
      }
    }

    this.logger.log(
      `Run ${run.id} [${status}]: ${rowsSynced ?? 0} rows | mode=${sync_mode} | cursor=${new_cursor}`,
    );
    return { received: true };
  }

  @Post('process-etl-jobs')
  async processEtlJobs(@Headers('x-internal-token') token: string) {
    if (token !== process.env.INTERNAL_TOKEN) {
      throw new UnauthorizedException('Invalid internal token');
    }

    // Optional: process pgmq messages (NestJS PipelineJobProcessor already polls)
    // This endpoint can be called by pg_cron as a backup trigger
    this.logger.log('process-etl-jobs: called (pgmq polling handled by PipelineJobProcessor)');
    return { dispatched: 0, failed: 0, message: 'Use PipelineJobProcessor for pgmq' };
  }
}
