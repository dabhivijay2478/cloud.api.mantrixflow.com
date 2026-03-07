/**
 * Internal ETL Controller
 * Endpoints called by the Python ETL pods (internal network only, no auth).
 *
 * - POST /internal/etl-callback — receive sync results from ETL pods
 * - GET  /internal/singer-state/:pipelineId — serve Singer state for --state file
 */

import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Logger,
  NotFoundException,
  Param,
  Post,
} from '@nestjs/common';
import { ApiExcludeController } from '@nestjs/swagger';
import { DataSourceConnectionRepository } from '../data-sources/repositories/data-source-connection.repository';
import { PipelineSourceSchemaRepository } from './repositories/pipeline-source-schema.repository';
import { PipelineRepository } from './repositories/pipeline.repository';
import { PipelineLifecycleService } from './services/pipeline-lifecycle.service';
import { PgmqQueueService } from '../queue';
import { PipelineStatus } from './types/pipeline-lifecycle.types';
import { sanitizeEtlError } from './utils/sanitize-etl-error';

interface EtlCallbackPayload {
  job_id: string;
  pipeline_id: string;
  organization_id: string;
  status: 'completed' | 'failed' | 'interrupted';
  rows_upserted?: number;
  rows_deleted?: number;
  lsn_end?: number;
  singer_state?: Record<string, unknown> | null;
  error?: string;
  duration_seconds?: number;
  source_tool?: string;
  dest_tool?: string;
  replication_method_used?: string;
}

@ApiExcludeController()
@Controller('internal')
export class InternalEtlController {
  private readonly logger = new Logger(InternalEtlController.name);

  constructor(
    private readonly pipelineRepository: PipelineRepository,
    private readonly pipelineSourceSchemaRepository: PipelineSourceSchemaRepository,
    private readonly connectionRepository: DataSourceConnectionRepository,
    private readonly lifecycleService: PipelineLifecycleService,
    private readonly pipelineQueueService: PgmqQueueService,
  ) {}

  /**
   * POST /internal/etl-callback
   * Called by ETL pods when a sync completes (or fails).
   */
  @Post('etl-callback')
  @HttpCode(HttpStatus.OK)
  async etlCallback(@Body() payload: EtlCallbackPayload) {
    const {
      job_id: runId,
      pipeline_id: pipelineId,
      organization_id: organizationId,
      status,
      rows_upserted: rowsUpserted = 0,
      rows_deleted: rowsDeleted = 0,
      lsn_end: lsnEnd,
      singer_state: singerState,
      error: errorMessage,
      duration_seconds: durationSeconds = 0,
      source_tool: sourceTool = 'tap-postgres',
      dest_tool: destTool = 'target-postgres',
      replication_method_used: replicationMethodUsed,
    } = payload;

    this.logger.log(
      `ETL callback: pipeline=${pipelineId} run=${runId} status=${status} rows=${rowsUpserted}`,
    );

    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (!pipeline) {
      this.logger.warn(`ETL callback for unknown pipeline ${pipelineId}`);
      return { received: true, warning: 'Pipeline not found' };
    }

    // Determine run status
    const runStatus = status === 'completed' ? 'success' : 'failed';
    const jobState = status === 'interrupted' ? 'failed' : 'completed';
    const sanitizedError = sanitizeEtlError(errorMessage);

    // Update pipeline_runs row
    try {
      await this.pipelineRepository.updateRun(runId, {
        status: runStatus,
        jobState,
        rowsRead: rowsUpserted,
        rowsWritten: rowsUpserted,
        rowsFailed: status === 'failed' ? 1 : 0,
        completedAt: new Date(),
        durationSeconds: Math.round(durationSeconds),
        errorMessage: sanitizedError,
      });
    } catch (err) {
      this.logger.error(`Failed to update run ${runId}: ${err}`);
    }

    // Save singer_state to pipeline (opaque blob — never parsed)
    if (singerState && status === 'completed') {
      try {
        await this.pipelineRepository.update(pipelineId, {
          checkpoint: singerState,
        });
      } catch (err) {
        this.logger.error(`Failed to save singer state for ${pipelineId}: ${err}`);
      }
    }

    // Determine target pipeline status after run
    const syncMode = pipeline.syncMode || 'full';
    let targetStatus: PipelineStatus;
    if (status !== 'completed') {
      targetStatus = PipelineStatus.FAILED;
    } else if (syncMode === 'cdc' || syncMode === 'log_based' || syncMode === 'incremental') {
      targetStatus = PipelineStatus.LISTING;
    } else {
      targetStatus = PipelineStatus.IDLE;
    }

    // Save replication slot to connection when first LOG_BASED run completes
    if (
      status === 'completed' &&
      (syncMode === 'cdc' || syncMode === 'log_based') &&
      replicationMethodUsed === 'LOG_BASED'
    ) {
      try {
        const sourceSchema = await this.pipelineSourceSchemaRepository.findById(
          pipeline.sourceSchemaId,
        );
        if (sourceSchema?.dataSourceId) {
          const connection = await this.connectionRepository.findByDataSourceId(
            sourceSchema.dataSourceId,
          );
          if (connection && !connection.replicationSlotName) {
            const slotName =
              'mxf_' +
              connection.id.replace(/-/g, '').slice(0, 8);
            await this.connectionRepository.updateByDataSourceId(
              sourceSchema.dataSourceId,
              { replicationSlotName: slotName },
            );
            this.logger.log(
              `Saved replication slot ${slotName} to connection for data source ${sourceSchema.dataSourceId}`,
            );
          }
        }
      } catch (err) {
        this.logger.error(
          `Failed to save replication slot for pipeline ${pipelineId}: ${err}`,
        );
      }
    }

    // Update cumulative stats on pipeline
    const newTotalProcessed = (pipeline.totalRowsProcessed || 0) + rowsUpserted;
    const setFullRefreshCompletedAt =
      status === 'completed' &&
      (syncMode === 'cdc' || syncMode === 'log_based') &&
      replicationMethodUsed === 'FULL_TABLE';
    try {
      await this.pipelineRepository.update(pipelineId, {
        lastRunAt: new Date(),
        lastRunStatus: runStatus,
        status: targetStatus,
        totalRowsProcessed: newTotalProcessed,
        totalRunsSuccessful:
          (pipeline.totalRunsSuccessful || 0) + (status === 'completed' ? 1 : 0),
        totalRunsFailed:
          (pipeline.totalRunsFailed || 0) + (status !== 'completed' ? 1 : 0),
        lastSyncAt: new Date(),
        ...(sanitizedError ? { lastError: sanitizedError } : {}),
        ...(setFullRefreshCompletedAt ? { fullRefreshCompletedAt: new Date() } : {}),
      });
    } catch (err) {
      this.logger.error(`Failed to update pipeline ${pipelineId}: ${err}`);
    }

    // Publish Socket.io status update
    if (this.pipelineQueueService.isReady()) {
      await this.pipelineQueueService.publishStatusUpdate({
        pipelineId,
        organizationId,
        status: targetStatus,
        rowsProcessed: newTotalProcessed,
        newRowsCount: rowsUpserted,
        error: sanitizedError || undefined,
        timestamp: new Date().toISOString(),
      });
    }

    return { received: true };
  }

  /**
   * GET /internal/singer-state/:pipelineId
   * Called by ETL pods before starting a LOG_BASED sync to get previous state.
   */
  @Get('singer-state/:pipelineId')
  async getSingerState(@Param('pipelineId') pipelineId: string) {
    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    const singerState = (pipeline.checkpoint as Record<string, unknown>) ?? null;

    return { singer_state: singerState };
  }
}
