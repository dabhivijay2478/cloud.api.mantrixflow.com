/**
 * ETL Jobs Service
 * Async job queue using pgmq (NO Redis, NO BullMQ)
 * Atomically inserts etl_jobs + pgmq.send, processes queue, handles callbacks
 */

import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import { eq, desc } from 'drizzle-orm';
import type { DrizzleDatabase } from '../../database/drizzle/database';
import { Inject } from '@nestjs/common';
import { etlJobs } from '../../database/schemas/etl-jobs';
import { PgmqQueueService } from '../queue/pgmq-queue.service';
import { PGMQ_QUEUE_NAMES } from '../queue/pgmq.constants';
import { PipelineRepository } from '../data-pipelines/repositories/pipeline.repository';
import { DataSourceRepository } from '../data-sources/repositories/data-source.repository';
import { DataSourceConnectionRepository } from '../data-sources/repositories/data-source-connection.repository';
import { ConnectionService } from '../data-sources/connection.service';
import type { EnqueueJobDto, EtlCallbackDto } from './dto';
import type { EtlJob } from '../../database/schemas/etl-jobs';

const SUPPORTED_DIRECTIONS = [
  'postgres-to-mongodb',
  'mongodb-to-postgres',
  'postgres-to-postgres',
  'mysql-to-postgres',
  'postgres-to-mysql',
  'mysql-to-mongodb',
  'mongodb-to-mysql',
  'mysql-to-mysql',
  'mongodb-to-mongodb',
] as const;

function normalizeSourceType(t: string): string {
  const lower = (t || '').trim().toLowerCase();
  if (lower === 'postgres' || lower === 'pg' || lower === 'pgvector' || lower === 'redshift' || lower === 'postgresql') return 'postgresql';
  if (lower === 'mysql' || lower === 'mariadb') return 'mysql';
  if (lower === 'mongodb' || lower === 'mongo') return 'mongodb';
  return lower;
}

function getDirectionForPipeline(
  sourceType: string,
  destType: string,
): (typeof SUPPORTED_DIRECTIONS)[number] | null {
  const s = normalizeSourceType(sourceType);
  const d = normalizeSourceType(destType);
  if (s === 'postgresql' && d === 'mongodb') return 'postgres-to-mongodb';
  if (s === 'mongodb' && d === 'postgresql') return 'mongodb-to-postgres';
  if (s === 'postgresql' && d === 'postgresql') return 'postgres-to-postgres';
  if ((s === 'mysql' || s === 'mariadb') && d === 'postgresql') return 'mysql-to-postgres';
  if (s === 'postgresql' && d === 'mysql') return 'postgres-to-mysql';
  if (s === 'mysql' && d === 'mongodb') return 'mysql-to-mongodb';
  if (s === 'mongodb' && d === 'mysql') return 'mongodb-to-mysql';
  if (s === 'mysql' && d === 'mysql') return 'mysql-to-mysql';
  if (s === 'mongodb' && d === 'mongodb') return 'mongodb-to-mongodb';
  return null;
}

@Injectable()
export class EtlJobsService {
  private readonly logger = new Logger(EtlJobsService.name);

  constructor(
    @Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase,
    private readonly configService: ConfigService,
    private readonly pgmqService: PgmqQueueService,
    private readonly pipelineRepository: PipelineRepository,
    private readonly dataSourceRepository: DataSourceRepository,
    private readonly connectionRepository: DataSourceConnectionRepository,
    private readonly connectionService: ConnectionService,
    private readonly httpService: HttpService,
  ) {}

  /**
   * Enqueue an ETL job atomically: INSERT etl_jobs + pgmq.send in one transaction
   */
  async enqueueJob(dto: EnqueueJobDto): Promise<string> {
    const pipelineWithSchemas = await this.pipelineRepository.findByIdWithSchemas(
      dto.pipelineId,
      dto.orgId,
    );
    if (!pipelineWithSchemas) {
      throw new NotFoundException(`Pipeline ${dto.pipelineId} not found`);
    }

    const { pipeline, sourceSchema, destinationSchema } = pipelineWithSchemas;

    if (pipeline.status === 'paused') {
      throw new BadRequestException('Pipeline is paused. Resume it before running.');
    }

    if (!sourceSchema.dataSourceId || !destinationSchema.dataSourceId) {
      throw new BadRequestException('Source and destination must have data source IDs');
    }

    const sourceDataSource = await this.dataSourceRepository.findById(
      sourceSchema.dataSourceId,
    );
    const destDataSource = await this.dataSourceRepository.findById(
      destinationSchema.dataSourceId,
    );
    if (!sourceDataSource || !destDataSource) {
      throw new BadRequestException('Source or destination data source not found');
    }

    const direction = getDirectionForPipeline(
      sourceDataSource.sourceType,
      destDataSource.sourceType,
    );
    if (!direction) {
      throw new BadRequestException(
        `Pipeline direction not supported. Supported: ${SUPPORTED_DIRECTIONS.join(', ')}. ` +
          `Your pipeline: ${sourceDataSource.sourceType} → ${destDataSource.sourceType}`,
      );
    }

    const sourceConnection = await this.connectionRepository.findByDataSourceId(
      sourceSchema.dataSourceId,
    );
    const destConnection = await this.connectionRepository.findByDataSourceId(
      destinationSchema.dataSourceId,
    );
    if (!sourceConnection || !destConnection) {
      throw new BadRequestException('Source or destination connection not found');
    }

    const sourceConnectionConfig = await this.connectionService.getDecryptedConnection(
      dto.orgId,
      sourceSchema.dataSourceId,
      'system',
    );
    const destConnectionConfig = await this.connectionService.getDecryptedConnection(
      dto.orgId,
      destinationSchema.dataSourceId,
      'system',
    );

    const meltanoJobId = `mantrix-${dto.pipelineId}-${Date.now()}`;
    const syncMode = dto.syncMode || pipeline.syncMode || 'full';

    const internalApiUrl = this.configService.get<string>('INTERNAL_API_URL');
    const internalToken = this.configService.get<string>('INTERNAL_TOKEN');
    const etlUrl = this.configService.get<string>('ETL_PYTHON_SERVICE_URL');
    const etlToken = this.configService.get<string>('ETL_PYTHON_SERVICE_TOKEN');

    if (!internalApiUrl || !internalToken || !etlUrl || !etlToken) {
      throw new BadRequestException(
        'INTERNAL_API_URL, INTERNAL_TOKEN, ETL_PYTHON_SERVICE_URL, ETL_PYTHON_SERVICE_TOKEN must be set',
      );
    }

    const callbackUrl = `${internalApiUrl.replace(/\/$/, '')}/internal/etl-callback`;

    const job = await this.pgmqService.runInTransaction(async (client) => {
      const insertResult = await client.query(
        `INSERT INTO etl_jobs (
          pipeline_id, org_id, status, sync_mode, direction,
          source_connection_id, dest_connection_id, meltano_job_id, payload
        ) VALUES ($1, $2, 'pending', $3, $4, $5, $6, $7, $8)
        RETURNING id`,
        [
          dto.pipelineId,
          dto.orgId,
          syncMode,
          direction,
          sourceConnection.id,
          destConnection.id,
          meltanoJobId,
          JSON.stringify({
            sourceConnectionId: sourceConnection.id,
            destConnectionId: destConnection.id,
            triggerType: dto.triggerType || 'manual',
          }),
        ],
      );
      const jobId = insertResult.rows[0].id;

      const pgmqPayload = {
        jobId,
        meltanoJobId,
        pipelineId: dto.pipelineId,
        orgId: dto.orgId,
        direction,
        syncMode,
        sourceConfig: sourceConnectionConfig,
        destConfig: destConnectionConfig,
        sourceTable: sourceSchema.sourceTable,
        sourceSchema: sourceSchema.sourceSchema || 'public',
        destTable: destinationSchema.destinationTable || sourceSchema.sourceTable,
        destSchema: destinationSchema.destinationSchema || 'public',
        stateId: `pipeline_${dto.pipelineId}`,
        callback_url: callbackUrl,
        callback_token: internalToken,
      };

      const sendResult = await client.query(
        `SELECT pgmq.send($1, $2::jsonb) as msg_id`,
        [PGMQ_QUEUE_NAMES.ETL_JOBS, JSON.stringify(pgmqPayload)],
      );
      const pgmqMsgId = Number(sendResult.rows[0]?.msg_id ?? 0);

      await client.query(
        `UPDATE etl_jobs SET pgmq_msg_id = $1, status = 'queued' WHERE id = $2`,
        [pgmqMsgId, jobId],
      );

      return { jobId, pgmqMsgId };
    });

    this.logger.log(
      `Enqueued ETL job ${job.jobId} for pipeline ${dto.pipelineId} (pgmq msg ${job.pgmqMsgId})`,
    );
    return job.jobId;
  }

  /**
   * Process queue: read from pgmq, mark running, dispatch to FastAPI (202)
   */
  async processQueue(qty: number = 5): Promise<number> {
    if (!this.pgmqService.isReady()) {
      this.logger.warn('PgMQ not ready, skipping processQueue');
      return 0;
    }

    const messages = await this.pgmqService.readMessages<{
      jobId: string;
      meltanoJobId: string;
      pipelineId: string;
      direction: string;
      syncMode: string;
      sourceConfig: any;
      destConfig: any;
      sourceTable?: string;
      sourceSchema?: string;
      destTable?: string;
      destSchema?: string;
      stateId?: string;
      callback_url: string;
      callback_token: string;
    }>(PGMQ_QUEUE_NAMES.ETL_JOBS, qty, 120);

    let processed = 0;
    const etlUrl =
      this.configService.get<string>('ETL_PYTHON_SERVICE_URL')?.replace(/\/$/, '') ||
      '';
    const etlToken = this.configService.get<string>('ETL_PYTHON_SERVICE_TOKEN');

    for (const msg of messages) {
      const payload = msg.message as any;
      if (!payload?.jobId || !payload?.callback_url) {
        this.logger.warn(`Invalid pgmq message, skipping: ${JSON.stringify(payload)}`);
        continue;
      }

      try {
        await this.db
          .update(etlJobs)
          .set({
            status: 'running',
            startedAt: new Date(),
          })
          .where(eq(etlJobs.id, payload.jobId));

        const runUrl = `${etlUrl}/run-meltano-pipeline`;
        const body = {
          jobId: payload.jobId,
          meltanoJobId: payload.meltanoJobId,
          direction: payload.direction,
          syncMode: payload.syncMode,
          source_connection_config: payload.sourceConfig,
          dest_connection_config: payload.destConfig,
          source_table: payload.sourceTable,
          source_schema: payload.sourceSchema,
          dest_table: payload.destTable,
          dest_schema: payload.destSchema,
          state_id: payload.stateId,
          callback_url: payload.callback_url,
          callback_token: payload.callback_token,
          pgmq_msg_id: Number(msg.msg_id),
        };

        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 10_000);

        const response = await firstValueFrom(
          this.httpService.post(runUrl, body, {
            headers: {
              'Content-Type': 'application/json',
              Authorization: `Bearer ${etlToken}`,
            },
            timeout: 10_000,
            signal: controller.signal,
          }),
        );
        clearTimeout(timeout);

        if (response.status === 202) {
          processed++;
          this.logger.log(`Dispatched job ${payload.jobId} to ETL (202)`);
        } else {
          this.logger.warn(
            `ETL returned ${response.status} for job ${payload.jobId}, expected 202`,
          );
          await this.markFailed(
            payload.jobId,
            `ETL returned ${response.status} instead of 202`,
          );
        }
      } catch (error: any) {
        const errMsg = error?.message || String(error);
        this.logger.error(`Failed to dispatch job ${payload.jobId}: ${errMsg}`);
        await this.markFailed(payload.jobId, errMsg);
      }
    }

    return processed;
  }

  /**
   * Handle callback from FastAPI when meltano run completes
   */
  async handleCallback(dto: EtlCallbackDto): Promise<void> {
    const job = await this.db
      .select()
      .from(etlJobs)
      .where(eq(etlJobs.id, dto.jobId))
      .limit(1);

    if (!job.length) {
      this.logger.warn(`Callback for unknown job ${dto.jobId}`);
      return;
    }

    await this.db
      .update(etlJobs)
      .set({
        status: dto.status === 'completed' ? 'completed' : 'failed',
        rowsSynced: dto.rowsSynced ?? job[0].rowsSynced,
        stateId: dto.stateId ?? job[0].stateId,
        errorMessage: dto.errorMessage ?? job[0].errorMessage,
        userMessage: dto.userMessage ?? job[0].userMessage,
        completedAt: new Date(),
      })
      .where(eq(etlJobs.id, dto.jobId));

    if (dto.status === 'completed' || dto.status === 'failed') {
      await this.pgmqService.deleteMessage(
        PGMQ_QUEUE_NAMES.ETL_JOBS,
        String(dto.pgmqMsgId),
      );
      this.logger.log(
        `Job ${dto.jobId} ${dto.status}, pgmq msg ${dto.pgmqMsgId} deleted`,
      );
    }
  }

  /**
   * Mark job as failed
   */
  async markFailed(id: string, userMessage: string): Promise<void> {
    await this.db
      .update(etlJobs)
      .set({
        status: 'failed',
        errorMessage: userMessage,
        userMessage,
        completedAt: new Date(),
      })
      .where(eq(etlJobs.id, id));
  }

  /**
   * Get a single ETL job by ID
   */
  async getJobById(jobId: string): Promise<EtlJob | null> {
    const [job] = await this.db
      .select()
      .from(etlJobs)
      .where(eq(etlJobs.id, jobId))
      .limit(1);
    return job ?? null;
  }

  /**
   * Get job history for a pipeline
   */
  async getJobsByPipeline(
    pipelineId: string,
    limit: number = 20,
  ): Promise<EtlJob[]> {
    return this.db
      .select()
      .from(etlJobs)
      .where(eq(etlJobs.pipelineId, pipelineId))
      .orderBy(desc(etlJobs.createdAt))
      .limit(limit);
  }
}
