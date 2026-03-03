/**
 * Python ETL Service Client
 * HTTP client for calling Python FastAPI ETL microservice
 * Replaces CollectorService, TransformerService, and EmitterService
 */

import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { normalizeEtlBaseUrl } from '../../../common/utils/etl-url';
import { DataSourceRepository } from '../../data-sources/repositories/data-source.repository';
import { ConnectionService } from '../../data-sources/connection.service';
import type { ColumnInfo } from '../types/common.types';
import type { PipelineSourceSchema, PipelineDestinationSchema } from '../../../database/schemas';

/** Default timeout values (override via env) */
const DEFAULT_DISCOVER_TIMEOUT_MS = 30_000;
const DEFAULT_PREVIEW_TIMEOUT_MS = 30_000;
const DEFAULT_SYNC_TIMEOUT_MS = 600_000;

@Injectable()
export class PythonETLService {
  private readonly logger = new Logger(PythonETLService.name);
  private readonly pythonServiceUrl: string;
  private readonly pythonServiceAuthToken: string;

  private readonly discoverTimeoutMs: number;
  private readonly previewTimeoutMs: number;
  private readonly syncTimeoutMs: number;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly dataSourceRepository: DataSourceRepository,
    private readonly connectionService: ConnectionService,
  ) {
    const raw =
      this.configService.get<string>('ETL_PYTHON_SERVICE_URL') ??
      this.configService.get<string>('PYTHON_SERVICE_URL');
    this.pythonServiceUrl = normalizeEtlBaseUrl(raw);
    this.pythonServiceAuthToken =
      this.configService.get<string>('SUPABASE_SERVICE_ROLE_KEY') ||
      this.configService.get<string>('ETL_PYTHON_SERVICE_TOKEN') ||
      '';
    if (!this.pythonServiceAuthToken) {
      this.logger.warn(
        'SUPABASE_SERVICE_ROLE_KEY not set — ETL requests will fail auth. Set it in apps/api/.env',
      );
    }
    if (!this.pythonServiceUrl) {
      const hint =
        raw != null && String(raw).trim().length > 0
          ? ` Value was normalized to an invalid URL (e.g. missing host). Set a valid base URL like http://localhost:8000 in apps/api/.env (from the api directory so .env is loaded).`
          : ' Set ETL_PYTHON_SERVICE_URL or PYTHON_SERVICE_URL in apps/api/.env and run the API from the api directory so .env is loaded.';
      throw new Error(
        `ETL_PYTHON_SERVICE_URL or PYTHON_SERVICE_URL must be a valid URL with host.${hint}`,
      );
    }
    this.logger.log(`Python ETL Service URL: ${this.pythonServiceUrl}`);

    this.discoverTimeoutMs =
      this.configService.get<number>('ETL_DISCOVER_TIMEOUT_MS') ?? DEFAULT_DISCOVER_TIMEOUT_MS;
    this.previewTimeoutMs =
      this.configService.get<number>('ETL_PREVIEW_TIMEOUT_MS') ?? DEFAULT_PREVIEW_TIMEOUT_MS;
    this.syncTimeoutMs =
      this.configService.get<number>('ETL_SYNC_TIMEOUT_MS') ?? DEFAULT_SYNC_TIMEOUT_MS;
  }

  private buildRequestConfig(timeout: number) {
    return {
      timeout,
      headers: {
        Authorization: `Bearer ${this.pythonServiceAuthToken}`,
      },
    };
  }

  /**
   * Ensures the request URL is valid before passing to axios.
   */
  private assertValidRequestUrl(url: string, label: string): void {
    if (!url || typeof url !== 'string') {
      throw new Error(
        `Python ETL ${label}: request URL is missing. Check ETL_PYTHON_SERVICE_URL (e.g. http://localhost:8000) and run the API from apps/api so .env is loaded.`,
      );
    }
    try {
      const parsed = new URL(url);
      if (!parsed.host) {
        throw new Error('URL has no host');
      }
    } catch (err: any) {
      throw new Error(
        `Python ETL ${label}: invalid URL "${url}". ${err?.message ?? ''} Set ETL_PYTHON_SERVICE_URL in apps/api/.env to a valid base URL (e.g. http://localhost:8000).`,
      );
    }
  }

  /**
   * Discover schema from source
   */
  async discoverSchema(options: {
    sourceSchema: PipelineSourceSchema;
    connectionConfig: any;
    organizationId: string;
    userId: string;
  }): Promise<{
    columns: ColumnInfo[];
    primaryKeys: string[];
    estimatedRowCount?: number;
  }> {
    const { sourceSchema, connectionConfig } = options;

    try {
      const sourceType = this.normalizeSourceType(sourceSchema.sourceType);
      const discoverUrl = `${this.pythonServiceUrl}/discover-schema/${sourceType}`;
      this.assertValidRequestUrl(discoverUrl, 'discover-schema');

      const response = await firstValueFrom(
        this.httpService.post(
          discoverUrl,
          {
            source_type: sourceType,
            connection_config: connectionConfig,
            source_config: sourceSchema.sourceConfig || {},
            table_name: sourceSchema.sourceTable,
            schema_name: sourceSchema.sourceSchema,
            query: sourceSchema.sourceQuery,
          },
          this.buildRequestConfig(this.discoverTimeoutMs),
        ),
      );

      return {
        columns: response.data.columns || [],
        primaryKeys: response.data.primary_keys || [],
        estimatedRowCount: response.data.estimated_row_count,
      };
    } catch (error: any) {
      const detail = this.extractPythonError(error, 'Schema discovery');
      this.logger.error(`Schema discovery failed: ${detail}`, error.stack);
      throw new Error(`Schema discovery failed: ${detail}`);
    }
  }

  /**
   * Preview first N rows from source (dlt-based, no transform/emit).
   */
  async preview(options: {
    sourceSchema: PipelineSourceSchema;
    connectionConfig: any;
    limit?: number;
  }): Promise<{
    records: any[];
    columns: string[];
    total: number;
    stream: string;
  }> {
    const { sourceSchema, connectionConfig, limit = 50 } = options;

    let sourceType: string;
    if (sourceSchema.sourceType) {
      sourceType = this.normalizeSourceType(sourceSchema.sourceType);
    } else {
      sourceType = this.normalizeSourceType(
        (await this.getDataSourceType(sourceSchema.dataSourceId!)) || 'postgresql',
      );
    }

    const sourceStream =
      sourceSchema.sourceSchema && sourceSchema.sourceTable
        ? `${sourceSchema.sourceSchema}.${sourceSchema.sourceTable}`
        : sourceSchema.sourceTable || '';

    if (!sourceStream) {
      throw new Error('Source table/stream is required for preview');
    }

    const previewUrl = `${this.pythonServiceUrl}/preview`;
    this.assertValidRequestUrl(previewUrl, 'preview');

    const response = await firstValueFrom(
      this.httpService.post(
        previewUrl,
        {
          source_type: sourceType,
          source_config: connectionConfig,
          source_stream: sourceStream,
          limit,
        },
        this.buildRequestConfig(this.previewTimeoutMs),
      ),
    );

    return {
      records: response.data.records || [],
      columns: response.data.columns || [],
      total: response.data.total ?? 0,
      stream: response.data.stream || sourceStream,
    };
  }

  /**
   * Run full sync via dlt — single call to ETL /sync/run-sync.
   * Replaces collect -> transform -> emit loop.
   */
  async runSync(options: {
    jobId: string;
    pipelineId: string;
    organizationId: string;
    sourceSchema: PipelineSourceSchema;
    destinationSchema: PipelineDestinationSchema;
    sourceConnectionConfig: any;
    destConnectionConfig: any;
    userId: string;
    syncMode?: 'full' | 'incremental' | 'cdc';
    writeMode?: 'append' | 'upsert' | 'replace';
    upsertKey?: string | string[];
    cursorField?: string;
    checkpoint?: any;
  }): Promise<{
    rowsSynced: number;
    syncMode: string;
    newCursor?: string;
    newState?: any;
    error?: string;
    userMessage?: string;
  }> {
    const {
      jobId,
      pipelineId,
      organizationId,
      sourceSchema,
      destinationSchema,
      sourceConnectionConfig,
      destConnectionConfig,
      syncMode = 'full',
      writeMode = 'append',
      upsertKey,
      cursorField,
      checkpoint,
    } = options;

    const sourceType = this.normalizeSourceType(
      (await this.getDataSourceType(sourceSchema.dataSourceId!)) || 'postgresql',
    );
    const destType = this.normalizeSourceType(
      (await this.getDataSourceType(destinationSchema.dataSourceId!)) || 'postgresql',
    );

    const sourceStream =
      sourceSchema.sourceSchema && sourceSchema.sourceTable
        ? `${sourceSchema.sourceSchema}.${sourceSchema.sourceTable}`
        : sourceSchema.sourceTable || '';

    if (!sourceStream) {
      throw new Error('Source table/stream is required');
    }

    const syncUrl = `${this.pythonServiceUrl}/sync/run-sync`;
    this.assertValidRequestUrl(syncUrl, 'sync/run-sync');

    const discoveredColumns = (sourceSchema as any).discoveredColumns as
      | Array<{ name: string }>
      | undefined;
    const selectedColumns = discoveredColumns?.map((c) => c.name) ?? undefined;

    const payload: Record<string, unknown> = {
      job_id: jobId,
      pipeline_id: pipelineId,
      organization_id: organizationId,
      source_config: sourceConnectionConfig,
      dest_config: destConnectionConfig,
      source_type: sourceType,
      dest_type: destType,
      source_stream: sourceStream,
      dest_table: destinationSchema.destinationTable || sourceSchema.sourceTable,
      sync_mode: syncMode,
      write_mode: writeMode,
      upsert_key: upsertKey != null
        ? (Array.isArray(upsertKey) ? upsertKey : [upsertKey])
        : undefined,
      cursor_field: cursorField,
      dataset_name: `org_${organizationId}`,
      dest_schema: destinationSchema.destinationSchema || undefined,
      destination_table_exists: destinationSchema.destinationTableExists ?? false,
      custom_sql: destinationSchema.customSql || undefined,
      transform_type: destinationSchema.transformType || 'dlt',
      transform_script: destinationSchema.transformScript || undefined,
      dbt_model: (destinationSchema as any).dbtModel || undefined,
      selected_columns: selectedColumns,
    };

    if (checkpoint) {
      payload.initial_state = {
        pipeline_id: pipelineId,
        source_type: sourceType,
        stream_name: sourceStream,
        sync_mode: syncMode,
        cursor_field: cursorField,
        cursor_value: checkpoint?.cursor_value ?? checkpoint?.cursorValue,
        state_blob: checkpoint,
      };
    }

    try {
      const response = await firstValueFrom(
        this.httpService.post(syncUrl, payload, this.buildRequestConfig(this.syncTimeoutMs)),
      );

      if (response.data?.error) {
        return {
          rowsSynced: 0,
          syncMode,
          error: response.data.error,
          userMessage: response.data.user_message,
        };
      }

      return {
        rowsSynced: response.data.rows_synced ?? 0,
        syncMode: response.data.sync_mode ?? syncMode,
        newCursor: response.data.new_cursor,
        newState: response.data.new_state,
      };
    } catch (error: any) {
      const detail = this.extractPythonError(error, 'Sync');
      this.logger.error(`Sync failed: ${detail}`, error?.stack);
      throw new Error(`Sync failed: ${detail}`);
    }
  }

  /**
   * Get connection config for a source schema
   */
  async getConnectionConfig(
    sourceSchema: PipelineSourceSchema,
    organizationId: string,
  ): Promise<any> {
    if (!sourceSchema.dataSourceId) {
      throw new Error('Source schema must have a data source ID');
    }

    const dataSource = await this.dataSourceRepository.findById(sourceSchema.dataSourceId);
    if (!dataSource) {
      throw new Error(`Data source ${sourceSchema.dataSourceId} not found`);
    }

    return await this.connectionService.getDecryptedConnection(
      organizationId,
      sourceSchema.dataSourceId,
      'system',
    );
  }

  /**
   * Extract the actual error detail from a Python FastAPI error response.
   */
  private extractPythonError(error: any, operation: string): string {
    const pythonDetail =
      error?.response?.data?.detail ||
      error?.response?.data?.message ||
      error?.response?.data?.error;

    if (pythonDetail) {
      const status = error?.response?.status || 'unknown';
      return `${operation} failed (HTTP ${status}): ${pythonDetail}`;
    }

    if (error?.code === 'ECONNABORTED') {
      return `${operation} timed out — Python ETL service did not respond in time`;
    }

    if (error?.code === 'ECONNREFUSED') {
      return `${operation} failed — Python ETL service is not running at ${this.pythonServiceUrl}`;
    }

    return error?.message || `${operation} failed with unknown error`;
  }

  /**
   * Delta check for CDC/incremental polling.
   * new-etl does not yet expose a delta-check endpoint; returns no changes.
   */
  async deltaCheck(options: {
    sourceSchema: PipelineSourceSchema;
    connectionConfig: any;
    checkpoint: Record<string, unknown>;
  }): Promise<{ hasChanges: boolean; checkpoint?: Record<string, unknown> }> {
    void options.sourceSchema;
    void options.connectionConfig;
    return { hasChanges: false, checkpoint: options.checkpoint };
  }

  /**
   * Normalize source type to match Python service expectations
   */
  private normalizeSourceType(sourceType: string): string {
    const normalized = sourceType.toLowerCase();
    if (normalized === 'postgres') {
      return 'postgresql';
    }
    return normalized;
  }

  /**
   * Get data source type from repository
   */
  private async getDataSourceType(dataSourceId: string): Promise<string> {
    const dataSource = await this.dataSourceRepository.findById(dataSourceId);
    if (!dataSource) {
      throw new Error(`Data source ${dataSourceId} not found`);
    }
    return dataSource.sourceType;
  }
}
