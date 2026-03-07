/**
 * Python ETL Service Client
 * HTTP client for calling Python FastAPI Singer-based ETL microservice
 */

import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { normalizeEtlBaseUrl } from '../../../common/utils/etl-url';
import { DataSourceRepository } from '../../data-sources/repositories/data-source.repository';
import { ConnectionService } from '../../data-sources/connection.service';
import type { ColumnInfo, IntrospectedColumn } from '../types/common.types';
import type { PipelineSourceSchema, PipelineDestinationSchema } from '../../../database/schemas';
import type { DiscoveredColumn } from '../../../database/schemas/data-pipelines/source-schemas/pipeline-source-schemas.schema';
import { parseTransformOutputMappings } from '../utils/transform-parser';

const DEFAULT_DISCOVER_TIMEOUT_MS = 120_000;
const DEFAULT_PREVIEW_TIMEOUT_MS = 120_000;
const DEFAULT_SYNC_TIMEOUT_MS = 30_000;

@Injectable()
export class PythonETLService {
  private readonly logger = new Logger(PythonETLService.name);
  private readonly pythonServiceUrl: string;
  private readonly pythonServiceAuthToken: string;
  private readonly nestjsInternalUrl: string;

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

    this.nestjsInternalUrl =
      this.configService.get<string>('NESTJS_INTERNAL_URL') ?? 'http://localhost:5000';

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
   * Discover schema from source via Singer catalog discovery.
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
      const discoverUrl = `${this.pythonServiceUrl}/discover`;
      this.assertValidRequestUrl(discoverUrl, 'discover');

      const response = await firstValueFrom(
        this.httpService.post(
          discoverUrl,
          {
            connection_config: connectionConfig,
            schema_name: sourceSchema.sourceSchema,
          },
          this.buildRequestConfig(this.discoverTimeoutMs),
        ),
      );

      const { streams } = response.data as {
        streams: Array<{
          stream: string;
          schema: { properties: Record<string, { type: string | string[] }> };
          key_properties: string[];
          metadata?: any[];
        }>;
        raw_catalog: any;
      };

      const targetStream =
        sourceSchema.sourceSchema && sourceSchema.sourceTable
          ? `${sourceSchema.sourceSchema}-${sourceSchema.sourceTable}`
          : sourceSchema.sourceTable || '';

      const matched = streams.find((s) => s.stream === targetStream) ?? streams[0];

      if (!matched) {
        return { columns: [], primaryKeys: [] };
      }

      const columns: ColumnInfo[] = Object.entries(matched.schema.properties).map(
        ([name, prop]) => {
          const resolvedType = Array.isArray(prop.type)
            ? prop.type.find((t) => t !== 'null') || 'string'
            : prop.type;
          return {
            name,
            type: resolvedType,
            dataType: resolvedType,
            nullable: Array.isArray(prop.type) ? prop.type.includes('null') : false,
          };
        },
      );

      return {
        columns,
        primaryKeys: matched.key_properties || [],
      };
    } catch (error: any) {
      const detail = this.extractPythonError(error, 'Schema discovery');
      this.logger.error(`Schema discovery failed: ${detail}`, error.stack);
      throw new Error(`Schema discovery failed: ${detail}`);
    }
  }

  /**
   * Preview first N rows from source, optionally with transform applied.
   */
  async preview(options: {
    sourceSchema: PipelineSourceSchema;
    connectionConfig: any;
    limit?: number;
    destinationSchema?: PipelineDestinationSchema | null;
    transformScript?: string | null;
    columnMap?: Record<string, string> | null;
    dropColumns?: string[] | null;
  }): Promise<{
    records: any[];
    columns: string[];
    total: number;
    stream: string;
  }> {
    const {
      sourceSchema,
      connectionConfig,
      limit = 50,
      destinationSchema,
      transformScript,
      columnMap,
      dropColumns,
    } = options;

    const sourceStream =
      sourceSchema.sourceSchema && sourceSchema.sourceTable
        ? `${sourceSchema.sourceSchema}-${sourceSchema.sourceTable}`
        : sourceSchema.sourceTable || '';

    if (!sourceStream) {
      throw new Error('Source table/stream is required for preview');
    }

    const script = transformScript ?? destinationSchema?.transformScript ?? undefined;
    const column_map = columnMap ?? undefined;
    const drop_columns = dropColumns ?? undefined;

    const previewUrl = `${this.pythonServiceUrl}/preview`;
    this.assertValidRequestUrl(previewUrl, 'preview');

    const sourceType = this.toRegistryType(sourceSchema.sourceType || 'postgres');

    const response = await firstValueFrom(
      this.httpService.post(
        previewUrl,
        {
          connection_config: connectionConfig,
          source_stream: sourceStream,
          limit,
          source_type: sourceType,
          transform_script: script || undefined,
          column_map: column_map || undefined,
          drop_columns: drop_columns || undefined,
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
   * Derive output_column_sql_types from transform mappings and source/destination columns.
   * Prefers introspected source columns (real PG types); falls back to discovered; then
   * destination introspection (ensures UUID etc. match destination table).
   */
  private deriveOutputColumnSqlTypes(
    transformScript: string | null | undefined,
    discoveredColumns: DiscoveredColumn[] | null | undefined,
    introspectedSourceColumns?: Array<{ name: string; data_type: string }> | null,
    introspectedDestColumns?: Array<{ name: string; data_type: string }> | null,
  ): Record<string, string> | undefined {
    if (!transformScript) return undefined;
    const mappings = parseTransformOutputMappings(transformScript);
    if (mappings.size === 0) return undefined;

    const result: Record<string, string> = {};
    // 1. Prefer introspected source (real PG types e.g. uuid)
    if (introspectedSourceColumns?.length) {
      const srcMap = new Map(
        introspectedSourceColumns.map((c) => [c.name.toLowerCase(), c.data_type]),
      );
      for (const [outCol, srcCol] of mappings) {
        const pgType = srcMap.get(srcCol.toLowerCase());
        if (pgType) result[outCol] = pgType;
      }
    }
    // 2. Fall back to discovered columns (tap may use "string" for UUID)
    if (Object.keys(result).length === 0 && discoveredColumns?.length) {
      const srcMap = new Map(
        discoveredColumns.map((c) => [c.name.toLowerCase(), c.dataType]),
      );
      for (const [outCol, srcCol] of mappings) {
        const dataType = srcMap.get(srcCol.toLowerCase());
        if (!dataType) continue;
        const pgType = this.discoveryTypeToPgType(dataType);
        if (pgType) result[outCol] = pgType;
      }
    }
    // 3. Fill gaps from destination introspection (ensures UUID->UUID when dest has uuid)
    if (introspectedDestColumns?.length) {
      const destMap = new Map(
        introspectedDestColumns.map((c) => [c.name.toLowerCase(), c.data_type]),
      );
      for (const [outCol] of mappings) {
        if (!result[outCol]) {
          const pgType = destMap.get(outCol.toLowerCase());
          if (pgType) result[outCol] = pgType;
        }
      }
    }
    return Object.keys(result).length > 0 ? result : undefined;
  }

  private discoveryTypeToPgType(dataType: string): string {
    const t = (dataType || '').toLowerCase().trim();
    if (t === 'uuid') return 'uuid';
    if (t === 'integer' || t === 'int') return 'bigint';
    if (t === 'number') return 'double precision';
    if (t === 'boolean' || t === 'bool') return 'boolean';
    if (t === 'object') return 'jsonb';
    if (t === 'array') return 'jsonb';
    if (t === 'character varying' || t === 'varchar') return 'character varying';
    if (t === 'text' || t === 'string') return 'text';
    return 'text';
  }

  /**
   * Submit an async Singer sync job to the ETL service.
   * Returns a job ID on acceptance or a retry signal when the pod is at capacity.
   */
  async runSync(options: {
    jobId: string;
    pipelineId: string;
    organizationId: string;
    sourceSchema: PipelineSourceSchema;
    destinationSchema: PipelineDestinationSchema;
    sourceConnectionConfig: any;
    destConnectionConfig: any;
    sourceType?: string;
    destType?: string;
    userId: string;
    syncMode?: 'full' | 'incremental' | 'cdc';
    writeMode?: 'append' | 'upsert' | 'replace';
    upsertKey?: string | string[];
    columnMap?: Record<string, string>;
    dropColumns?: string[];
    hardDelete?: boolean;
    replicationSlotName?: string;
  }): Promise<{ jobId: string; status: string } | { retry: true }> {
    const {
      jobId,
      pipelineId,
      organizationId,
      sourceSchema,
      destinationSchema,
      sourceConnectionConfig,
      destConnectionConfig,
      sourceType = 'postgres',
      destType = 'postgres',
      syncMode = 'full',
      writeMode = 'append',
      upsertKey,
      columnMap,
      dropColumns,
      hardDelete,
      replicationSlotName,
    } = options;

    const replicationMethod = this.mapSyncModeToReplicationMethod(syncMode);

    const sourceStream =
      sourceSchema.sourceSchema && sourceSchema.sourceTable
        ? `${sourceSchema.sourceSchema}-${sourceSchema.sourceTable}`
        : sourceSchema.sourceTable || '';

    if (!sourceStream) {
      throw new Error('Source table/stream is required');
    }

    const syncUrl = `${this.pythonServiceUrl}/sync`;
    this.assertValidRequestUrl(syncUrl, 'sync');

    const nestjsCallbackUrl = `${this.nestjsInternalUrl}/api/internal/etl-callback`;
    const nestjsStateUrl = `${this.nestjsInternalUrl}/api/internal/singer-state`;

    const emitMethod = writeMode === 'upsert' ? 'upsert' : writeMode === 'replace' ? 'replace' : 'append';

    let introspectedSource: { columns: IntrospectedColumn[] } | null = null;
    let introspectedDest: { columns: IntrospectedColumn[] } | null = null;
    if (destinationSchema.transformScript && sourceSchema.sourceTable) {
      try {
        introspectedSource = await this.introspectTable({
          connectionConfig: sourceConnectionConfig,
          schemaName: sourceSchema.sourceSchema || 'public',
          tableName: sourceSchema.sourceTable,
        });
      } catch {
        // Non-blocking: fall back to discovered columns
      }
    }
    if (destinationSchema.transformScript && destinationSchema.destinationTable) {
      try {
        introspectedDest = await this.introspectTable({
          connectionConfig: destConnectionConfig,
          schemaName: destinationSchema.destinationSchema || 'public',
          tableName: destinationSchema.destinationTable,
        });
      } catch {
        // Non-blocking: destination introspection optional
      }
    }

    const outputColumnSqlTypes = this.deriveOutputColumnSqlTypes(
      destinationSchema.transformScript,
      sourceSchema.discoveredColumns as DiscoveredColumn[] | null,
      introspectedSource?.columns,
      introspectedDest?.columns,
    );

    const payload: Record<string, unknown> = {
      job_id: jobId,
      pipeline_id: pipelineId,
      organization_id: organizationId,
      source_connection_config: sourceConnectionConfig,
      dest_connection_config: destConnectionConfig,
      source_type: sourceType,
      dest_type: destType,
      replication_method: replicationMethod,
      source_stream: sourceStream,
      dest_table: destinationSchema.destinationTable || sourceSchema.sourceTable,
      dest_schema: destinationSchema.destinationSchema || undefined,
      replication_slot_name: replicationSlotName || undefined,
      column_map: columnMap || undefined,
      drop_columns: dropColumns || undefined,
      transform_script: destinationSchema.transformScript || undefined,
      output_column_sql_types: outputColumnSqlTypes || undefined,
      emit_method: emitMethod,
      upsert_key: upsertKey != null
        ? (Array.isArray(upsertKey) ? upsertKey : [upsertKey])
        : undefined,
      hard_delete: hardDelete ?? false,
      nestjs_callback_url: nestjsCallbackUrl,
      nestjs_state_url: nestjsStateUrl,
    };

    try {
      const response = await firstValueFrom(
        this.httpService.post(syncUrl, payload, {
          ...this.buildRequestConfig(this.syncTimeoutMs),
          validateStatus: (status: number) => status < 500 || status === 503,
        }),
      );

      if (response.status === 503) {
        this.logger.warn('ETL pod at capacity — signalling caller to requeue');
        return { retry: true };
      }

      if (response.data?.error) {
        throw new Error(response.data.error);
      }

      return {
        jobId: response.data.job_id ?? jobId,
        status: response.data.status ?? 'accepted',
      };
    } catch (error: any) {
      if (error?.response?.status === 503) {
        this.logger.warn('ETL pod at capacity (caught) — signalling caller to requeue');
        return { retry: true };
      }
      const detail = this.extractPythonError(error, 'Sync');
      this.logger.error(`Sync failed: ${detail}`, error?.stack);
      throw new Error(`Sync failed: ${detail}`);
    }
  }

  /**
   * Get connection config for a source schema.
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
   * Introspect an existing destination table to get real PostgreSQL column
   * types (not Singer JSON Schema types). Returns actual PG data_type,
   * identity info, etc.
   */
  async introspectTable(options: {
    connectionConfig: any;
    schemaName: string;
    tableName: string;
  }): Promise<{ columns: IntrospectedColumn[] }> {
    const { connectionConfig, schemaName, tableName } = options;

    const url = `${this.pythonServiceUrl}/introspect-table`;
    this.assertValidRequestUrl(url, 'introspect-table');

    try {
      const response = await firstValueFrom(
        this.httpService.post(
          url,
          {
            connection_config: connectionConfig,
            schema_name: schemaName,
            table_name: tableName,
          },
          this.buildRequestConfig(this.discoverTimeoutMs),
        ),
      );

      return response.data as { columns: IntrospectedColumn[] };
    } catch (error: any) {
      const detail = this.extractPythonError(error, 'Table introspection');
      this.logger.error(`Table introspection failed: ${detail}`, error?.stack);
      throw new Error(`Table introspection failed: ${detail}`);
    }
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

  /** Map connection type to Singer registry key. Only PostgreSQL is supported. */
  private toRegistryType(type: string): string {
    const t = (type || 'postgres').toLowerCase();
    if (t === 'postgres' || t === 'postgresql' || t === 'pgvector' || t === 'redshift') return 'postgres';
    throw new Error('Only PostgreSQL is supported');
  }

  /**
   * Map the caller's sync-mode string to a Singer replication method.
   */
  private mapSyncModeToReplicationMethod(syncMode: string): string {
    switch (syncMode) {
      case 'full':
      case 'full_table':
        return 'FULL_TABLE';
      case 'cdc':
      case 'log_based':
      case 'incremental':
        return 'LOG_BASED';
      default:
        throw new Error(
          `Unknown sync mode "${syncMode}". Supported: "full", "incremental", "log_based".`,
        );
    }
  }
}
