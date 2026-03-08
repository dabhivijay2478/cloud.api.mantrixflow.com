/**
 * Connector Metadata Service
 * listConnectors: reads from static config (no ETL call).
 * discover, preview, health: call Singer-based Python ETL (apps/new-etl).
 */

import { HttpService } from '@nestjs/axios';
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { normalizeEtlBaseUrl } from '../../common/utils/etl-url';
import * as connectorsConfig from '../../config/connectors.json';
import { findSourceConnector, resolveSourceConnectorType } from './utils/connector-resolver';

@Injectable()
export class ConnectorMetadataService {
  private readonly logger = new Logger(ConnectorMetadataService.name);
  private readonly baseUrl: string;
  private readonly token: string;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
  ) {
    const raw =
      this.configService.get<string>('ETL_PYTHON_SERVICE_URL') ??
      this.configService.get<string>('PYTHON_SERVICE_URL') ??
      '';
    this.baseUrl = normalizeEtlBaseUrl(raw);
    this.token =
      this.configService.get<string>('SUPABASE_SERVICE_ROLE_KEY') ??
      this.configService.get<string>('ETL_PYTHON_SERVICE_TOKEN') ??
      '';
  }

  private headers(): Record<string, string> {
    return {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${this.token}`,
    };
  }

  async listConnectors(): Promise<{
    sources: Array<{ id: string; type?: string; label: string; category?: string; cdc?: boolean }>;
    destinations: Array<{ id: string; label: string }>;
  }> {
    return connectorsConfig as {
      sources: Array<{
        id: string;
        type?: string;
        label: string;
        category?: string;
        cdc?: boolean;
      }>;
      destinations: Array<{ id: string; label: string }>;
    };
  }

  /**
   * Test connection to source via ETL — POST /test-connection.
   * Call before discover to validate connectivity.
   */
  async testConnection(options: {
    source_type: string;
    source_config: Record<string, unknown>;
  }): Promise<{ success: boolean; error?: string }> {
    if (!this.baseUrl) {
      return { success: false, error: 'ETL service not configured' };
    }
    try {
      const res = await firstValueFrom(
        this.httpService.post(
          `${this.baseUrl}/test-connection`,
          {
            source_type: resolveSourceConnectorType(options.source_type).registryType,
            connection_config: options.source_config ?? {},
            source_config: options.source_config ?? {},
          },
          { headers: this.headers(), timeout: 10_000 },
        ),
      );
      const success = res.data?.success === true;
      return {
        success,
        error: success ? undefined : (res.data?.error ?? 'Connection test failed'),
      };
    } catch (err: any) {
      const detail = err?.response?.data?.detail ?? err?.response?.data?.error ?? err?.message;
      this.logger.warn(`ETL test connection failed: ${detail}`);
      return { success: false, error: String(detail ?? 'Connection test failed') };
    }
  }

  async health(): Promise<object> {
    if (!this.baseUrl) return { status: 'unconfigured' };
    try {
      const res = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/health`, {
          headers: this.headers(),
          timeout: 5000,
        }),
      );
      return res.data;
    } catch (err) {
      this.logger.warn(
        `ETL health check failed: ${err instanceof Error ? err.message : String(err)}`,
      );
      return { status: 'unreachable' };
    }
  }

  async discover(options: {
    source_type: string;
    source_config: Record<string, unknown>;
  }): Promise<{ streams?: Array<{ name: string; columns?: string[] }> }> {
    if (!this.baseUrl) return { streams: [] };
    try {
      const full = await this.discoverFull(options);
      return { streams: full.streams ?? [] };
    } catch (err) {
      this.logger.warn(`ETL discover failed: ${err instanceof Error ? err.message : String(err)}`);
      return { streams: [] };
    }
  }

  /**
   * Discover full schema from Singer ETL — POST /discover.
   * Returns parsed columns, primary_keys, streams from tap-postgres --discover.
   */
  async discoverFull(options: {
    source_type: string;
    source_config: Record<string, unknown>;
    schema_name?: string;
    table_name?: string;
    query?: string;
  }): Promise<{
    columns?: Array<{ name: string; type?: string; table?: string; nullable?: boolean }>;
    primary_keys?: string[];
    estimated_row_count?: number;
    streams?: Array<{ name: string }>;
    schemas?: Array<{
      name: string;
      tables: Array<{ name: string; schema: string; type?: string }>;
    }>;
  }> {
    if (!this.baseUrl) {
      throw new Error('ETL service not configured. Set ETL_PYTHON_SERVICE_URL.');
    }
    let res: { data?: any };
    try {
      res = await firstValueFrom(
        this.httpService.post(
          `${this.baseUrl}/discover`,
          {
            connection_config: options.source_config ?? {},
            schema_name: options.schema_name ?? 'public',
            source_type: resolveSourceConnectorType(options.source_type).registryType,
          },
          { headers: this.headers(), timeout: 120_000 },
        ),
      );
    } catch (err: any) {
      const detail = err?.response?.data?.detail ?? err?.response?.data?.message ?? err?.message;
      this.logger.error(`ETL discover failed: ${detail}`);
      throw new Error(`Schema discovery failed: ${detail || 'ETL service returned an error'}`);
    }
    const data = res.data ?? {};

    // Singer ETL returns { streams: [...], raw_catalog: {...} }
    // Each stream has: stream_name, tap_stream_id, columns, primary_keys, replication_methods
    const singerStreams = (data.streams ?? []) as Array<{
      stream_name: string;
      tap_stream_id: string;
      columns: Array<{ name: string; type: string; nullable: boolean; is_primary_key?: boolean }>;
      primary_keys: string[];
      replication_methods: string[];
      log_based_eligible: boolean;
    }>;

    // Filter by table_name if provided
    let matchedStreams = singerStreams;
    if (options.table_name) {
      matchedStreams = singerStreams.filter(
        (s) =>
          s.stream_name.endsWith(`.${options.table_name}`) ||
          s.stream_name === options.table_name ||
          s.tap_stream_id.includes(options.table_name!),
      );
      if (matchedStreams.length === 0) matchedStreams = singerStreams;
    }

    // Flatten columns from all matched streams
    const columns: Array<{ name: string; type?: string; table?: string; nullable?: boolean }> = [];
    const primaryKeys: string[] = [];
    const streamNames: Array<{ name: string }> = [];

    // Group streams by schema for the schemas response
    const schemaMap = new Map<string, Array<{ name: string; schema: string; type?: string }>>();

    for (const s of matchedStreams) {
      const parts = s.stream_name.split('-');
      const schemaName = parts.length >= 2 ? parts[parts.length - 2] : 'public';
      const tableName = parts.length >= 2 ? parts[parts.length - 1] : s.stream_name;

      streamNames.push({ name: s.stream_name });

      for (const col of s.columns) {
        columns.push({
          name: col.name,
          type: col.type,
          table: tableName,
          nullable: col.nullable,
        });
      }
      if (s.primary_keys.length > 0) {
        primaryKeys.push(...s.primary_keys);
      }

      if (!schemaMap.has(schemaName!)) {
        schemaMap.set(schemaName!, []);
      }
      schemaMap.get(schemaName!)!.push({
        name: tableName!,
        schema: schemaName!,
        type: 'table',
      });
    }

    const schemas = Array.from(schemaMap.entries()).map(([name, tables]) => ({
      name,
      tables,
    }));

    return {
      columns,
      primary_keys: [...new Set(primaryKeys)],
      streams: streamNames,
      schemas,
    };
  }

  async preview(options: {
    source_type: string;
    source_config: Record<string, unknown>;
    source_stream: string;
    limit?: number;
    transform_script?: string;
  }): Promise<{
    records?: unknown[];
    columns?: unknown[];
    total?: number;
    stream?: string;
    warning?: string;
  }> {
    if (!this.baseUrl) {
      return { records: [], columns: [], total: 0, stream: options.source_stream };
    }
    try {
      const res = await firstValueFrom(
        this.httpService.post(
          `${this.baseUrl}/preview`,
          {
            connection_config: options.source_config,
            source_stream: options.source_stream,
            limit: options.limit ?? 50,
            source_type: resolveSourceConnectorType(options.source_type).registryType,
            ...(options.transform_script ? { transform_script: options.transform_script } : {}),
          },
          { headers: this.headers(), timeout: 120_000 },
        ),
      );
      return res.data ?? {};
    } catch (err) {
      this.logger.warn(`ETL preview failed: ${err instanceof Error ? err.message : String(err)}`);
      throw err;
    }
  }

  async getCdcSetup(sourceType: string): Promise<{
    source_type: string;
    cdc_providers?: Array<{ id: string; label: string; instructions?: Record<string, unknown> }>;
    cdc_verify_steps?: string[];
    instructions?: string[];
  }> {
    const connector = findSourceConnector(sourceType);
    if (connector?.cdc_providers) {
      return {
        source_type: sourceType,
        cdc_providers: connector.cdc_providers as Array<{
          id: string;
          label: string;
          instructions?: Record<string, unknown>;
        }>,
        cdc_verify_steps: (connector.cdc_verify_steps as string[]) ?? [
          'wal_level',
          'wal2json',
          'replication_role',
          'replication_test',
        ],
      };
    }
    return {
      source_type: sourceType,
      instructions: [
        'Ensure wal2json extension is installed on your PostgreSQL server',
        'Set wal_level = logical in postgresql.conf',
        'Restart PostgreSQL after configuration changes',
        'LOG_BASED sync stays blocked unless source DB mutations are explicitly allowed by platform policy',
      ],
    };
  }
}
