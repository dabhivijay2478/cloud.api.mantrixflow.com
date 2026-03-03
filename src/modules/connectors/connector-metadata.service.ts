/**
 * Connector Metadata Service
 * listConnectors: reads from static config (no ETL call).
 * discover, preview, getCdcSetup, health: call Python ETL (apps/new-etl).
 */

import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { normalizeEtlBaseUrl } from '../../common/utils/etl-url';
import * as connectorsConfig from '../../config/connectors.json';

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
      sources: Array<{ id: string; type?: string; label: string; category?: string; cdc?: boolean }>;
      destinations: Array<{ id: string; label: string }>;
    };
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
      this.logger.warn(`ETL health check failed: ${err instanceof Error ? err.message : String(err)}`);
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
      this.logger.warn(
        `ETL discover failed: ${err instanceof Error ? err.message : String(err)}`,
      );
      return { streams: [] };
    }
  }

  /**
   * Discover full schema from ETL (columns, primary_keys, streams, etc.)
   * Used by discover-schema endpoint for Add Collector / pipeline flow.
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
    schemas?: Array<{ name: string; tables: Array<{ name: string; schema: string; type?: string }> }>;
  }> {
    if (!this.baseUrl) {
      throw new Error('ETL service not configured. Set ETL_PYTHON_SERVICE_URL.');
    }
    const etlSourceType = this.toEtlSourceType(options.source_type);
    const res = await firstValueFrom(
      this.httpService.post(
        `${this.baseUrl}/discover-schema/${etlSourceType}`,
        {
          source_type: etlSourceType,
          connection_config: options.source_config,
          source_config: options.source_config,
          schema_name: options.schema_name ?? 'public',
          table_name: options.table_name,
          query: options.query,
        },
        { headers: this.headers(), timeout: 30000 },
      ),
    );
    const data = res.data ?? {};
    const columns = (data.columns ?? []) as Array<{
      name: string;
      type?: string;
      table?: string;
      nullable?: boolean;
    }>;
    const streams = (data.streams ?? []) as Array<{ name: string }>;

    // Normalize: ensure columns have table when missing (derive from stream name)
    if (columns.length > 0 && streams.length > 0) {
      const normalizedColumns = columns.map((col) => {
        if (col.table) return col;
        // Single stream: use table name from stream
        if (streams.length === 1) {
          const parts = streams[0]!.name.split('.');
          const tableName = parts.length > 1 ? parts[1]! : streams[0]!.name;
          return { ...col, table: tableName };
        }
        return col;
      });
      return {
        columns: normalizedColumns,
        primary_keys: data.primary_keys ?? [],
        estimated_row_count: data.estimated_row_count,
        streams,
        schemas: data.schemas,
      };
    }

    return {
      columns,
      primary_keys: data.primary_keys ?? [],
      estimated_row_count: data.estimated_row_count,
      streams,
      schemas: data.schemas,
    };
  }

  private toEtlSourceType(type: string): string {
    const t = (type ?? 'postgres').toLowerCase();
    if (t === 'postgres' || t === 'postgresql') return 'source-postgres';
    if (t === 'mongodb') return 'source-mongodb-v2';
    return t.startsWith('source-') ? t : `source-${t}`;
  }

  async preview(options: {
    source_type: string;
    source_config: Record<string, unknown>;
    source_stream: string;
    limit?: number;
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
      const sourceType = this.normalizeSourceType(options.source_type);
      const res = await firstValueFrom(
        this.httpService.post(
          `${this.baseUrl}/preview`,
          {
            source_type: sourceType,
            source_config: options.source_config,
            source_stream: options.source_stream,
            limit: options.limit ?? 50,
          },
          { headers: this.headers(), timeout: 30000 },
        ),
      );
      return res.data ?? {};
    } catch (err) {
      this.logger.warn(
        `ETL preview failed: ${err instanceof Error ? err.message : String(err)}`,
      );
      throw err;
    }
  }

  private normalizeSourceType(type: string): string {
    const t = type.toLowerCase();
    if (t === 'postgres') return 'postgresql';
    return t;
  }

  async getCdcSetup(sourceType: string): Promise<object> {
    if (!this.baseUrl) {
      return { source_type: sourceType, message: 'ETL service not configured' };
    }
    try {
      const res = await firstValueFrom(
        this.httpService.get(`${this.baseUrl}/connectors/${encodeURIComponent(sourceType)}/cdc-setup`, {
          headers: this.headers(),
          timeout: 5000,
        }),
      );
      return res.data;
    } catch (err) {
      this.logger.warn(
        `ETL cdc-setup failed for ${sourceType}: ${err instanceof Error ? err.message : String(err)}`,
      );
      return { source_type: sourceType, error: String(err) };
    }
  }
}
