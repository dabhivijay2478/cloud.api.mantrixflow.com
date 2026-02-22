/**
 * ETL Service — Proxies requests to the new PyAirbyte ETL Server
 *
 * The ETL server has NO connections or sync-state endpoints.
 * NestJS owns connections (ConnectionService) and sync state (pipeline checkpoint).
 *
 * Auth: ETL uses Supabase JWT. Pass SUPABASE_SERVICE_ROLE_KEY as Bearer token.
 */

import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import type { Pipeline, PipelineDestinationSchema, PipelineRun, PipelineSourceSchema } from '../../database/schemas';
import { getFallbackConnectors } from './connector-registry.fallback';

@Injectable()
export class EtlService {
  private readonly logger = new Logger(EtlService.name);
  private readonly baseUrl: string;
  private readonly token: string;

  constructor(private readonly configService: ConfigService) {
    this.baseUrl =
      this.configService.get<string>('ETL_SERVICE_URL') ??
      this.configService.get<string>('ETL_PYTHON_SERVICE_URL') ??
      this.configService.get<string>('PYTHON_SERVICE_URL') ??
      '';
    this.token =
      this.configService.get<string>('SUPABASE_SERVICE_ROLE_KEY') ??
      this.configService.get<string>('ETL_TOKEN') ??
      '';
    if (!this.baseUrl) {
      this.logger.warn('ETL_SERVICE_URL or ETL_PYTHON_SERVICE_URL not set');
    }
    if (!this.token) {
      this.logger.warn('SUPABASE_SERVICE_ROLE_KEY not set — ETL requests will fail auth');
    }
  }

  private headers(): Record<string, string> {
    return {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${this.token}`,
    };
  }

  private async fetch<T>(path: string, init?: RequestInit): Promise<T> {
    const url = `${this.baseUrl.replace(/\/$/, '')}${path}`;
    const res = await fetch(url, {
      ...init,
      headers: { ...this.headers(), ...init?.headers },
    });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`ETL ${path}: ${res.status} ${text}`);
    }
    return res.json() as Promise<T>;
  }

  // ── Health ────────────────────────────────────────────────────────────────

  async health(): Promise<object> {
    return this.fetch('/health');
  }

  // ── Connectors ─────────────────────────────────────────────────────────────

  async listConnectors(): Promise<{
    sources: Array<{ id: string; type?: string; label: string; category?: string; cdc?: boolean }>;
    destinations: Array<{ id: string; label: string }>;
  }> {
    try {
      const data = await this.fetch<{
        sources: Array<{ id: string; type?: string; label: string; category?: string; cdc?: boolean }>;
        destinations: Array<{ id: string; label: string }>;
      }>('/connectors');
      return data;
    } catch (err) {
      this.logger.warn(
        `ETL /connectors unreachable, using fallback registry: ${err instanceof Error ? err.message : String(err)}`,
      );
      return getFallbackConnectors();
    }
  }

  async getCdcSetup(sourceType: string): Promise<object> {
    return this.fetch(`/connectors/${encodeURIComponent(sourceType)}/cdc-setup`);
  }

  // ── Testing + Discovery ────────────────────────────────────────────────────

  async testConnection(body: object): Promise<{ success: boolean; message?: string }> {
    return this.fetch('/test-connection', {
      method: 'POST',
      body: JSON.stringify(body),
    });
  }

  async discover(body: object): Promise<object> {
    return this.fetch('/discover', {
      method: 'POST',
      body: JSON.stringify(body),
    });
  }

  async preview(body: object): Promise<object> {
    return this.fetch('/preview', {
      method: 'POST',
      body: JSON.stringify(body),
    });
  }

  // ── Collector / Emitter / Transformer ───────────────────────────────────────

  async collect(body: object): Promise<object> {
    return this.fetch('/collect', {
      method: 'POST',
      body: JSON.stringify(body),
    });
  }

  async emit(body: object): Promise<object> {
    return this.fetch('/emit', {
      method: 'POST',
      body: JSON.stringify(body),
    });
  }

  async transform(body: object): Promise<object> {
    return this.fetch('/transform', {
      method: 'POST',
      body: JSON.stringify(body),
    });
  }

  // ── Sync dispatch ──────────────────────────────────────────────────────────

  /**
   * Dispatch a pipeline run to the ETL server.
   * Sends connection IDs only — ETL resolves configs from NestJS internal API.
   * No inline credentials. ETL runs async and POSTs to callback_url when done.
   */
  async dispatchSync(
    pipeline: Pipeline & { sourceSchema?: PipelineSourceSchema | null; destinationSchema?: PipelineDestinationSchema | null },
    run: PipelineRun,
    pgmqMsgId?: string,
  ): Promise<object> {
    if (!pipeline.sourceSchema?.dataSourceId || !pipeline.destinationSchema?.dataSourceId) {
      throw new Error('Pipeline must have source and destination data sources');
    }

    const sourceStream =
      pipeline.sourceSchema.sourceTable ?? pipeline.sourceSchema.sourceQuery ?? 'stream';
    const destTable = pipeline.destinationSchema.destinationTable ?? 'output';

    const syncMode = (pipeline.syncMode as string) || 'full';
    const endpoint =
      syncMode === 'full'
        ? '/sync/full'
        : syncMode === 'incremental'
          ? '/sync/incremental'
          : syncMode === 'cdc'
            ? '/sync/cdc'
            : '/sync/run-pipeline';

    const initialState = this.buildInitialState(pipeline);

    const body = {
      job_id: run.id,
      pgmq_msg_id: pgmqMsgId ?? undefined,
      pipeline_id: pipeline.id,
      organization_id: pipeline.organizationId,
      source_conn_id: pipeline.sourceSchema.dataSourceId,
      dest_conn_id: pipeline.destinationSchema.dataSourceId,
      source_stream: sourceStream,
      dest_table: destTable,
      sync_mode: syncMode,
      write_mode: pipeline.destinationSchema?.writeMode ?? 'append',
      upsert_key: (() => {
        const uk = pipeline.destinationSchema?.upsertKey;
        if (Array.isArray(uk) && uk.length > 0) return uk[0];
        return typeof uk === 'string' ? uk : undefined;
      })(),
      column_map: this.buildColumnMap(pipeline),
      transformations: pipeline.transformations ?? [],
      cursor_field: pipeline.incrementalColumn ?? undefined,
      initial_state: initialState,
      callback_url: `${this.configService.get('INTERNAL_API_URL') ?? ''}/internal/etl-callback`,
      callback_token: this.configService.get('INTERNAL_TOKEN'),
    };

    return this.fetch(endpoint, {
      method: 'POST',
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(10_000),
    });
  }

  private toEtlSourceType(type: string): string {
    const t = (type ?? 'postgres').toLowerCase();
    if (t === 'postgres' || t === 'postgresql') return 'source-postgres';
    if (t === 'mongodb') return 'source-mongodb-v2';
    if (t === 'mysql') return 'source-mysql';
    if (t === 'mssql' || t === 'sqlserver') return 'source-mssql';
    return t.startsWith('source-') ? t : `source-${t}`;
  }

  private toEtlDestType(type: string): string {
    const t = (type ?? 'postgres').toLowerCase();
    if (t === 'postgres' || t === 'postgresql') return 'postgres';
    if (t === 'mongodb') return 'mongodb';
    if (t === 'mysql') return 'mysql';
    if (t === 'mssql' || t === 'sqlserver') return 'mssql';
    return t;
  }

  private buildInitialState(
    pipeline: Pipeline & { sourceSchema?: PipelineSourceSchema | null; destinationSchema?: PipelineDestinationSchema | null },
  ): object | null {
    const cp = pipeline.checkpoint as Record<string, unknown> | null;
    if (!cp) return null;
    return {
      pipeline_id: pipeline.id,
      source_type: pipeline.sourceSchema?.sourceType ?? '',
      stream_name: pipeline.sourceSchema?.sourceTable ?? '',
      sync_mode: pipeline.syncMode ?? 'incremental',
      cursor_field: pipeline.incrementalColumn ?? cp.watermarkField,
      cursor_value: (cp.lastValue ?? cp.last_value) as string | undefined,
      lsn: (cp.lsn ?? cp.walPosition) as string | undefined,
      binlog_file: cp.binlog_file as string | undefined,
      binlog_position: cp.binlog_position as number | undefined,
      state_blob: cp,
    };
  }

  private buildColumnMap(pipeline: Pipeline): Array<{ from_col: string; to_col: string }> {
    const transforms =
      (pipeline.transformations as Array<{ sourceColumn?: string; destinationColumn?: string }>) ??
      [];
    return transforms
      .filter((t) => t.sourceColumn && t.destinationColumn)
      .map((t) => ({ from_col: t.sourceColumn!, to_col: t.destinationColumn! }));
  }

  // ── Sync state (NestJS-owned, not proxied to ETL) ───────────────────────────
  // ETL has no /sync-state. NestJS stores state in pipeline.checkpoint.
  // These methods read/write from the pipeline record.
}
