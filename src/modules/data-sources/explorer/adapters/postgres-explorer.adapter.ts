/**
 * Postgres Explorer Adapter
 * Streams rows from PostgreSQL tables for in-browser DuckDB loading.
 * Also supports executing arbitrary SQL (JOINs, etc.) against the remote DB.
 * Handles PostgreSQL and Redshift (registry_type: postgres).
 */

import { Logger } from '@nestjs/common';
import { Pool } from 'pg';
import type {
  ExecuteQueryResult,
  IExplorerDbAdapter,
} from './explorer-db.adapter.interface';

/** Normalize config from various sources (tap-postgres uses dbname, node-pg uses database, etc.) */
function normalizePostgresConfig(config: Record<string, unknown>): {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
  ssl: boolean | { rejectUnauthorized: boolean } | undefined;
} {
  const host = (config.host as string) || '';
  const port = Number(config.port) || 5432;
  const database = (config.database ?? config.dbname) as string;
  const user = (config.username ?? config.user) as string;
  const password = (config.password ?? '') as string;

  const sslConfig = config.ssl as Record<string, unknown> | boolean | undefined;
  const useSsl =
    sslConfig !== undefined &&
    sslConfig !== null &&
    (sslConfig === true ||
      (typeof sslConfig === 'object' && sslConfig.enabled === true));

  return {
    host,
    port,
    database: database || '',
    user: user || '',
    password,
    ssl: useSsl ? { rejectUnauthorized: false } : undefined,
  };
}

export class PostgresExplorerAdapter implements IExplorerDbAdapter {
  readonly type = 'postgres';
  private readonly logger = new Logger(PostgresExplorerAdapter.name);

  async executeQuery(params: {
    config: Record<string, unknown>;
    query: string;
    maxRows?: number;
    timeoutMs?: number;
  }): Promise<ExecuteQueryResult> {
    const { config, query, maxRows = 10000, timeoutMs = 60000 } = params;

    const normalized = normalizePostgresConfig(config);
    this.logger.log(
      `Executing query against ${normalized.host}:${normalized.port}/${normalized.database}`,
    );

    const pool = new Pool({
      host: normalized.host,
      port: normalized.port,
      database: normalized.database,
      user: normalized.user,
      password: normalized.password,
      ssl: normalized.ssl,
      connectionTimeoutMillis: timeoutMs,
      statement_timeout: timeoutMs,
      max: 1,
    });

    let client;
    const start = Date.now();
    try {
      client = await pool.connect();
      await client.query({
        text: `SET search_path TO public, "$user"`,
        rowMode: 'object',
      });
      let sql = query.trim().replace(/;\s*$/, '');
      if (maxRows > 0 && /^\s*SELECT\b/i.test(sql) && !/\bLIMIT\s+\d+/i.test(sql)) {
        sql = `${sql} LIMIT ${Math.min(maxRows, 100000)}`;
      }
      const result = await client.query({
        text: sql,
        rowMode: 'object',
      });
      const executionTimeMs = Date.now() - start;

      const columns =
        result.fields?.map((f) => f.name) ??
        (result.rows[0]
          ? Object.keys(result.rows[0] as Record<string, unknown>)
          : []);

      const rows = (result.rows ?? []).map((r) => r as Record<string, unknown>);

      return {
        columns,
        rows,
        rowCount: rows.length,
        executionTimeMs,
      };
    } finally {
      client?.release();
      await pool.end();
    }
  }

  async *streamRows(params: {
    config: Record<string, unknown>;
    schema: string;
    table: string;
    limit: number;
  }): AsyncIterable<Record<string, unknown>> {
    const { config, schema, table, limit } = params;

    const normalized = normalizePostgresConfig(config);

    const pool = new Pool({
      host: normalized.host,
      port: normalized.port,
      database: normalized.database,
      user: normalized.user,
      password: normalized.password,
      ssl: normalized.ssl,
      connectionTimeoutMillis: 30_000,
      max: 1,
    });

    let client;
    try {
      client = await pool.connect();
      const quotedSchema = `"${schema.replace(/"/g, '""')}"`;
      const quotedTable = `"${table.replace(/"/g, '""')}"`;
      const sql = `SELECT * FROM ${quotedSchema}.${quotedTable} LIMIT $1`;

      const result = await client.query(sql, [limit]);

      for (const row of result.rows) {
        yield row as Record<string, unknown>;
      }
    } finally {
      client?.release();
      await pool.end();
    }
  }
}
