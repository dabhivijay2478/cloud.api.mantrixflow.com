/**
 * Postgres Explorer Adapter
 * Streams rows from PostgreSQL tables for in-browser DuckDB loading
 */

import { Pool } from 'pg';
import type { IExplorerDbAdapter } from './explorer-db.adapter.interface';

export class PostgresExplorerAdapter implements IExplorerDbAdapter {
  readonly type = 'postgres';

  async *streamRows(params: {
    config: Record<string, unknown>;
    schema: string;
    table: string;
    limit: number;
  }): AsyncIterable<Record<string, unknown>> {
    const { config, schema, table, limit } = params;

    const host = config.host as string;
    const port = Number(config.port) || 5432;
    const database = config.database as string;
    const user = (config.username ?? config.user) as string;
    const password = config.password as string;
    const sslConfig = config.ssl as Record<string, unknown> | boolean | undefined;
    const useSsl =
      sslConfig !== undefined &&
      sslConfig !== null &&
      (sslConfig === true || (typeof sslConfig === 'object' && sslConfig.enabled === true));

    const pool = new Pool({
      host,
      port,
      database,
      user,
      password,
      ssl: useSsl ? { rejectUnauthorized: false } : undefined,
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
