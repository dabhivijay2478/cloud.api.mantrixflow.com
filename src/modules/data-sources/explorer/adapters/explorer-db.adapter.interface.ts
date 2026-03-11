/**
 * Explorer DB Adapter Interface
 * Abstraction for streaming rows from different RDBMS (Postgres, MySQL, Snowflake, etc.)
 * Adding a new database requires only implementing this interface and registering the adapter.
 */

export interface ExecuteQueryResult {
  columns: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
  executionTimeMs: number;
}

export interface IExplorerDbAdapter {
  /** Connection type this adapter handles (e.g. 'postgres', 'mysql', 'snowflake') */
  readonly type: string;

  /** Stream rows from a table. Config is DB-specific (from getDecryptedConnection). */
  streamRows(params: {
    config: Record<string, unknown>;
    schema: string;
    table: string;
    limit: number;
  }): AsyncIterable<Record<string, unknown>>;

  /**
   * Execute arbitrary SQL against the remote database (JOINs, subqueries, etc.).
   * Like Snowflake/Redshift SQL editors - runs on the server, not in-browser.
   */
  executeQuery?(params: {
    config: Record<string, unknown>;
    query: string;
    maxRows?: number;
    timeoutMs?: number;
  }): Promise<ExecuteQueryResult>;
}
