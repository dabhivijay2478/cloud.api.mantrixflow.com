/**
 * Explorer DB Adapter Interface
 * Abstraction for streaming rows from different RDBMS (Postgres, MySQL, Snowflake, etc.)
 * Adding a new database requires only implementing this interface and registering the adapter.
 */

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
}
