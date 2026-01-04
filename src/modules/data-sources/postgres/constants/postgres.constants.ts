/**
 * PostgreSQL Connector Constants
 */

/**
 * Connection Configuration Defaults
 */
export const CONNECTION_DEFAULTS = {
  PORT: 5432,
  CONNECTION_TIMEOUT_MS: 30000, // 30 seconds
  IDLE_TIMEOUT_MS: 300000, // 5 minutes
  QUERY_TIMEOUT_MS: 60000, // 60 seconds
  MAX_POOL_SIZE: 10,
  MIN_POOL_SIZE: 1,
  MAX_CONNECTIONS_PER_ORG: 10,
} as const;

/**
 * Export MAX_CONNECTIONS_PER_ORG separately for convenience
 */
export const MAX_CONNECTIONS_PER_ORG =
  CONNECTION_DEFAULTS.MAX_CONNECTIONS_PER_ORG;

/**
 * Schema Discovery Configuration
 */
export const SCHEMA_DISCOVERY = {
  CACHE_TTL_MS: 3600000, // 1 hour
  MAX_TABLES: 10000, // Maximum tables to discover
  BATCH_SIZE: 100, // Tables per batch query
} as const;

/**
 * Query Execution Configuration
 */
export const QUERY_CONFIG = {
  MAX_ROWS_PER_PAGE: 10000,
  DEFAULT_LIMIT: 1000,
  MAX_QUERY_LENGTH: 100000, // characters
  RATE_LIMIT_QUERIES_PER_HOUR: 100,
  SLOW_QUERY_THRESHOLD_MS: 10000, // 10 seconds
} as const;

/**
 * Sync Configuration
 */
export const SYNC_CONFIG = {
  BATCH_SIZE: 1000, // rows per batch
  MAX_RETRIES: 3,
  RETRY_DELAY_MS: 1000, // initial delay
  RETRY_MULTIPLIER: 2, // exponential backoff
  MAX_BATCH_SIZE: 10000,
} as const;

/**
 * Health Check Configuration
 */
export const HEALTH_CHECK = {
  INTERVAL_MS: 300000, // 5 minutes
  TIMEOUT_MS: 5000, // 5 seconds
} as const;

/**
 * Query Log Retention
 */
export const QUERY_LOG_RETENTION_DAYS = 90;

/**
 * Supported PostgreSQL Versions
 */
export const SUPPORTED_PG_VERSIONS = [10, 11, 12, 13, 14, 15, 16, 17] as const;

/**
 * PostgreSQL Type Mappings
 */
export const PG_TYPE_MAPPINGS: Record<string, string> = {
  // Numeric types
  smallint: 'number',
  integer: 'number',
  bigint: 'number',
  decimal: 'number',
  numeric: 'number',
  real: 'number',
  double: 'number',
  'double precision': 'number',
  smallserial: 'number',
  serial: 'number',
  bigserial: 'number',
  money: 'number',
  // Character types
  char: 'string',
  varchar: 'string',
  text: 'string',
  // Boolean
  boolean: 'boolean',
  // Date/Time
  date: 'Date',
  time: 'Date',
  'time without time zone': 'Date',
  'time with time zone': 'Date',
  timestamp: 'Date',
  'timestamp without time zone': 'Date',
  'timestamp with time zone': 'Date',
  interval: 'string',
  // Network
  inet: 'string',
  cidr: 'string',
  macaddr: 'string',
  // JSON
  json: 'object',
  jsonb: 'object',
  // Binary
  bytea: 'Buffer',
  // UUID
  uuid: 'string',
  // Arrays (will be detected separately)
  // Geometric (PostGIS)
  point: 'object',
  line: 'object',
  lseg: 'object',
  box: 'object',
  path: 'object',
  polygon: 'object',
  circle: 'object',
  // Vector (pg_vector)
  vector: 'number[]',
} as const;

/**
 * Dangerous SQL Keywords (for query sanitization)
 */
export const DANGEROUS_KEYWORDS = [
  'DROP',
  'DELETE',
  'UPDATE',
  'INSERT',
  'ALTER',
  'CREATE',
  'TRUNCATE',
  'GRANT',
  'REVOKE',
  'EXECUTE',
  'EXEC',
  'CALL',
] as const;

/**
 * Allowed SQL Keywords (only SELECT and related)
 */
export const ALLOWED_KEYWORDS = [
  'SELECT',
  'WITH',
  'FROM',
  'WHERE',
  'JOIN',
  'INNER',
  'LEFT',
  'RIGHT',
  'FULL',
  'OUTER',
  'ON',
  'GROUP',
  'BY',
  'HAVING',
  'ORDER',
  'LIMIT',
  'OFFSET',
  'UNION',
  'INTERSECT',
  'EXCEPT',
  'DISTINCT',
  'AS',
  'AND',
  'OR',
  'NOT',
  'IN',
  'EXISTS',
  'LIKE',
  'ILIKE',
  'IS',
  'NULL',
  'TRUE',
  'FALSE',
  'CASE',
  'WHEN',
  'THEN',
  'ELSE',
  'END',
  'CAST',
  '::',
] as const;

/**
 * Common Time Field Patterns
 */
export const TIME_FIELD_PATTERNS = [
  /created_at/i,
  /updated_at/i,
  /timestamp/i,
  /date/i,
  /time/i,
  /_at$/i,
  /_date$/i,
  /_time$/i,
] as const;

/**
 * Common Amount Field Patterns
 */
export const AMOUNT_FIELD_PATTERNS = [
  /amount/i,
  /price/i,
  /cost/i,
  /revenue/i,
  /total/i,
  /sum/i,
  /value/i,
] as const;

/**
 * Common Count Field Patterns
 */
export const COUNT_FIELD_PATTERNS = [
  /quantity/i,
  /count/i,
  /num_/i,
  /total_/i,
  /_count$/i,
] as const;

/**
 * Common ID Field Patterns
 */
export const ID_FIELD_PATTERNS = [/^id$/i, /_id$/i, /uuid/i] as const;
