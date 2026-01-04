/**
 * PostgreSQL Connector Error Codes
 * Standardized error codes for better error handling and debugging
 */

export enum PostgresErrorCode {
  // Connection Errors (PG_CONN_001 - PG_CONN_099)
  CONNECTION_TIMEOUT = 'PG_CONN_001',
  CONNECTION_REFUSED = 'PG_CONN_002',
  CONNECTION_LOST = 'PG_CONN_003',
  INVALID_CREDENTIALS = 'PG_CONN_004',
  DATABASE_NOT_FOUND = 'PG_CONN_005',
  HOST_NOT_FOUND = 'PG_CONN_006',
  SSL_ERROR = 'PG_CONN_007',
  SSH_TUNNEL_ERROR = 'PG_CONN_008',
  POOL_EXHAUSTED = 'PG_CONN_009',
  MAX_CONNECTIONS_EXCEEDED = 'PG_CONN_010',

  // Validation Errors (PG_VAL_001 - PG_VAL_099)
  INVALID_HOST = 'PG_VAL_001',
  INVALID_PORT = 'PG_VAL_002',
  INVALID_DATABASE = 'PG_VAL_003',
  INVALID_USERNAME = 'PG_VAL_004',
  INVALID_PASSWORD = 'PG_VAL_005',
  INVALID_SSL_CONFIG = 'PG_VAL_006',
  INVALID_SSH_CONFIG = 'PG_VAL_007',
  INVALID_POOL_SIZE = 'PG_VAL_008',
  INVALID_QUERY_TIMEOUT = 'PG_VAL_009',

  // Query Errors (PG_QUERY_001 - PG_QUERY_099)
  QUERY_TIMEOUT = 'PG_QUERY_001',
  QUERY_SYNTAX_ERROR = 'PG_QUERY_002',
  QUERY_PERMISSION_DENIED = 'PG_QUERY_003',
  QUERY_TABLE_NOT_FOUND = 'PG_QUERY_004',
  QUERY_COLUMN_NOT_FOUND = 'PG_QUERY_005',
  QUERY_DANGEROUS_KEYWORD = 'PG_QUERY_006',
  QUERY_TOO_LARGE = 'PG_QUERY_007',
  QUERY_RATE_LIMIT_EXCEEDED = 'PG_QUERY_008',
  QUERY_CANCELLED = 'PG_QUERY_009',
  QUERY_INVALID_TYPE = 'PG_QUERY_010',

  // Schema Discovery Errors (PG_SCHEMA_001 - PG_SCHEMA_099)
  SCHEMA_DISCOVERY_TIMEOUT = 'PG_SCHEMA_001',
  SCHEMA_PERMISSION_DENIED = 'PG_SCHEMA_002',
  SCHEMA_TOO_LARGE = 'PG_SCHEMA_003',
  SCHEMA_CACHE_ERROR = 'PG_SCHEMA_004',

  // Sync Errors (PG_SYNC_001 - PG_SYNC_099)
  SYNC_CONNECTION_LOST = 'PG_SYNC_001',
  SYNC_TABLE_NOT_FOUND = 'PG_SYNC_002',
  SYNC_COLUMN_NOT_FOUND = 'PG_SYNC_003',
  SYNC_SCHEMA_CHANGED = 'PG_SYNC_004',
  SYNC_TIMEOUT = 'PG_SYNC_005',
  SYNC_RETRY_EXHAUSTED = 'PG_SYNC_006',
  SYNC_INVALID_INCREMENTAL_COLUMN = 'PG_SYNC_007',
  SYNC_MEMORY_ERROR = 'PG_SYNC_008',
  SYNC_CANCELLED = 'PG_SYNC_009',

  // Encryption Errors (PG_ENC_001 - PG_ENC_099)
  ENCRYPTION_FAILED = 'PG_ENC_001',
  DECRYPTION_FAILED = 'PG_ENC_002',
  INVALID_ENCRYPTION_FORMAT = 'PG_ENC_003',

  // General Errors (PG_GEN_001 - PG_GEN_099)
  UNKNOWN_ERROR = 'PG_GEN_001',
  NOT_FOUND = 'PG_GEN_002',
  UNAUTHORIZED = 'PG_GEN_003',
  FORBIDDEN = 'PG_GEN_004',
  INTERNAL_ERROR = 'PG_GEN_005',
}

/**
 * User-friendly error messages mapped to error codes
 */
export const ERROR_MESSAGES: Record<string, string> = {
  // PostgreSQL native error codes
  '08001': 'Could not connect to database. Check host and port.',
  '08006': 'Connection lost. Database might be down.',
  '28000': 'Invalid username or password.',
  '28P01': 'Invalid password.',
  '3D000': 'Database does not exist.',
  '42P01': 'Table not found. It might have been deleted.',
  '42703': 'Column not found in table.',
  '42501': 'Permission denied. Contact your database administrator.',
  '42P07': 'Table already exists.',
  '42P16': 'Invalid table definition.',
  '23505': 'Unique constraint violation.',
  '23503': 'Foreign key constraint violation.',

  // Node.js error codes
  ECONNREFUSED: 'Connection refused. Is the database running?',
  ETIMEDOUT: 'Connection timed out. Check firewall settings.',
  ENOTFOUND: 'Host not found. Check the hostname.',
  ECONNRESET: 'Connection was reset by the server.',

  // Custom error codes
  [PostgresErrorCode.CONNECTION_TIMEOUT]:
    'Connection timed out. Please check your network and database settings.',
  [PostgresErrorCode.CONNECTION_REFUSED]:
    'Connection refused. Verify the database is running and accessible.',
  [PostgresErrorCode.INVALID_CREDENTIALS]:
    'Invalid username or password. Please check your credentials.',
  [PostgresErrorCode.DATABASE_NOT_FOUND]: 'Database not found. Please verify the database name.',
  [PostgresErrorCode.QUERY_TIMEOUT]:
    'Query execution timed out. The query may be too complex or the database is slow.',
  [PostgresErrorCode.QUERY_PERMISSION_DENIED]:
    'Permission denied. You do not have access to execute this query.',
  [PostgresErrorCode.QUERY_DANGEROUS_KEYWORD]:
    'Query contains dangerous keywords. Only SELECT queries are allowed.',
  [PostgresErrorCode.QUERY_RATE_LIMIT_EXCEEDED]:
    'Rate limit exceeded. Please wait before making more queries.',
  [PostgresErrorCode.SYNC_CONNECTION_LOST]:
    'Sync connection lost. The sync will be retried automatically.',
  [PostgresErrorCode.SYNC_SCHEMA_CHANGED]:
    'Table schema changed during sync. Please refresh the schema and try again.',
  [PostgresErrorCode.MAX_CONNECTIONS_EXCEEDED]:
    'Maximum number of connections exceeded for your organization.',
  [PostgresErrorCode.ENCRYPTION_FAILED]: 'Failed to encrypt credentials. Please try again.',
  [PostgresErrorCode.DECRYPTION_FAILED]:
    'Failed to decrypt credentials. The data may be corrupted.',
};

/**
 * Get user-friendly error message
 */
export function getErrorMessage(errorCode: string, defaultMessage?: string): string {
  return ERROR_MESSAGES[errorCode] || defaultMessage || 'An unexpected error occurred.';
}

/**
 * Map PostgreSQL error to our error code
 */
export function mapPostgresError(error: unknown): PostgresErrorCode {
  const code = (error as { code?: string })?.code;

  const message = (error as { message?: string })?.message?.toLowerCase() || '';

  // Connection errors
  if (code === '08001' || code === 'ECONNREFUSED') {
    return PostgresErrorCode.CONNECTION_REFUSED;
  }
  if (code === '08006' || code === 'ECONNRESET') {
    return PostgresErrorCode.CONNECTION_LOST;
  }

  if (code === 'ETIMEDOUT' || message.includes('timeout')) {
    return PostgresErrorCode.CONNECTION_TIMEOUT;
  }

  if (
    code === '28000' ||
    code === '28P01' ||
    message.includes('password') ||
    message.includes('authentication')
  ) {
    return PostgresErrorCode.INVALID_CREDENTIALS;
  }

  if (code === '3D000' || message.includes('database') || message.includes('does not exist')) {
    return PostgresErrorCode.DATABASE_NOT_FOUND;
  }

  if (code === 'ENOTFOUND' || message.includes('host')) {
    return PostgresErrorCode.HOST_NOT_FOUND;
  }

  // Query errors

  if (code === '42P01' || message.includes('table') || message.includes('does not exist')) {
    return PostgresErrorCode.QUERY_TABLE_NOT_FOUND;
  }

  if (code === '42703' || message.includes('column') || message.includes('does not exist')) {
    return PostgresErrorCode.QUERY_COLUMN_NOT_FOUND;
  }

  if (code === '42501' || message.includes('permission') || message.includes('denied')) {
    return PostgresErrorCode.QUERY_PERMISSION_DENIED;
  }

  if (code === '42601' || message.includes('syntax')) {
    return PostgresErrorCode.QUERY_SYNTAX_ERROR;
  }

  return PostgresErrorCode.UNKNOWN_ERROR;
}
