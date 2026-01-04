/**
 * Error Mapper Utility
 * Maps various error types to standardized error responses
 */

import {
  PostgresErrorCode,
  mapPostgresError,
  getErrorMessage,
} from '../constants/error-codes.constants';

/**
 * Standardized error response
 */
export interface StandardizedError {
  code: PostgresErrorCode | string;
  message: string;
  details?: Record<string, unknown>;
  suggestion?: string;
}

/**
 * Map error to standardized format
 */
export function mapErrorToStandardized(error: unknown): StandardizedError {
  const errorObj = error as Record<string, unknown>;
  // If already standardized

  if (
    errorObj.code &&
    errorObj.message &&
    Object.values(PostgresErrorCode).includes(
      errorObj.code as PostgresErrorCode,
    )
  ) {
    return {
      code: errorObj.code as PostgresErrorCode | string,

      message: errorObj.message as string,

      details: errorObj.details as Record<string, unknown> | undefined,

      suggestion: errorObj.suggestion as string | undefined,
    };
  }

  // Extract actual PostgreSQL error from Drizzle errors
  // Drizzle wraps PostgreSQL errors in a 'cause' property
  let actualError = error;
  const drizzleError = error as any;
  if (drizzleError?.cause) {
    actualError = drizzleError.cause;
  }

  // Check for direct PostgreSQL error codes first (before mapping)
  const pgError = actualError as { code?: string; detail?: string; constraint?: string };
  if (pgError?.code) {
    // This is a direct PostgreSQL error - use the code directly
    const pgErrorCode = pgError.code;
    
    // Map known PostgreSQL error codes
    if (pgErrorCode === '23503') {
      // Foreign key constraint violation
      return {
        code: 'PG_CONSTRAINT_001',
        message: `Foreign key constraint violation: ${pgError.detail || 'Referenced record does not exist'}`,
        details: {
          postgresErrorCode: pgErrorCode,
          constraint: pgError.constraint,
          detail: pgError.detail,
        },
        suggestion: 'The referenced record may not exist. Please check that all related records exist in the database.',
      };
    }
    
    if (pgErrorCode === '23502') {
      // NOT NULL constraint violation
      return {
        code: 'PG_CONSTRAINT_002',
        message: `Required field is missing: ${pgError.detail || 'NOT NULL constraint violation'}`,
        details: {
          postgresErrorCode: pgErrorCode,
          constraint: pgError.constraint,
          detail: pgError.detail,
        },
        suggestion: 'Please check that all required fields are provided.',
      };
    }
    
    if (pgErrorCode === '23505') {
      // Unique constraint violation
      return {
        code: 'PG_CONSTRAINT_003',
        message: `Unique constraint violation: ${pgError.detail || 'Duplicate value'}`,
        details: {
          postgresErrorCode: pgErrorCode,
          constraint: pgError.constraint,
          detail: pgError.detail,
        },
        suggestion: 'A record with this value may already exist.',
      };
    }
  }

  // Map PostgreSQL errors using the error mapper
  const pgErrorCode = mapPostgresError(actualError);

  const message = getErrorMessage(
    pgErrorCode,
    (actualError as { message?: string })?.message || errorObj.message as string | undefined,
  );

  // Extract additional details from the actual error
  const actualErrorObj = actualError as Record<string, unknown>;
  const details: Record<string, unknown> = {};

  if (actualErrorObj.hint) details.hint = actualErrorObj.hint;
  if (actualErrorObj.position) details.position = actualErrorObj.position;
  if (actualErrorObj.where) details.where = actualErrorObj.where;
  if (actualErrorObj.schema) details.schema = actualErrorObj.schema;
  if (actualErrorObj.table) details.table = actualErrorObj.table;
  if (actualErrorObj.column) details.column = actualErrorObj.column;
  if (actualErrorObj.detail) details.detail = actualErrorObj.detail;
  if (actualErrorObj.constraint) details.constraint = actualErrorObj.constraint;
  if (actualErrorObj.code) details.postgresErrorCode = actualErrorObj.code;

  // Generate suggestion based on error code
  const suggestion = generateSuggestion(pgErrorCode, actualError);

  return {
    code: pgErrorCode,
    message,
    details: Object.keys(details).length > 0 ? details : undefined,
    suggestion,
  };
}

/**
 * Generate helpful suggestion based on error code
 */
function generateSuggestion(
  code: PostgresErrorCode,
  error: unknown,
): string | undefined {
  const errorObj = error as Record<string, unknown>;
  switch (code) {
    case PostgresErrorCode.CONNECTION_TIMEOUT:
      return 'Check your network connection and firewall settings. Ensure the database is accessible from this server.';
    case PostgresErrorCode.CONNECTION_REFUSED:
      return 'Verify that PostgreSQL is running and listening on the specified port. Check if the host and port are correct.';
    case PostgresErrorCode.INVALID_CREDENTIALS:
      return 'Double-check your username and password. Ensure the user has the necessary permissions.';
    case PostgresErrorCode.DATABASE_NOT_FOUND:
      return 'Verify the database name is correct. The database must exist before connecting.';
    case PostgresErrorCode.HOST_NOT_FOUND:
      return 'Check the hostname or IP address. Ensure DNS resolution is working correctly.';
    case PostgresErrorCode.QUERY_TIMEOUT:
      return 'The query may be too complex or the database is under heavy load. Try simplifying the query or increasing the timeout.';
    case PostgresErrorCode.QUERY_TABLE_NOT_FOUND:
      return 'The table may have been renamed or deleted. Refresh the schema to see current tables.';
    case PostgresErrorCode.QUERY_COLUMN_NOT_FOUND:
      return 'The column may have been renamed or removed. Refresh the schema to see current columns.';
    case PostgresErrorCode.QUERY_PERMISSION_DENIED:
      return 'Contact your database administrator to grant SELECT permissions on the required tables.';
    case PostgresErrorCode.QUERY_SYNTAX_ERROR:
      if (errorObj.position) {
        return `Check the SQL syntax around position ${errorObj.position as number}. Review PostgreSQL documentation for correct syntax.`;
      }
      return 'Review the SQL syntax. Ensure all keywords, table names, and column names are correct.';
    case PostgresErrorCode.SYNC_SCHEMA_CHANGED:
      return 'The table schema has changed. Please refresh the schema discovery and update your sync configuration.';
    case PostgresErrorCode.SYNC_INVALID_INCREMENTAL_COLUMN:
      return 'The incremental column must be a timestamp or auto-incrementing integer. Verify the column exists and has the correct type.';
    case PostgresErrorCode.MAX_CONNECTIONS_EXCEEDED:
      return 'You have reached the maximum number of connections for your organization. Delete unused connections or contact support.';
    default:
      return undefined;
  }
}

/**
 * Create user-friendly error response for API
 */
export function createErrorResponse(
  error: unknown,
  statusCode: number = 500,
): {
  statusCode: number;
  error: StandardizedError;
} {
  const standardized = mapErrorToStandardized(error);

  // Map error codes to HTTP status codes
  let httpStatus = statusCode;
  const code = standardized.code as PostgresErrorCode;
  if (code === PostgresErrorCode.NOT_FOUND) {
    httpStatus = 404;
  } else if (code === PostgresErrorCode.UNAUTHORIZED) {
    httpStatus = 401;
  } else if (code === PostgresErrorCode.FORBIDDEN) {
    httpStatus = 403;
  } else if (
    code === PostgresErrorCode.CONNECTION_TIMEOUT ||
    code === PostgresErrorCode.CONNECTION_REFUSED ||
    code === PostgresErrorCode.INVALID_CREDENTIALS ||
    code === PostgresErrorCode.DATABASE_NOT_FOUND
  ) {
    httpStatus = 400;
  } else if (
    code === PostgresErrorCode.QUERY_PERMISSION_DENIED ||
    code === PostgresErrorCode.QUERY_DANGEROUS_KEYWORD ||
    code === PostgresErrorCode.QUERY_RATE_LIMIT_EXCEEDED
  ) {
    httpStatus = 403;
  }

  return {
    statusCode: httpStatus,
    error: standardized,
  };
}
