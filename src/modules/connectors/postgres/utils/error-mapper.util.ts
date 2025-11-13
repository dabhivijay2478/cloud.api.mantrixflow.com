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
  details?: any;
  suggestion?: string;
}

/**
 * Map error to standardized format
 */
export function mapErrorToStandardized(error: any): StandardizedError {
  // If already standardized
  if (
    error.code &&
    error.message &&
    Object.values(PostgresErrorCode).includes(error.code as PostgresErrorCode)
  ) {
    return {
      code: error.code,
      message: error.message,
      details: error.details,
      suggestion: error.suggestion,
    };
  }

  // Map PostgreSQL errors
  const pgErrorCode = mapPostgresError(error);
  const message = getErrorMessage(pgErrorCode, error.message);

  // Extract additional details
  const details: any = {};
  if (error.hint) details.hint = error.hint;
  if (error.position) details.position = error.position;
  if (error.where) details.where = error.where;
  if (error.schema) details.schema = error.schema;
  if (error.table) details.table = error.table;
  if (error.column) details.column = error.column;

  // Generate suggestion based on error code
  const suggestion = generateSuggestion(pgErrorCode, error);

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
  error: any,
): string | undefined {
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
      if (error.position) {
        return `Check the SQL syntax around position ${error.position}. Review PostgreSQL documentation for correct syntax.`;
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
  error: any,
  statusCode: number = 500,
): {
  statusCode: number;
  error: StandardizedError;
} {
  const standardized = mapErrorToStandardized(error);

  // Map error codes to HTTP status codes
  let httpStatus = statusCode;
  if (standardized.code === PostgresErrorCode.NOT_FOUND) {
    httpStatus = 404;
  } else if (standardized.code === PostgresErrorCode.UNAUTHORIZED) {
    httpStatus = 401;
  } else if (standardized.code === PostgresErrorCode.FORBIDDEN) {
    httpStatus = 403;
  } else if (
    [
      PostgresErrorCode.CONNECTION_TIMEOUT,
      PostgresErrorCode.CONNECTION_REFUSED,
      PostgresErrorCode.INVALID_CREDENTIALS,
      PostgresErrorCode.DATABASE_NOT_FOUND,
    ].includes(standardized.code as PostgresErrorCode)
  ) {
    httpStatus = 400;
  } else if (
    [
      PostgresErrorCode.QUERY_PERMISSION_DENIED,
      PostgresErrorCode.QUERY_DANGEROUS_KEYWORD,
      PostgresErrorCode.QUERY_RATE_LIMIT_EXCEEDED,
    ].includes(standardized.code as PostgresErrorCode)
  ) {
    httpStatus = 403;
  }

  return {
    statusCode: httpStatus,
    error: standardized,
  };
}
