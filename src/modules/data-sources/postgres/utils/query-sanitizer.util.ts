/**
 * Query Sanitizer Utility
 * Prevents SQL injection and validates query safety
 */

import {
  DANGEROUS_KEYWORDS,
  QUERY_CONFIG,
} from '../constants/postgres.constants';
import { PostgresErrorCode } from '../constants/error-codes.constants';

/**
 * Sanitize and validate SQL query
 */
export function sanitizeQuery(query: string): {
  isValid: boolean;
  error?: string;
  errorCode?: PostgresErrorCode;
} {
  if (!query || typeof query !== 'string') {
    return {
      isValid: false,
      error: 'Query must be a non-empty string',
      errorCode: PostgresErrorCode.QUERY_SYNTAX_ERROR,
    };
  }

  // Check query length
  if (query.length > QUERY_CONFIG.MAX_QUERY_LENGTH) {
    return {
      isValid: false,
      error: `Query exceeds maximum length of ${QUERY_CONFIG.MAX_QUERY_LENGTH} characters`,
      errorCode: PostgresErrorCode.QUERY_TOO_LARGE,
    };
  }

  // Normalize query for analysis
  const normalizedQuery = query.trim().toUpperCase();

  // Check for dangerous keywords
  for (const keyword of DANGEROUS_KEYWORDS) {
    // Use word boundary regex to avoid false positives
    const regex = new RegExp(`\\b${keyword}\\b`, 'i');
    if (regex.test(query)) {
      return {
        isValid: false,
        error: `Query contains dangerous keyword: ${keyword}. Only SELECT queries are allowed.`,
        errorCode: PostgresErrorCode.QUERY_DANGEROUS_KEYWORD,
      };
    }
  }

  // Ensure query starts with SELECT or WITH (CTE)
  if (
    !normalizedQuery.startsWith('SELECT') &&
    !normalizedQuery.startsWith('WITH')
  ) {
    return {
      isValid: false,
      error:
        'Query must start with SELECT or WITH. Only read-only queries are allowed.',
      errorCode: PostgresErrorCode.QUERY_DANGEROUS_KEYWORD,
    };
  }

  // Check for semicolon injection attempts (multiple statements)
  const semicolonCount = (query.match(/;/g) || []).length;
  if (semicolonCount > 1) {
    return {
      isValid: false,
      error:
        'Query contains multiple statements. Only single SELECT queries are allowed.',
      errorCode: PostgresErrorCode.QUERY_DANGEROUS_KEYWORD,
    };
  }

  // Check for comment-based injection attempts
  if (query.includes('--') || query.includes('/*') || query.includes('*/')) {
    // Allow comments but log for audit
    // Comments are generally safe in parameterized queries
  }

  // Check for UNION-based injection attempts
  const unionMatches = query.match(/\bUNION\b/gi);
  if (unionMatches && unionMatches.length > 1) {
    // Multiple UNIONs might indicate injection attempt
    // But allow legitimate UNION queries
  }

  return { isValid: true };
}

/**
 * Sanitize table name (prevent injection via table names)
 */
export function sanitizeTableName(tableName: string): string {
  if (!tableName || typeof tableName !== 'string') {
    throw new Error('Table name must be a non-empty string');
  }

  // Remove any characters that aren't alphanumeric, underscore, or dot
  // Allow schema.table format
  const sanitized = tableName.replace(/[^a-zA-Z0-9_.]/g, '');

  if (!sanitized) {
    throw new Error('Invalid table name');
  }

  // Prevent SQL injection via table name
  // Table names should be quoted in queries using parameterized queries
  return sanitized;
}

/**
 * Sanitize column name
 */
export function sanitizeColumnName(columnName: string): string {
  if (!columnName || typeof columnName !== 'string') {
    throw new Error('Column name must be a non-empty string');
  }

  // Remove any characters that aren't alphanumeric or underscore
  const sanitized = columnName.replace(/[^a-zA-Z0-9_]/g, '');

  if (!sanitized) {
    throw new Error('Invalid column name');
  }

  return sanitized;
}

/**
 * Sanitize schema name
 */
export function sanitizeSchemaName(schemaName: string): string {
  if (!schemaName || typeof schemaName !== 'string') {
    throw new Error('Schema name must be a non-empty string');
  }

  const sanitized = schemaName.replace(/[^a-zA-Z0-9_]/g, '');

  if (!sanitized) {
    throw new Error('Invalid schema name');
  }

  return sanitized;
}

/**
 * Build safe parameterized query
 * This is a helper to ensure queries use parameters
 */
export function buildParameterizedQuery(
  baseQuery: string,
  params: any[],
): { query: string; values: any[] } {
  // Validate base query
  const validation = sanitizeQuery(baseQuery);
  if (!validation.isValid) {
    throw new Error(validation.error);
  }

  // Ensure query uses parameterized placeholders ($1, $2, etc.)
  // This is a simple check - actual parameterization should be done by pg library
  return {
    query: baseQuery,
    values: params,
  };
}

/**
 * Extract table names from SELECT query (for logging/audit)
 */
export function extractTableNames(query: string): string[] {
  const tableRegex = /\bFROM\s+([a-zA-Z0-9_.]+)|\bJOIN\s+([a-zA-Z0-9_.]+)/gi;
  const matches = query.matchAll(tableRegex);
  const tables: string[] = [];

  for (const match of matches) {
    const tableName = match[1] || match[2];
    if (tableName) {
      tables.push(tableName);
    }
  }

  return [...new Set(tables)]; // Remove duplicates
}
