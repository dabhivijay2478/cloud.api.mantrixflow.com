/**
 * PostgreSQL Type Mapper Utility
 * Maps PostgreSQL data types to TypeScript types
 */

import { PG_TYPE_MAPPINGS } from '../constants/postgres.constants';

/**
 * Map PostgreSQL type to TypeScript type
 */
export function mapPostgresTypeToTypeScript(
  pgType: string,
  isArray: boolean = false,
  isJsonb: boolean = false,
): string {
  // Handle arrays
  if (isArray) {
    const baseType = mapPostgresTypeToTypeScript(pgType, false, false);
    return `${baseType}[]`;
  }

  // Handle JSONB
  if (isJsonb || pgType.toLowerCase() === 'jsonb') {
    return 'object';
  }

  // Normalize type name
  const normalizedType = pgType.toLowerCase().trim();

  // Check direct mapping
  if (PG_TYPE_MAPPINGS[normalizedType]) {
    return PG_TYPE_MAPPINGS[normalizedType];
  }

  // Handle array types (e.g., integer[], text[])
  if (normalizedType.endsWith('[]')) {
    const baseType = normalizedType.slice(0, -2);
    const tsType = PG_TYPE_MAPPINGS[baseType] || 'any';
    return `${tsType}[]`;
  }

  // Handle custom types (enums, domains, etc.)
  // For now, default to string or any
  if (normalizedType.includes('enum') || normalizedType.includes('domain')) {
    return 'string';
  }

  // Handle PostGIS geometry types
  if (
    normalizedType.includes('geometry') ||
    normalizedType.includes('geography') ||
    ['point', 'line', 'lseg', 'box', 'path', 'polygon', 'circle'].includes(
      normalizedType,
    )
  ) {
    return 'object';
  }

  // Handle pg_vector
  if (normalizedType === 'vector') {
    return 'number[]';
  }

  // Default fallback
  return 'any';
}

/**
 * Detect if a type is an array
 */
export function isArrayType(pgType: string): boolean {
  return (
    pgType.toLowerCase().endsWith('[]') ||
    pgType.toLowerCase().includes('array')
  );
}

/**
 * Detect if a type is JSONB
 */
export function isJsonbType(pgType: string): boolean {
  return pgType.toLowerCase() === 'jsonb' || pgType.toLowerCase() === 'json';
}

/**
 * Detect if a type is an enum
 */
export function isEnumType(pgType: string): boolean {
  return (
    pgType.toLowerCase().includes('enum') ||
    pgType.toLowerCase().startsWith('user_defined')
  );
}

/**
 * Extract enum values from PostgreSQL (requires query)
 * This is a placeholder - actual implementation would query pg_enum
 */
export async function getEnumValues(
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  connection: any,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  enumTypeName: string,
): Promise<string[]> {
  // TODO: Implement enum value extraction
  // Query: SELECT enumlabel FROM pg_enum WHERE enumtypid = (SELECT oid FROM pg_type WHERE typname = $1)
  return [];
}

/**
 * Format PostgreSQL type with modifiers
 */
export function formatPostgresType(
  dataType: string,
  maxLength?: number,
  numericPrecision?: number,
  numericScale?: number,
): string {
  let formatted = dataType;

  if (maxLength) {
    formatted += `(${maxLength})`;
  } else if (numericPrecision !== undefined) {
    if (numericScale !== undefined) {
      formatted += `(${numericPrecision},${numericScale})`;
    } else {
      formatted += `(${numericPrecision})`;
    }
  }

  return formatted;
}
