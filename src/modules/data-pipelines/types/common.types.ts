/**
 * Common Types for Data Pipelines
 * Generic types that work with all data source types
 */

/**
 * Supported data source types
 * Only PostgreSQL, MySQL, and MongoDB are supported
 *
 * Guide: To add a new source:
 * 1. Add enum value here
 * 2. Create connector in Python service: etl-service/connectors/{source-name}.py
 * 3. Register in Python main.py CONNECTORS dict
 * 4. Update Python service endpoints if needed
 */
export enum DataSourceType {
  POSTGRES = 'postgres',
  MYSQL = 'mysql',
  MONGODB = 'mongodb',
}

/**
 * Column mapping for data transformation
 * Enhanced to support NoSQL ↔ SQL bidirectional mapping
 */
export interface ColumnMapping {
  // Basic mapping (simple columns)
  sourceColumn: string;
  destinationColumn: string;
  dataType: string;
  nullable: boolean;
  isPrimaryKey?: boolean;
  defaultValue?: string;
  maxLength?: number;

  // Enhanced mapping for NoSQL ↔ SQL
  /** Source entity (table/collection name) */
  sourceEntity?: string;
  /** Source path using dot notation: 'address.city' or 'orders[].amount' */
  sourcePath?: string;
  /** Destination entity (table/collection name) */
  destEntity?: string;
  /** Destination path using dot notation for nesting */
  destPath?: string;
  /** Built-in transformation to apply */
  transformation?: ColumnTransformationType;
  /** Custom transformation expression */
  transformExpression?: string;
  /** Flag for unwinding arrays (MongoDB → PostgreSQL) */
  isArray?: boolean;
  /** Foreign key column for linking flattened data */
  foreignKey?: string;
  /** Whether this mapping is required */
  required?: boolean;
}

/**
 * Built-in transformation types for NoSQL ↔ SQL
 */
export type ColumnTransformationType =
  | 'none'
  | 'flattenObject' // Flatten nested object: { a: { b: 1 } } → { a_b: 1 }
  | 'flattenArray' // Flatten array into separate rows
  | 'embedObject' // Embed into nested object (SQL → NoSQL)
  | 'embedArray' // Embed as array element
  | 'jsonStringify' // Convert to JSON string
  | 'jsonParse' // Parse JSON string to object
  | 'toISODate' // Convert to ISO date string
  | 'toTimestamp' // Convert to timestamp
  | 'objectIdToUuid' // MongoDB ObjectId to UUID string
  | 'uuidToObjectId' // UUID to MongoDB ObjectId
  | 'toNumber' // Convert to number
  | 'toString' // Convert to string
  | 'toBoolean' // Convert to boolean
  | 'trim' // Trim whitespace
  | 'lowercase' // Lowercase string
  | 'uppercase'; // Uppercase string

/**
 * Data transformation configuration
 */
export interface Transformation {
  sourceColumn: string;
  transformType: 'rename' | 'cast' | 'concat' | 'split' | 'custom' | 'filter' | 'mask' | 'hash';
  transformConfig: TransformConfig;
  destinationColumn: string;
}

/**
 * Transformation configuration options
 */
export interface TransformConfig {
  // Cast transformation
  targetType?: string;

  // Concat transformation
  fields?: string[];
  separator?: string;

  // Split transformation
  index?: number;

  // Filter transformation
  operator?: 'eq' | 'ne' | 'gt' | 'lt' | 'gte' | 'lte' | 'contains' | 'startsWith' | 'endsWith';
  value?: any;

  // Mask transformation
  maskChar?: string;
  visibleChars?: number;

  // Hash transformation
  algorithm?: 'md5' | 'sha256' | 'sha512';

  // Custom transformation
  transform?: (value: any) => any;
  expression?: string;
}

/**
 * Write operation result
 */
export interface WriteResult {
  rowsWritten: number;
  rowsSkipped: number;
  rowsFailed: number;
  errors?: PipelineError[];
  metadata?: Record<string, any>;
}

/**
 * Pipeline execution error
 */
export interface PipelineError {
  row?: number;
  column?: string;
  message: string;
  error?: Error;
  code?: string;
  retryable?: boolean;
}

/**
 * Custom pipeline exception
 */
export class PipelineException extends Error {
  constructor(
    message: string,
    public readonly code: string,
    public readonly retryable: boolean = false,
    public readonly details?: Record<string, any>,
  ) {
    super(message);
    this.name = 'PipelineException';
  }
}

/**
 * Pipeline run result
 */
export interface PipelineRunResult {
  runId: string;
  status: 'success' | 'failed' | 'cancelled' | 'partial';
  rowsRead: number;
  rowsWritten: number;
  rowsSkipped: number;
  rowsFailed: number;
  durationSeconds: number;
  errors: PipelineError[];
  bytesProcessed?: number;
}

/**
 * Validation result
 */
export interface ValidationResult {
  valid: boolean;
  errors: string[];
  warnings?: string[];
}

/**
 * Dry run result
 */
export interface DryRunResult {
  wouldWrite: number;
  sourceRowCount?: number;
  sampleRows: any[];
  errors: PipelineError[];
  transformedSample?: any[];
  appliedMappings?: Array<{ sourcePath: string; destPath: string }>;
}

/**
 * Column information
 */
export interface ColumnInfo {
  name: string;
  dataType: string;
  nullable: boolean;
  isPrimaryKey?: boolean;
  isForeignKey?: boolean;
  defaultValue?: any;
  maxLength?: number;
  precision?: number;
  scale?: number;
}

/**
 * Schema validation result
 */
export interface SchemaValidationResult {
  valid: boolean;
  errors: string[];
  warnings?: string[];
  missingColumns?: string[];
  typeMismatches?: TypeMismatch[];
}

/**
 * Type mismatch information
 */
export interface TypeMismatch {
  column: string;
  expectedType: string;
  actualType: string;
  severity?: 'error' | 'warning';
  canCast?: boolean;
}

/**
 * Table statistics
 */
export interface TableStats {
  rowCount: number;
  sizeBytes: number;
  indexCount: number;
  lastAnalyzed?: Date;
  lastUpdated?: Date;
}

/**
 * Type inference result
 */
export interface TypeInferenceResult {
  column: string;
  inferredType: string;
  confidence: number;
  sampleValues?: any[];
}

/**
 * Validation error
 */
export interface ValidationError {
  field: string;
  message: string;
  code?: string;
}

/**
 * Batch processing options
 */
export interface BatchOptions {
  batchSize: number;
  parallelWorkers?: number;
  retryAttempts?: number;
  retryDelayMs?: number;
}

/**
 * Progress callback for tracking
 */
export type ProgressCallback = (progress: ProgressInfo) => void;

/**
 * Progress information
 */
export interface ProgressInfo {
  phase: 'collecting' | 'transforming' | 'emitting' | 'complete';
  currentBatch: number;
  totalBatches?: number;
  rowsProcessed: number;
  rowsTotal?: number;
  percentage?: number;
  message?: string;
}

/**
 * Schema information for orchestration
 * Used internally by PipelineService for metadata
 */
export interface SchemaInfo {
  columns: ColumnInfo[];
  primaryKeys: string[];
  estimatedRowCount?: number;
  // For NoSQL databases
  sampleDocuments?: any[];
  // For nested structures
  nestedFields?: Map<string, ColumnInfo[]>;
  // Schema type indicators for transformation
  /** Whether this is a relational (SQL) schema */
  isRelational?: boolean;
  /** The source type (postgres, mongodb, etc.) */
  sourceType?: string;
  /** Entity name (table/collection) */
  entityName?: string;
}

/**
 * Rate limiter configuration for API sources
 */
export interface RateLimiterConfig {
  requestsPerSecond: number;
  burstSize?: number;
  maxRetries?: number;
  retryDelayMs?: number;
}

/**
 * Cursor-based pagination state
 */
export interface CursorState {
  cursor: string;
  offset?: number;
  pageToken?: string;
  lastId?: string;
  lastTimestamp?: Date;
}

/**
 * Connection pool configuration
 */
export interface PoolConfig {
  min: number;
  max: number;
  idleTimeoutMs?: number;
  acquireTimeoutMs?: number;
}

// ---------------------------------------------------------------------------
// PostgreSQL type compatibility utilities
// ---------------------------------------------------------------------------

/**
 * Canonical type families for PostgreSQL column types.
 * Higher rank = "wider" type that can safely hold values from lower-rank types.
 */
const PG_TYPE_RANK: Record<string, number> = {
  boolean: 100,
  smallint: 200,
  integer: 210,
  bigint: 220,
  real: 300,
  float: 310,
  'double precision': 320,
  numeric: 330,
  decimal: 330,
  date: 400,
  time: 410,
  'time without time zone': 410,
  'time with time zone': 415,
  timestamp: 420,
  'timestamp without time zone': 420,
  'timestamp with time zone': 425,
  interval: 430,
  uuid: 500,
  json: 600,
  jsonb: 610,
  bytea: 700,
  character: 800,
  'character varying': 810,
  varchar: 810,
  text: 900,
};

function normalizeTypeName(raw: string): string {
  return raw
    .toLowerCase()
    .replace(/\(\d+[\d,\s]*\)/g, '')
    .replace(/\[\]/g, '')
    .trim();
}

function rankOf(pgType: string): number | undefined {
  const norm = normalizeTypeName(pgType);
  if (PG_TYPE_RANK[norm] !== undefined) return PG_TYPE_RANK[norm];
  for (const [key, rank] of Object.entries(PG_TYPE_RANK)) {
    if (norm.startsWith(key)) return rank;
  }
  return undefined;
}

/**
 * Same type family groupings — types within a group can be widened to the
 * higher-ranked member without semantic data loss.
 */
const WIDENING_FAMILIES: string[][] = [
  ['boolean'],
  ['smallint', 'integer', 'bigint'],
  ['real', 'float', 'double precision'],
  ['numeric', 'decimal'],
  ['date', 'timestamp', 'timestamp without time zone', 'timestamp with time zone'],
  ['character', 'character varying', 'varchar', 'text'],
  ['json', 'jsonb'],
];

function familyOf(pgType: string): string[] | undefined {
  const norm = normalizeTypeName(pgType);
  return WIDENING_FAMILIES.find((f) => f.some((t) => norm.startsWith(t)));
}

export type TypeCompatibility =
  | 'identical'
  | 'safe_widening'
  | 'unsafe_narrowing'
  | 'cross_family_safe'
  | 'cross_family_unsafe'
  | 'unknown';

/**
 * Check whether writing `sourceType` data into an existing `destType` column
 * is safe, produces a warning, or should be an error.
 *
 * Rules:
 *  - identical types → "identical"
 *  - any type → text/varchar → always safe ("cross_family_safe")
 *  - within the same family, higher-rank source into lower-rank dest → "unsafe_narrowing"
 *  - within the same family, lower-rank source into higher-rank dest → "safe_widening"
 *  - integer family → float/numeric family → "safe_widening"
 *  - text → anything non-text → "cross_family_unsafe"
 *  - everything else → "unknown" (treated as warning)
 */
export function checkTypeCompatibility(
  sourceType: string,
  destType: string,
): TypeCompatibility {
  const srcNorm = normalizeTypeName(sourceType);
  const dstNorm = normalizeTypeName(destType);

  if (srcNorm === dstNorm) return 'identical';

  const srcRank = rankOf(srcNorm);
  const dstRank = rankOf(dstNorm);

  // Anything → text/varchar is always safe
  if (dstNorm === 'text' || dstNorm.startsWith('character varying') || dstNorm === 'varchar') {
    return 'cross_family_safe';
  }

  // text → non-text is unsafe (potential parse failure)
  if (srcNorm === 'text' || srcNorm.startsWith('character varying') || srcNorm === 'varchar') {
    return 'cross_family_unsafe';
  }

  const srcFamily = familyOf(srcNorm);
  const dstFamily = familyOf(dstNorm);

  if (srcFamily && dstFamily && srcFamily === dstFamily && srcRank != null && dstRank != null) {
    return srcRank <= dstRank ? 'safe_widening' : 'unsafe_narrowing';
  }

  // Integer → float/numeric is safe
  const intFamily = WIDENING_FAMILIES.find((f) => f.includes('integer'));
  const floatFamily = WIDENING_FAMILIES.find((f) => f.includes('numeric'));
  if (
    intFamily &&
    floatFamily &&
    intFamily.some((t) => srcNorm.startsWith(t)) &&
    floatFamily.some((t) => dstNorm.startsWith(t))
  ) {
    return 'safe_widening';
  }

  return 'unknown';
}

/**
 * Compare source columns against an existing destination table's columns
 * and return structured validation results.
 */
export function validateColumnTypeCompatibility(
  sourceColumns: Array<{ name: string; dataType: string }>,
  destColumns: Array<{ name: string; dataType: string }>,
): {
  missingColumns: string[];
  typeMismatches: TypeMismatch[];
  errors: string[];
  warnings: string[];
} {
  const missingColumns: string[] = [];
  const typeMismatches: TypeMismatch[] = [];
  const errors: string[] = [];
  const warnings: string[] = [];

  const destMap = new Map(destColumns.map((c) => [c.name.toLowerCase(), c]));

  for (const src of sourceColumns) {
    const dest = destMap.get(src.name.toLowerCase());
    if (!dest) {
      missingColumns.push(src.name);
      continue;
    }

    const compat = checkTypeCompatibility(src.dataType, dest.dataType);
    switch (compat) {
      case 'identical':
      case 'safe_widening':
      case 'cross_family_safe':
        break;
      case 'unsafe_narrowing':
        typeMismatches.push({
          column: src.name,
          expectedType: dest.dataType,
          actualType: src.dataType,
          severity: 'warning',
          canCast: true,
        });
        warnings.push(
          `Column "${src.name}": source type ${src.dataType} is wider than destination type ${dest.dataType} — potential overflow`,
        );
        break;
      case 'cross_family_unsafe':
        typeMismatches.push({
          column: src.name,
          expectedType: dest.dataType,
          actualType: src.dataType,
          severity: 'error',
          canCast: false,
        });
        errors.push(
          `Column "${src.name}": source type ${src.dataType} cannot be safely cast to destination type ${dest.dataType}`,
        );
        break;
      case 'unknown':
        typeMismatches.push({
          column: src.name,
          expectedType: dest.dataType,
          actualType: src.dataType,
          severity: 'warning',
          canCast: true,
        });
        warnings.push(
          `Column "${src.name}": source type ${src.dataType} differs from destination type ${dest.dataType} — verify compatibility`,
        );
        break;
    }
  }

  if (missingColumns.length > 0) {
    warnings.push(
      `Destination table is missing ${missingColumns.length} column(s) that will be auto-created: ${missingColumns.join(', ')}`,
    );
  }

  return { missingColumns, typeMismatches, errors, warnings };
}

// ---------------------------------------------------------------------------
// Singer JSON Schema → PostgreSQL type mapping
// ---------------------------------------------------------------------------

/**
 * Map a Singer JSON Schema type to the PostgreSQL type that target-postgres
 * would create for a new column. This lets us predict exactly what type
 * target-postgres expects and detect conflicts with existing dest columns.
 */
export function singerTypeToPgType(singerType: string): string {
  const norm = (singerType || '').toLowerCase().trim();
  switch (norm) {
    case 'integer':
      return 'bigint';
    case 'number':
      return 'double precision';
    case 'boolean':
      return 'boolean';
    case 'object':
      return 'jsonb';
    case 'array':
      return 'jsonb';
    case 'string':
    default:
      return 'text';
  }
}

/**
 * Column descriptor returned by the ETL /introspect-table endpoint.
 */
export interface IntrospectedColumn {
  name: string;
  data_type: string;
  udt_name: string;
  is_nullable: boolean;
  has_default: boolean;
  is_identity: boolean;
  identity_generation: string | null;
  character_maximum_length: number | null;
  numeric_precision: number | null;
  numeric_scale: number | null;
}

/**
 * Validate Singer source types against an existing destination table's real
 * PostgreSQL column types (from /introspect-table).
 *
 * With target-postgres `allow_column_alter = False`, **any** type mismatch
 * causes a fatal NotImplementedError at sync time. So every mismatch that
 * target-postgres would detect is treated as a blocking error here.
 */
export function validateSingerVsDestination(
  singerColumns: Array<{ name: string; type: string }>,
  destColumns: IntrospectedColumn[],
): {
  errors: string[];
  warnings: string[];
  mismatches: TypeMismatch[];
} {
  const errors: string[] = [];
  const warnings: string[] = [];
  const mismatches: TypeMismatch[] = [];

  const destMap = new Map(
    destColumns.map((c) => [c.name.toLowerCase(), c]),
  );

  for (const src of singerColumns) {
    const dest = destMap.get(src.name.toLowerCase());
    if (!dest) {
      continue;
    }

    const expectedPg = singerTypeToPgType(src.type);
    const actualPg = normalizeTypeName(dest.data_type);

    if (expectedPg === actualPg) continue;

    // Check same family — some mismatches are harmless (e.g. integer vs bigint
    // within the integer family where the dest is wider)
    const compat = checkTypeCompatibility(expectedPg, actualPg);
    if (compat === 'identical' || compat === 'safe_widening') continue;

    const identityNote = dest.is_identity
      ? ' (IDENTITY column — cannot be altered)'
      : '';

    mismatches.push({
      column: src.name,
      expectedType: actualPg,
      actualType: expectedPg,
      severity: 'error',
      canCast: false,
    });

    errors.push(
      `Column "${src.name}": Singer sends ${expectedPg.toUpperCase()} but destination has ${actualPg.toUpperCase()}${identityNote}. ` +
        'target-postgres cannot alter existing columns. Drop the destination table or set destinationTableExists=false ' +
        'to let target-postgres create it with the correct types.',
    );
  }

  return { errors, warnings, mismatches };
}
