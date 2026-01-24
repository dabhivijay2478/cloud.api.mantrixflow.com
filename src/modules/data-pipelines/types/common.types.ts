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
  | 'flattenObject'      // Flatten nested object: { a: { b: 1 } } → { a_b: 1 }
  | 'flattenArray'       // Flatten array into separate rows
  | 'embedObject'        // Embed into nested object (SQL → NoSQL)
  | 'embedArray'         // Embed as array element
  | 'jsonStringify'      // Convert to JSON string
  | 'jsonParse'          // Parse JSON string to object
  | 'toISODate'          // Convert to ISO date string
  | 'toTimestamp'        // Convert to timestamp
  | 'objectIdToUuid'     // MongoDB ObjectId to UUID string
  | 'uuidToObjectId'     // UUID to MongoDB ObjectId
  | 'toNumber'           // Convert to number
  | 'toString'           // Convert to string
  | 'toBoolean'          // Convert to boolean
  | 'trim'               // Trim whitespace
  | 'lowercase'          // Lowercase string
  | 'uppercase';         // Uppercase string

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
