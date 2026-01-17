/**
 * Common Types for Data Pipelines
 * Generic types that work with all data source types
 */

/**
 * Supported data source types
 */
export enum DataSourceType {
  POSTGRES = 'postgres',
  MYSQL = 'mysql',
  MONGODB = 'mongodb',
  S3 = 's3',
  API = 'api',
  BIGQUERY = 'bigquery',
  SNOWFLAKE = 'snowflake',
  CSV = 'csv',
}

/**
 * Column mapping for data transformation
 */
export interface ColumnMapping {
  sourceColumn: string;
  destinationColumn: string;
  dataType: string;
  nullable: boolean;
  isPrimaryKey?: boolean;
  defaultValue?: string;
  maxLength?: number;
}

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
 * Collector interface - reads data from source
 */
export interface ICollector {
  /**
   * Collect data from source
   */
  collect(options: {
    sourceSchema: any;
    connection: any;
    limit?: number;
    offset?: number;
    cursor?: string;
    incrementalColumn?: string;
    lastSyncValue?: any;
  }): Promise<{
    rows: any[];
    totalRows?: number;
    nextCursor?: string;
    hasMore?: boolean;
  }>;

  /**
   * Discover schema from source
   */
  discoverSchema(options: { sourceSchema: any; connection: any }): Promise<{
    columns: ColumnInfo[];
    primaryKeys: string[];
    estimatedRowCount?: number;
  }>;
}

/**
 * Transformer interface - transforms data
 */
export interface ITransformer {
  /**
   * Transform a batch of rows
   */
  transform(
    rows: any[],
    mappings: ColumnMapping[],
    transformations?: Transformation[],
  ): Promise<any[]>;

  /**
   * Validate transformation configuration
   */
  validate(mappings: ColumnMapping[], transformations?: Transformation[]): ValidationResult;
}

/**
 * Emitter interface - writes data to destination
 */
export interface IEmitter {
  /**
   * Write data to destination
   */
  emit(options: {
    destinationSchema: any;
    connection: any;
    rows: any[];
    writeMode: 'append' | 'upsert' | 'replace';
    upsertKey?: string[];
  }): Promise<WriteResult>;

  /**
   * Validate destination schema
   */
  validateSchema(options: {
    destinationSchema: any;
    connection: any;
    columnMappings: ColumnMapping[];
  }): Promise<SchemaValidationResult>;

  /**
   * Create destination table if needed
   */
  createTable(options: {
    destinationSchema: any;
    connection: any;
    columnMappings: ColumnMapping[];
  }): Promise<{ created: boolean; tableName: string }>;

  /**
   * Check if table exists
   */
  tableExists(options: { destinationSchema: any; connection: any }): Promise<boolean>;
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
