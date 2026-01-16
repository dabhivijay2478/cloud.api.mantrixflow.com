/**
 * Common Types for Data Pipelines
 * Generic types that work with all data source types
 */

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
  transformType: 'rename' | 'cast' | 'concat' | 'split' | 'custom';
  transformConfig: any;
  destinationColumn: string;
}

/**
 * Write operation result
 */
export interface WriteResult {
  rowsWritten: number;
  rowsSkipped: number;
  rowsFailed: number;
  errors?: PipelineError[];
}

/**
 * Pipeline execution error
 */
export interface PipelineError {
  row?: number;
  column?: string;
  message: string;
  error?: Error;
}

/**
 * Pipeline run result
 */
export interface PipelineRunResult {
  runId: string;
  status: 'success' | 'failed' | 'cancelled';
  rowsRead: number;
  rowsWritten: number;
  rowsSkipped: number;
  rowsFailed: number;
  durationSeconds: number;
  errors: PipelineError[];
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
}

/**
 * Column information
 */
export interface ColumnInfo {
  name: string;
  dataType: string;
  nullable: boolean;
  isPrimaryKey?: boolean;
  defaultValue?: any;
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
  }): Promise<{
    rows: any[];
    totalRows?: number;
    nextCursor?: string;
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
