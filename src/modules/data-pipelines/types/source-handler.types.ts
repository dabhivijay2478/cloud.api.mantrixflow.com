/**
 * Source Handler Types
 * Interfaces for the strategy pattern implementation of data source handlers
 */

import { ColumnInfo, DataSourceType } from './common.types';

/**
 * Source schema configuration passed to handlers
 * This is a simplified interface that handlers use
 */
export interface SourceSchemaConfig {
  // Database/schema info
  schema?: string;
  table?: string;
  tableName?: string;
  database?: string;
  
  // MongoDB specific
  collection?: string;
  
  // S3 specific
  prefix?: string;
  pathPrefix?: string;
  filePattern?: string;
  
  // API specific
  method?: string;
  path?: string;
  endpoint?: string;
  queryParams?: Record<string, any>;
  body?: any;
  dataPath?: string;
  pagination?: {
    type?: 'offset' | 'cursor' | 'page';
    offsetParam?: string;
    limitParam?: string;
    cursorParam?: string;
    pageParam?: string;
    totalPath?: string;
    nextCursorPath?: string;
    hasMorePath?: string;
  };
  
  // Generic
  [key: string]: any;
}

/**
 * Pipeline source schema with config - handlers receive this
 */
export interface PipelineSourceSchemaWithConfig {
  id: string;
  organizationId: string;
  sourceType: string;
  dataSourceId?: string | null;
  sourceSchema?: string | null;
  sourceTable?: string | null;
  sourceQuery?: string | null;
  sourceConfig?: SourceSchemaConfig | null;
  name?: string | null;
  
  // Convenience getter for handlers - merged config
  config: SourceSchemaConfig;
}

/**
 * Schema information returned by schema discovery
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
 * Parameters for data collection
 */
export interface CollectParams {
  limit: number;
  offset: number;
  cursor?: string;
  incrementalColumn?: string;
  lastSyncValue?: any;
  batchSize?: number;
  filters?: Record<string, any>;
}

/**
 * Result of data collection
 */
export interface CollectResult {
  rows: any[];
  totalRows?: number;
  nextCursor?: string;
  hasMore?: boolean;
  metadata?: Record<string, any>;
}

/**
 * Connection test result
 */
export interface ConnectionTestResult {
  success: boolean;
  message: string;
  details?: {
    version?: string;
    serverInfo?: any;
    latencyMs?: number;
  };
}

/**
 * Source Handler Interface
 * All data source handlers must implement this interface
 */
export interface ISourceHandler {
  /**
   * The data source type this handler supports
   */
  readonly type: DataSourceType;

  /**
   * Test connection to the data source
   */
  testConnection(connectionConfig: any): Promise<ConnectionTestResult>;

  /**
   * Discover schema from the data source
   */
  discoverSchema(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
  ): Promise<SchemaInfo>;

  /**
   * Collect data from the data source
   * Uses async generator for streaming large datasets
   */
  collect(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    params: CollectParams,
  ): Promise<CollectResult>;

  /**
   * Collect incremental data (new/changed records since last checkpoint)
   * This method MUST implement strict incremental filtering:
   * - PostgreSQL/MySQL: WHERE watermarkField > lastValue
   * - MongoDB: { watermarkField: { $gt: lastValue } }
   * 
   * @param checkpoint - Checkpoint containing lastSyncValue and watermarkField
   * @returns Only new/changed records since checkpoint
   */
  collectIncremental(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    checkpoint: { watermarkField: string; lastValue: string | number; pauseTimestamp?: string },
    params: Omit<CollectParams, 'incrementalColumn' | 'lastSyncValue'>,
  ): Promise<CollectResult>;

  /**
   * Optional: Stream data using async iterable for very large datasets
   */
  collectStream?(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    params: CollectParams,
  ): AsyncIterable<any[]>;

  /**
   * Get sample data for preview
   */
  getSampleData?(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    sampleSize?: number,
  ): Promise<any[]>;

  /**
   * Validate source schema configuration
   */
  validateConfig?(sourceSchema: PipelineSourceSchemaWithConfig): {
    valid: boolean;
    errors: string[];
  };
}

/**
 * Handler registry type
 */
export type SourceHandlerRegistry = Map<DataSourceType, ISourceHandler>;

/**
 * Base abstract class for handlers with common functionality
 */
export abstract class BaseSourceHandler implements ISourceHandler {
  abstract readonly type: DataSourceType;

  abstract testConnection(connectionConfig: any): Promise<ConnectionTestResult>;

  abstract discoverSchema(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
  ): Promise<SchemaInfo>;

  abstract collect(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    params: CollectParams,
  ): Promise<CollectResult>;

  abstract collectIncremental(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    checkpoint: { watermarkField: string; lastValue: string | number; pauseTimestamp?: string },
    params: Omit<CollectParams, 'incrementalColumn' | 'lastSyncValue'>,
  ): Promise<CollectResult>;

  /**
   * Common retry logic for operations
   */
  protected async withRetry<T>(
    operation: () => Promise<T>,
    maxRetries: number = 3,
    delayMs: number = 1000,
  ): Promise<T> {
    let lastError: Error | undefined;
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await operation();
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        
        if (attempt < maxRetries) {
          await new Promise(resolve => setTimeout(resolve, delayMs * attempt));
        }
      }
    }
    
    throw lastError;
  }

  /**
   * Map database type to standardized type
   */
  protected normalizeDataType(dbType: string): string {
    const typeMap: Record<string, string> = {
      // Integer types
      'int': 'integer',
      'integer': 'integer',
      'int4': 'integer',
      'int8': 'bigint',
      'bigint': 'bigint',
      'smallint': 'smallint',
      'int2': 'smallint',
      'tinyint': 'smallint',
      
      // Floating point
      'float': 'float',
      'float4': 'float',
      'float8': 'double',
      'double': 'double',
      'double precision': 'double',
      'real': 'float',
      'numeric': 'decimal',
      'decimal': 'decimal',
      
      // String types
      'varchar': 'varchar',
      'character varying': 'varchar',
      'char': 'char',
      'character': 'char',
      'text': 'text',
      'string': 'string',
      
      // Date/Time
      'date': 'date',
      'time': 'time',
      'timestamp': 'timestamp',
      'timestamp with time zone': 'timestamptz',
      'timestamp without time zone': 'timestamp',
      'timestamptz': 'timestamptz',
      'datetime': 'datetime',
      
      // Boolean
      'boolean': 'boolean',
      'bool': 'boolean',
      
      // Binary
      'bytea': 'binary',
      'blob': 'binary',
      'binary': 'binary',
      
      // JSON
      'json': 'json',
      'jsonb': 'jsonb',
      
      // UUID
      'uuid': 'uuid',
      
      // Array
      'array': 'array',
    };

    const lowerType = dbType.toLowerCase();
    return typeMap[lowerType] || lowerType;
  }

  /**
   * Infer JavaScript type from value
   */
  protected inferTypeFromValue(value: any): string {
    if (value === null || value === undefined) {
      return 'null';
    }
    if (Array.isArray(value)) {
      return 'array';
    }
    if (value instanceof Date) {
      return 'datetime';
    }
    if (typeof value === 'object') {
      return 'object';
    }
    return typeof value;
  }
}
