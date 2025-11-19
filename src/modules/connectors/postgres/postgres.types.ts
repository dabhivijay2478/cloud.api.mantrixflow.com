/**
 * PostgreSQL Connector Types
 * Type definitions for PostgreSQL connector functionality
 */

import { z } from 'zod';
import type { PostgresConnection as PostgresConnectionSchema } from '../../../database/drizzle/schema/postgres-connectors.schema';

/**
 * Re-export PostgresConnection from schema for convenience
 */
export type PostgresConnection = PostgresConnectionSchema;

/**
 * Connection Status
 */
export enum ConnectionStatus {
  ACTIVE = 'active',
  INACTIVE = 'inactive',
  ERROR = 'error',
}

/**
 * Sync Job Status
 */
export enum SyncJobStatus {
  PENDING = 'pending',
  RUNNING = 'running',
  SUCCESS = 'success',
  FAILED = 'failed',
}

/**
 * Sync Mode
 */
export enum SyncMode {
  FULL = 'full',
  INCREMENTAL = 'incremental',
}

/**
 * Sync Frequency
 */
export enum SyncFrequency {
  MANUAL = 'manual',
  MIN_15 = '15min',
  HOUR_1 = '1hour',
  HOURS_24 = '24hours',
}

/**
 * PostgreSQL Connection Configuration
 */
export interface PostgresConnectionConfig {
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  ssl?: {
    enabled: boolean;
    caCert?: string;
    rejectUnauthorized?: boolean;
  };
  sshTunnel?: {
    enabled: boolean;
    host: string;
    port: number;
    username: string;
    privateKey: string;
  };
  connectionTimeout?: number; // milliseconds
  queryTimeout?: number; // milliseconds
  poolSize?: number;
}

/**
 * Decrypted Connection Credentials (for internal use only)
 */
export interface DecryptedConnectionCredentials {
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  sslEnabled: boolean;
  sslCaCert?: string;
  sshTunnelEnabled: boolean;
  sshHost?: string;
  sshPort?: number;
  sshUsername?: string;
  sshPrivateKey?: string;
  connectionPoolSize?: number;
}

/**
 * Database Information
 */
export interface DatabaseInfo {
  name: string;
  size?: string;
  encoding?: string;
  collation?: string;
}

/**
 * Schema Information
 */
export interface SchemaInfo {
  name: string;
  owner?: string;
  tables?: TableInfo[]; // Tables in this schema (optional for backward compatibility)
}

/**
 * Column Information
 */
export interface ColumnInfo {
  name: string;
  dataType: string;
  tsType: string; // TypeScript type mapping
  isNullable: boolean;
  isPrimaryKey: boolean;
  isForeignKey: boolean;
  foreignKeyTable?: string;
  foreignKeyColumn?: string;
  defaultValue?: string;
  maxLength?: number;
  numericPrecision?: number;
  numericScale?: number;
  isArray?: boolean;
  isJsonb?: boolean;
  isEnum?: boolean;
  enumValues?: string[];
}

/**
 * Index Information
 */
export interface IndexInfo {
  name: string;
  columns: string[];
  isUnique: boolean;
  isPrimary: boolean;
}

/**
 * Table Information
 */
export interface TableInfo {
  name: string;
  schema: string;
  rowCount: number;
  size: number; // bytes
  sizeFormatted: string; // human-readable (e.g., "1.5 MB")
  columns: ColumnInfo[];
  primaryKeys: string[];
  foreignKeys: Array<{
    column: string;
    referencedTable: string;
    referencedColumn: string;
  }>;
  indexes: IndexInfo[];
  isView: boolean;
  isMaterializedView: boolean;
  isPartitioned: boolean;
  parentTable?: string; // for partitioned tables
  lastUpdated?: Date;
}

/**
 * Schema Discovery Result
 */
export interface SchemaDiscoveryResult {
  databases: DatabaseInfo[];
  schemas: SchemaInfo[];
  tables: TableInfo[];
  cached: boolean;
  cachedAt?: Date;
}

/**
 * Query Execution Result
 */
export interface QueryExecutionResult {
  rows: any[];
  rowCount: number;
  columns: Array<{
    name: string;
    dataType: string;
  }>;
  executionTimeMs: number;
  queryPlan?: any; // EXPLAIN result
}

/**
 * Sync Progress
 */
export interface SyncProgress {
  jobId: string;
  connectionId: string;
  tableName: string;
  status: SyncJobStatus;
  rowsSynced: number;
  totalRows?: number;
  percentage?: number;
  startedAt: Date;
  estimatedCompletion?: Date;
  error?: string;
}

/**
 * Connection Health Status
 */
export interface ConnectionHealth {
  status: 'healthy' | 'unhealthy' | 'error';
  lastChecked: Date;
  responseTimeMs?: number;
  error?: string;
  version?: string;
  activeConnections?: number;
  maxConnections?: number;
}

/**
 * Query Log Entry
 */
export interface QueryLogEntry {
  id: string;
  connectionId: string;
  userId: string;
  query: string;
  executionTimeMs: number;
  rowsReturned: number;
  status: 'success' | 'error';
  errorMessage?: string;
  createdAt: Date;
}

/**
 * Connection Metrics
 */
export interface ConnectionMetrics {
  connectionId: string;
  totalQueries: number;
  successfulQueries: number;
  failedQueries: number;
  averageExecutionTimeMs: number;
  slowQueries: number; // queries > 10 seconds
  totalRowsReturned: number;
  dataVolumeTransferred: number; // bytes
  lastQueryAt?: Date;
  connectionPoolUtilization: number; // percentage
}

/**
 * AI Hints for PostgreSQL Connection
 */
export interface PostgresAIHints {
  connectorType: 'postgresql';
  timeFields: string[];
  amountFields: string[];
  countFields: string[];
  idFields: string[];
  foreignKeyPattern: string;
  numericAggs: string[];
  textAggs: string[];
  timestampAggs: string[];
  suggestJoins: boolean;
  samplePrompts: string[];
}

/**
 * Zod Schemas for Validation
 */
export const PostgresConnectionConfigSchema = z.object({
  host: z.string().min(1).max(255),
  port: z.number().int().min(1).max(65535).default(5432),
  database: z.string().min(1).max(255),
  username: z.string().min(1).max(255),
  password: z.string().min(1),
  ssl: z
    .object({
      enabled: z.boolean().default(false),
      caCert: z.string().optional(),
      rejectUnauthorized: z.boolean().default(true),
    })
    .optional(),
  sshTunnel: z
    .object({
      enabled: z.boolean().default(false),
      host: z.string().min(1),
      port: z.number().int().min(1).max(65535),
      username: z.string().min(1),
      privateKey: z.string().min(1),
    })
    .optional(),
  connectionTimeout: z.number().int().positive().optional(),
  queryTimeout: z.number().int().positive().optional(),
  poolSize: z.number().int().min(1).max(10).optional(),
});

export const TestConnectionSchema = PostgresConnectionConfigSchema;

export const ExecuteQuerySchema = z.object({
  query: z.string().min(1),
  timeout: z.number().int().positive().optional(),
  limit: z.number().int().positive().max(10000).optional(),
  offset: z.number().int().nonnegative().optional(),
});

export const CreateSyncJobSchema = z.object({
  tableName: z.string().min(1),
  schema: z.string().default('public'),
  syncMode: z.enum(['full', 'incremental']),
  incrementalColumn: z.string().optional(),
  customWhereClause: z.string().optional(),
  syncFrequency: z
    .enum(['manual', '15min', '1hour', '24hours'])
    .default('manual'),
});

export const UpdateSyncScheduleSchema = z.object({
  syncFrequency: z.enum(['manual', '15min', '1hour', '24hours']),
  nextSyncAt: z.date().optional(),
});

/**
 * Pipeline-related Types
 */

/**
 * Write Mode for destination
 */
export enum WriteMode {
  APPEND = 'append',
  UPSERT = 'upsert',
  REPLACE = 'replace',
}

/**
 * Pipeline Status
 */
export enum PipelineStatus {
  ACTIVE = 'active',
  PAUSED = 'paused',
  ERROR = 'error',
}

/**
 * Pipeline Run Status
 */
export enum PipelineRunStatus {
  PENDING = 'pending',
  RUNNING = 'running',
  SUCCESS = 'success',
  FAILED = 'failed',
  CANCELLED = 'cancelled',
}

/**
 * Trigger Type
 */
export enum TriggerType {
  MANUAL = 'manual',
  SCHEDULED = 'scheduled',
  WEBHOOK = 'webhook',
}

/**
 * Pipeline Configuration
 */
export interface Pipeline {
  id: string;
  orgId: string;
  userId: string;
  name: string;
  description?: string;
  sourceType: string;
  sourceConnectionId?: string;
  sourceConfig?: any;
  sourceSchema?: string;
  sourceTable?: string;
  sourceQuery?: string;
  destinationConnectionId: string;
  destinationSchema: string;
  destinationTable: string;
  destinationTableExists: boolean;
  columnMappings?: ColumnMapping[];
  transformations?: Transformation[];
  writeMode: WriteMode;
  upsertKey?: string[];
  syncMode: SyncMode;
  incrementalColumn?: string;
  lastSyncValue?: string;
  syncFrequency: SyncFrequency;
  nextSyncAt?: Date;
  status: PipelineStatus;
  lastRunAt?: Date;
  lastRunStatus?: PipelineRunStatus;
  lastError?: string;
  totalRowsProcessed: number;
  totalRunsSuccessful: number;
  totalRunsFailed: number;
  createdAt: Date;
  updatedAt: Date;
  deletedAt?: Date;
}

/**
 * Pipeline Run Result
 */
export interface PipelineRunResult {
  runId: string;
  status: 'success' | 'failed';
  rowsRead: number;
  rowsWritten: number;
  rowsSkipped: number;
  rowsFailed: number;
  durationSeconds: number;
  errors: PipelineError[];
}

/**
 * Pipeline Error
 */
export interface PipelineError {
  stage: 'source' | 'transform' | 'destination';
  message: string;
  row?: number;
  code?: string;
}

/**
 * Write Result
 */
export interface WriteResult {
  rowsWritten: number;
  rowsSkipped: number;
  rowsFailed: number;
  errors: WriteError[];
}

/**
 * Write Error
 */
export interface WriteError {
  row: number;
  error: string;
  data: any;
}

/**
 * Schema Validation Result
 */
export interface SchemaValidationResult {
  valid: boolean;
  missingColumns: string[];
  typeMismatches: TypeMismatch[];
}

/**
 * Type Mismatch
 */
export interface TypeMismatch {
  column: string;
  expectedType: string;
  actualType: string;
}

/**
 * Table Statistics
 */
export interface TableStats {
  rowCount: number;
  sizeBytes: number;
  lastUpdated: Date;
}

/**
 * Validation Result
 */
export interface ValidationResult {
  valid: boolean;
  errors: string[];
  warnings: string[];
}

/**
 * Dry Run Result
 */
export interface DryRunResult {
  sourceRowCount: number;
  sampleRows: any[];
  destinationSchemaPreview: ColumnMapping[];
  estimatedDuration: number;
}

/**
 * Type Inference Result
 */
export interface TypeInferenceResult {
  column: string;
  inferredType: string;
  confidence: number;
  alternatives: string[];
}

/**
 * Validation Error
 */
export interface ValidationError {
  column: string;
  error: string;
  severity: 'error' | 'warning';
}

/**
 * Column Mapping (re-exported from schema)
 */
export interface ColumnMapping {
  sourceColumn: string;
  destinationColumn: string;
  dataType: string;
  nullable: boolean;
  defaultValue?: string;
  isPrimaryKey?: boolean;
  maxLength?: number;
}

/**
 * Transformation (re-exported from schema)
 */
export interface Transformation {
  sourceColumn: string;
  transformType: 'rename' | 'cast' | 'concat' | 'split' | 'custom';
  transformConfig: any;
  destinationColumn: string;
}
