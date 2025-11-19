import {
    pgTable,
    uuid,
    varchar,
    integer,
    boolean,
    text,
    timestamp,
    jsonb,
    pgEnum,
} from 'drizzle-orm/pg-core';
import { postgresConnections } from './postgres-connectors.schema';

/**
 * Enum for write mode
 */
export const writeModeEnum = pgEnum('write_mode', [
    'append',
    'upsert',
    'replace',
]);

/**
 * Enum for pipeline status
 */
export const pipelineStatusEnum = pgEnum('pipeline_status', [
    'active',
    'paused',
    'error',
]);

/**
 * Enum for run status
 */
export const runStatusEnum = pgEnum('run_status', [
    'pending',
    'running',
    'success',
    'failed',
    'cancelled',
]);

/**
 * Enum for trigger type
 */
export const triggerTypeEnum = pgEnum('trigger_type', [
    'manual',
    'scheduled',
    'webhook',
]);

/**
 * PostgreSQL Pipelines Table
 * Stores pipeline configurations for data synchronization
 */
export const postgresPipelines = pgTable('postgres_pipelines', {
    id: uuid('id').primaryKey().defaultRandom(),
    orgId: uuid('org_id').notNull(),
    userId: uuid('user_id').notNull(),
    name: varchar('name', { length: 255 }).notNull(),
    description: text('description'),

    // Source configuration
    sourceType: varchar('source_type', { length: 100 }).notNull(), // 'postgres', 'stripe', 'salesforce', etc.
    sourceConnectionId: uuid('source_connection_id').references(
        () => postgresConnections.id,
        { onDelete: 'cascade' },
    ), // If source is postgres
    sourceConfig: jsonb('source_config').$type<SourceConfig>(), // If source is external
    sourceSchema: varchar('source_schema', { length: 255 }),
    sourceTable: varchar('source_table', { length: 255 }),
    sourceQuery: text('source_query'), // Custom SQL for source

    // Destination configuration (PostgreSQL)
    destinationConnectionId: uuid('destination_connection_id')
        .notNull()
        .references(() => postgresConnections.id, { onDelete: 'cascade' }),
    destinationSchema: varchar('destination_schema', { length: 255 }).default(
        'public',
    ),
    destinationTable: varchar('destination_table', { length: 255 }).notNull(),
    destinationTableExists: boolean('destination_table_exists').default(false),

    // Schema mapping
    columnMappings: jsonb('column_mappings').$type<ColumnMapping[]>(),
    transformations: jsonb('transformations').$type<Transformation[]>(),

    // Write mode
    writeMode: writeModeEnum('write_mode').default('append'),
    upsertKey: jsonb('upsert_key').$type<string[]>(), // Columns for upsert (e.g., ['id', 'email'])

    // Sync configuration
    syncMode: varchar('sync_mode', { length: 50 }).default('full'), // 'full' or 'incremental'
    incrementalColumn: varchar('incremental_column', { length: 255 }),
    lastSyncValue: text('last_sync_value'),
    syncFrequency: varchar('sync_frequency', { length: 50 }).default('manual'), // 'manual', '15min', '1hour', '24hours'
    nextSyncAt: timestamp('next_sync_at'),

    // Execution status
    status: pipelineStatusEnum('status').default('active'),
    lastRunAt: timestamp('last_run_at'),
    lastRunStatus: runStatusEnum('last_run_status'),
    lastError: text('last_error'),

    // Statistics
    totalRowsProcessed: integer('total_rows_processed').default(0),
    totalRunsSuccessful: integer('total_runs_successful').default(0),
    totalRunsFailed: integer('total_runs_failed').default(0),

    // Metadata
    createdAt: timestamp('created_at').notNull().defaultNow(),
    updatedAt: timestamp('updated_at').notNull().defaultNow(),
    deletedAt: timestamp('deleted_at'),
});

/**
 * PostgreSQL Pipeline Runs Table
 * Tracks individual pipeline execution runs
 */
export const postgresPipelineRuns = pgTable('postgres_pipeline_runs', {
    id: uuid('id').primaryKey().defaultRandom(),
    pipelineId: uuid('pipeline_id')
        .notNull()
        .references(() => postgresPipelines.id, { onDelete: 'cascade' }),
    orgId: uuid('org_id').notNull(),

    status: runStatusEnum('status').default('pending'),

    // Execution details
    rowsRead: integer('rows_read').default(0),
    rowsWritten: integer('rows_written').default(0),
    rowsSkipped: integer('rows_skipped').default(0),
    rowsFailed: integer('rows_failed').default(0),
    bytesProcessed: integer('bytes_processed').default(0),

    // Timing
    startedAt: timestamp('started_at'),
    completedAt: timestamp('completed_at'),
    durationSeconds: integer('duration_seconds'),

    // Error tracking
    errorMessage: text('error_message'),
    errorCode: varchar('error_code', { length: 50 }),
    errorStack: text('error_stack'),

    // Metadata
    triggerType: triggerTypeEnum('trigger_type').default('manual'),
    triggeredBy: uuid('triggered_by'), // user_id
    runMetadata: jsonb('run_metadata').$type<RunMetadata>(),

    createdAt: timestamp('created_at').notNull().defaultNow(),
});

/**
 * TypeScript Interfaces for JSONB columns
 */

/**
 * Source configuration for external sources (Stripe, Salesforce, etc.)
 */
export interface SourceConfig {
    // For external sources
    apiKey?: string;
    accountId?: string;
    endpoint?: string;
    accessToken?: string;
    refreshToken?: string;
    instanceUrl?: string;
    // Source-specific configurations
    [key: string]: any;
}

/**
 * Column mapping from source to destination
 */
export interface ColumnMapping {
    sourceColumn: string;
    destinationColumn: string;
    dataType: string; // PostgreSQL data type
    nullable: boolean;
    defaultValue?: string;
    isPrimaryKey?: boolean;
    maxLength?: number;
}

/**
 * Data transformation configuration
 */
export interface Transformation {
    sourceColumn: string;
    transformType: 'rename' | 'cast' | 'concat' | 'split' | 'custom';
    transformConfig: any; // transformation-specific config
    destinationColumn: string;
}

/**
 * Pipeline run metadata
 */
export interface RunMetadata {
    batchSize: number;
    parallelWorkers?: number;
    sourceChecksum?: string;
    destinationChecksum?: string;
    [key: string]: any;
}

/**
 * Type exports for TypeScript
 */
export type PostgresPipeline = typeof postgresPipelines.$inferSelect;
export type NewPostgresPipeline = typeof postgresPipelines.$inferInsert;
export type PostgresPipelineRun = typeof postgresPipelineRuns.$inferSelect;
export type NewPostgresPipelineRun = typeof postgresPipelineRuns.$inferInsert;
