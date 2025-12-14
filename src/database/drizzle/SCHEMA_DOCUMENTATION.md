# Database Schema Documentation

This document describes the database schema structure for the MantrixFlow data pipeline system.

## Schema Organization

The database schema is organized into two main modules, aligned with the application structure:

### 1. Data Sources Schema (`postgres-connectors.schema.ts`)
Contains tables related to PostgreSQL data source connections and management.

### 2. Data Pipelines Schema (`postgres-pipeline.schema.ts`)
Contains tables related to data pipeline execution and management.

## Tables

### Data Sources Module

#### `postgres_connections`
Stores PostgreSQL connection configurations with encrypted credentials.

**Key Fields:**
- `id` (uuid, PK) - Unique connection identifier
- `org_id` (uuid) - Organization ID
- `user_id` (uuid) - User who created the connection
- `name` (varchar) - Connection name
- `host`, `port`, `database`, `username`, `password` (encrypted)
- `ssl_enabled`, `ssl_ca_cert` - SSL configuration
- `ssh_tunnel_enabled`, `ssh_host`, `ssh_port`, `ssh_username`, `ssh_private_key` - SSH tunnel configuration
- `connection_pool_size`, `query_timeout_seconds` - Connection settings
- `status` (enum: 'active', 'inactive', 'error')
- `schema_cache` (jsonb) - Cached schema discovery results
- `last_connected_at`, `last_error` - Status tracking
- `created_at`, `updated_at` - Timestamps

**Indexes:** None (consider adding indexes on `org_id` and `status` for performance)

**Foreign Keys:** None

#### `postgres_sync_jobs`
Tracks data synchronization jobs from PostgreSQL sources to destinations.

**Key Fields:**
- `id` (uuid, PK) - Unique sync job identifier
- `connection_id` (uuid, FK → postgres_connections) - Source connection
- `table_name` (varchar) - Source table name
- `sync_mode` (enum: 'full', 'incremental')
- `incremental_column` (varchar) - Column for incremental sync
- `last_sync_value` (text) - Last synced value
- `destination_table` (varchar) - Destination table name
- `status` (enum: 'pending', 'running', 'success', 'failed')
- `rows_synced` (integer) - Number of rows synced
- `sync_frequency` (enum: 'manual', '15min', '1hour', '24hours')
- `next_sync_at` (timestamp) - Next scheduled sync time
- `custom_where_clause` (text) - Optional WHERE clause for filtering
- `started_at`, `completed_at`, `error_message` - Execution tracking
- `created_at`, `updated_at` - Timestamps

**Indexes:** None (consider adding indexes on `connection_id`, `status`, `next_sync_at`)

**Foreign Keys:**
- `connection_id` → `postgres_connections.id` (CASCADE DELETE)

#### `postgres_query_logs`
Audit log for all queries executed against PostgreSQL connections.

**Key Fields:**
- `id` (uuid, PK) - Unique log entry identifier
- `connection_id` (uuid, FK → postgres_connections) - Connection used
- `user_id` (uuid) - User who executed the query
- `query` (text) - SQL query executed
- `execution_time_ms` (integer) - Query execution time
- `rows_returned` (integer) - Number of rows returned
- `status` (enum: 'success', 'error')
- `error_message` (text) - Error message if failed
- `created_at` (timestamp) - When query was executed

**Indexes:** None (consider adding indexes on `connection_id`, `user_id`, `created_at` for querying)

**Foreign Keys:**
- `connection_id` → `postgres_connections.id` (CASCADE DELETE)

### Data Pipelines Module

#### `postgres_pipelines`
Stores pipeline configurations for data synchronization.

**Key Fields:**
- `id` (uuid, PK) - Unique pipeline identifier
- `org_id` (uuid) - Organization ID
- `user_id` (uuid) - User who created the pipeline
- `name` (varchar) - Pipeline name
- `description` (text) - Pipeline description

**Source Configuration:**
- `source_type` (varchar) - Type of source ('postgres', 'stripe', 'salesforce', etc.)
- `source_connection_id` (uuid, FK → postgres_connections) - Source connection (if PostgreSQL)
- `source_config` (jsonb) - Source configuration (for external sources)
- `source_schema` (varchar) - Source schema name
- `source_table` (varchar) - Source table name
- `source_query` (text) - Custom SQL query for source

**Destination Configuration:**
- `destination_connection_id` (uuid, FK → postgres_connections) - Destination connection
- `destination_schema` (varchar) - Destination schema (default: 'public')
- `destination_table` (varchar) - Destination table name
- `destination_table_exists` (boolean) - Whether destination table exists

**Schema Mapping:**
- `column_mappings` (jsonb) - Column mapping configuration
- `transformations` (jsonb) - Data transformation rules

**Write Configuration:**
- `write_mode` (enum: 'append', 'upsert', 'replace')
- `upsert_key` (jsonb) - Columns for upsert operations

**Sync Configuration:**
- `sync_mode` (varchar) - 'full' or 'incremental'
- `incremental_column` (varchar) - Column for incremental sync
- `last_sync_value` (text) - Last synced value
- `sync_frequency` (varchar) - 'manual', '15min', '1hour', '24hours'
- `next_sync_at` (timestamp) - Next scheduled sync

**Status & Statistics:**
- `status` (enum: 'active', 'paused', 'error')
- `last_run_at` (timestamp) - Last execution time
- `last_run_status` (enum: 'pending', 'running', 'success', 'failed', 'cancelled')
- `last_error` (text) - Last error message
- `total_rows_processed` (integer) - Total rows processed
- `total_runs_successful` (integer) - Successful runs count
- `total_runs_failed` (integer) - Failed runs count

**Metadata:**
- `created_at`, `updated_at`, `deleted_at` (timestamps)

**Indexes:** None (consider adding indexes on `org_id`, `status`, `next_sync_at`)

**Foreign Keys:**
- `source_connection_id` → `postgres_connections.id` (CASCADE DELETE)
- `destination_connection_id` → `postgres_connections.id` (CASCADE DELETE)

#### `postgres_pipeline_runs`
Tracks individual pipeline execution runs.

**Key Fields:**
- `id` (uuid, PK) - Unique run identifier
- `pipeline_id` (uuid, FK → postgres_pipelines) - Pipeline executed
- `org_id` (uuid) - Organization ID

**Execution Status:**
- `status` (enum: 'pending', 'running', 'success', 'failed', 'cancelled')

**Execution Metrics:**
- `rows_read` (integer) - Rows read from source
- `rows_written` (integer) - Rows written to destination
- `rows_skipped` (integer) - Rows skipped
- `rows_failed` (integer) - Rows that failed
- `bytes_processed` (integer) - Bytes processed

**Timing:**
- `started_at` (timestamp) - When run started
- `completed_at` (timestamp) - When run completed
- `duration_seconds` (integer) - Execution duration

**Error Tracking:**
- `error_message` (text) - Error message
- `error_code` (varchar) - Error code
- `error_stack` (text) - Error stack trace

**Metadata:**
- `trigger_type` (enum: 'manual', 'scheduled', 'webhook')
- `triggered_by` (uuid) - User who triggered the run
- `run_metadata` (jsonb) - Additional run metadata
- `created_at` (timestamp) - When run was created

**Indexes:** None (consider adding indexes on `pipeline_id`, `org_id`, `status`, `created_at`)

**Foreign Keys:**
- `pipeline_id` → `postgres_pipelines.id` (CASCADE DELETE)

## Enums

### Connection Status
- `active` - Connection is active and working
- `inactive` - Connection is inactive
- `error` - Connection has an error

### Sync Job Status
- `pending` - Job is pending execution
- `running` - Job is currently running
- `success` - Job completed successfully
- `failed` - Job failed

### Sync Frequency
- `manual` - Manual execution only
- `15min` - Every 15 minutes
- `1hour` - Every hour
- `24hours` - Every 24 hours

### Sync Mode
- `full` - Full table sync
- `incremental` - Incremental sync based on column

### Query Log Status
- `success` - Query executed successfully
- `error` - Query execution failed

### Pipeline Status
- `active` - Pipeline is active
- `paused` - Pipeline is paused
- `error` - Pipeline has an error

### Run Status
- `pending` - Run is pending
- `running` - Run is executing
- `success` - Run completed successfully
- `failed` - Run failed
- `cancelled` - Run was cancelled

### Trigger Type
- `manual` - Manually triggered
- `scheduled` - Scheduled execution
- `webhook` - Webhook triggered

### Write Mode
- `append` - Append new rows
- `upsert` - Update or insert rows
- `replace` - Replace all data

## JSONB Column Structures

### `schema_cache` (postgres_connections)
```typescript
{
  databases: string[];
  schemas: Array<{
    name: string;
    description?: string;
  }>;
  tables: Array<{
    name: string;
    schema: string;
    rowCount: number;
    sizeMB: number;
    columns: Array<{
      name: string;
      type: string;
      nullable: boolean;
    }>;
  }>;
  cachedAt: string; // ISO timestamp
}
```

### `column_mappings` (postgres_pipelines)
```typescript
Array<{
  sourceColumn: string;
  destinationColumn: string;
  dataType: string;
  nullable: boolean;
  defaultValue?: string;
  isPrimaryKey?: boolean;
  maxLength?: number;
}>
```

### `transformations` (postgres_pipelines)
```typescript
Array<{
  sourceColumn: string;
  transformType: 'rename' | 'cast' | 'concat' | 'split' | 'custom';
  transformConfig: any;
  destinationColumn: string;
}>
```

### `source_config` (postgres_pipelines)
```typescript
{
  apiKey?: string;
  accountId?: string;
  endpoint?: string;
  accessToken?: string;
  refreshToken?: string;
  instanceUrl?: string;
  [key: string]: any;
}
```

### `upsert_key` (postgres_pipelines)
```typescript
string[] // Array of column names for upsert
```

### `run_metadata` (postgres_pipeline_runs)
```typescript
{
  batchSize: number;
  parallelWorkers?: number;
  sourceChecksum?: string;
  destinationChecksum?: string;
  [key: string]: any;
}
```

## Migration Commands

### Generate Migration
```bash
bun run db:generate
```

### Run Migrations
```bash
bun run db:migrate
```

### Push Schema (Development)
```bash
bun run db:push
```

### Open Drizzle Studio
```bash
bun run db:studio
```

## Recommended Indexes

For better query performance, consider adding these indexes:

```sql
-- postgres_connections
CREATE INDEX idx_postgres_connections_org_id ON postgres_connections(org_id);
CREATE INDEX idx_postgres_connections_status ON postgres_connections(status);

-- postgres_sync_jobs
CREATE INDEX idx_postgres_sync_jobs_connection_id ON postgres_sync_jobs(connection_id);
CREATE INDEX idx_postgres_sync_jobs_status ON postgres_sync_jobs(status);
CREATE INDEX idx_postgres_sync_jobs_next_sync_at ON postgres_sync_jobs(next_sync_at) WHERE next_sync_at IS NOT NULL;

-- postgres_query_logs
CREATE INDEX idx_postgres_query_logs_connection_id ON postgres_query_logs(connection_id);
CREATE INDEX idx_postgres_query_logs_user_id ON postgres_query_logs(user_id);
CREATE INDEX idx_postgres_query_logs_created_at ON postgres_query_logs(created_at DESC);

-- postgres_pipelines
CREATE INDEX idx_postgres_pipelines_org_id ON postgres_pipelines(org_id);
CREATE INDEX idx_postgres_pipelines_status ON postgres_pipelines(status);
CREATE INDEX idx_postgres_pipelines_next_sync_at ON postgres_pipelines(next_sync_at) WHERE next_sync_at IS NOT NULL;

-- postgres_pipeline_runs
CREATE INDEX idx_postgres_pipeline_runs_pipeline_id ON postgres_pipeline_runs(pipeline_id);
CREATE INDEX idx_postgres_pipeline_runs_org_id ON postgres_pipeline_runs(org_id);
CREATE INDEX idx_postgres_pipeline_runs_status ON postgres_pipeline_runs(status);
CREATE INDEX idx_postgres_pipeline_runs_created_at ON postgres_pipeline_runs(created_at DESC);
```

## Security Considerations

1. **Encrypted Fields**: The following fields are encrypted at rest:
   - `host`, `database`, `username`, `password` (postgres_connections)
   - `ssl_ca_cert` (postgres_connections)
   - `ssh_host`, `ssh_username`, `ssh_private_key` (postgres_connections)

2. **Row-Level Security**: Consider implementing RLS policies to ensure:
   - Users can only access connections for their organization
   - Users can only access pipelines for their organization
   - Query logs are only visible to authorized users

3. **Audit Trail**: The `postgres_query_logs` table provides an audit trail for all queries executed.

## Relationships

```
postgres_connections (1) ──< (many) postgres_sync_jobs
postgres_connections (1) ──< (many) postgres_query_logs
postgres_connections (1) ──< (many) postgres_pipelines (as source)
postgres_connections (1) ──< (many) postgres_pipelines (as destination)
postgres_pipelines (1) ──< (many) postgres_pipeline_runs
```

## Notes

- All timestamps use PostgreSQL's `timestamp` type (without timezone)
- UUIDs are used for all primary keys and foreign keys
- JSONB columns allow flexible schema for configuration data
- Cascade deletes ensure data consistency when connections or pipelines are deleted
- The schema supports both PostgreSQL sources and external sources (Stripe, Salesforce, etc.)

