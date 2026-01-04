# Migration Instructions - Pipeline Architecture Refactor

## Migration File

**File:** `apps/api/src/database/drizzle/migrations/0007_add_pipeline_run_job_tracking.sql`

## To Apply Migration

### Option 1: Using psql (Recommended)
```bash
cd apps/api
psql $DATABASE_URL -f src/database/drizzle/migrations/0007_add_pipeline_run_job_tracking.sql
```

### Option 2: Using Drizzle Kit
```bash
cd apps/api
# First, update the migration journal if needed
# Then run:
bun run db:migrate
```

### Option 3: Manual Execution
1. Connect to your PostgreSQL database
2. Copy the contents of `0007_add_pipeline_run_job_tracking.sql`
3. Execute in your database client

## What This Migration Does

1. **Creates `job_state` enum** with values:
   - pending, setup, running, paused, listing, stopped, completed, error

2. **Adds new columns to `postgres_pipeline_runs`:**
   - `resolved_destination_schema` - Locked destination schema
   - `resolved_destination_table` - Locked destination table
   - `destination_table_was_created` - Whether table was created
   - `resolved_column_mappings` - Authoritative column mappings
   - `job_state` - Authoritative job execution state
   - `last_sync_cursor` - Authoritative cursor for incremental sync
   - `job_state_updated_at` - Timestamp of last state update
   - `updated_at` - General update timestamp

3. **Creates indexes:**
   - Index on `job_state` for efficient querying
   - Index on resolved destination (schema, table)
   - Index on `last_sync_cursor` for incremental sync queries

## Verification

After running the migration, verify the changes:

```sql
-- Check if enum was created
SELECT enum_range(NULL::job_state);

-- Check if columns were added
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'postgres_pipeline_runs' 
  AND column_name IN (
    'resolved_destination_schema',
    'resolved_destination_table',
    'job_state',
    'last_sync_cursor'
  );

-- Check if indexes were created
SELECT indexname 
FROM pg_indexes 
WHERE tablename = 'postgres_pipeline_runs' 
  AND indexname LIKE '%job_state%' OR indexname LIKE '%resolved%' OR indexname LIKE '%cursor%';
```

## Rollback (if needed)

If you need to rollback this migration:

```sql
-- Drop indexes
DROP INDEX IF EXISTS idx_postgres_pipeline_runs_job_state;
DROP INDEX IF EXISTS idx_postgres_pipeline_runs_resolved_destination;
DROP INDEX IF EXISTS idx_postgres_pipeline_runs_last_sync_cursor;

-- Drop columns
ALTER TABLE postgres_pipeline_runs 
  DROP COLUMN IF EXISTS resolved_destination_schema,
  DROP COLUMN IF EXISTS resolved_destination_table,
  DROP COLUMN IF EXISTS destination_table_was_created,
  DROP COLUMN IF EXISTS resolved_column_mappings,
  DROP COLUMN IF EXISTS job_state,
  DROP COLUMN IF EXISTS last_sync_cursor,
  DROP COLUMN IF EXISTS job_state_updated_at,
  DROP COLUMN IF EXISTS updated_at;

-- Drop enum (only if no other tables use it)
DROP TYPE IF EXISTS job_state;
```

## Postgres Sync Jobs

**Note:** `postgres_sync_jobs` is a **separate system** from pipelines. It's used for data source sync operations (legacy system). The sync service has been updated to properly track state, progress, and cursor during execution.

See `SYNC_JOBS_VS_PIPELINE_RUNS.md` for details on the separation.

