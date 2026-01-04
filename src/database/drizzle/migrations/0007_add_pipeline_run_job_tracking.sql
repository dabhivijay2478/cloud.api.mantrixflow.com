-- Migration: Add Job Tracking Fields to Pipeline Runs
-- This migration adds authoritative job tracking fields to postgres_pipeline_runs
-- to support the new architectural pattern where:
-- 1. Destination table is resolved and locked during setup phase
-- 2. Job state drives migration behavior
-- 3. Run record is the AUTHORITATIVE source of truth

-- Step 1: Create job_state enum
DO $$ BEGIN
    CREATE TYPE "job_state" AS ENUM (
        'pending',
        'setup',
        'running',
        'paused',
        'listing',
        'stopped',
        'completed',
        'error'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Step 2: Add resolved destination table fields (AUTHORITATIVE - locked during setup)
ALTER TABLE "postgres_pipeline_runs" 
    ADD COLUMN IF NOT EXISTS "resolved_destination_schema" varchar(255),
    ADD COLUMN IF NOT EXISTS "resolved_destination_table" varchar(255),
    ADD COLUMN IF NOT EXISTS "destination_table_was_created" varchar(10), -- 'true' | 'false'
    ADD COLUMN IF NOT EXISTS "resolved_column_mappings" jsonb;

-- Step 3: Add job state tracking fields (AUTHORITATIVE - drives migration behavior)
ALTER TABLE "postgres_pipeline_runs"
    ADD COLUMN IF NOT EXISTS "job_state" "job_state" DEFAULT 'pending',
    ADD COLUMN IF NOT EXISTS "last_sync_cursor" text, -- Authoritative cursor for incremental sync
    ADD COLUMN IF NOT EXISTS "job_state_updated_at" timestamp;

-- Step 4: Add updated_at column if it doesn't exist
ALTER TABLE "postgres_pipeline_runs"
    ADD COLUMN IF NOT EXISTS "updated_at" timestamp DEFAULT now() NOT NULL;

-- Step 5: Create index on job_state for efficient querying
CREATE INDEX IF NOT EXISTS "idx_postgres_pipeline_runs_job_state" 
    ON "postgres_pipeline_runs"("job_state");

-- Step 6: Create index on resolved_destination_table for lookups
CREATE INDEX IF NOT EXISTS "idx_postgres_pipeline_runs_resolved_destination" 
    ON "postgres_pipeline_runs"("resolved_destination_schema", "resolved_destination_table");

-- Step 7: Create index on last_sync_cursor for incremental sync queries
CREATE INDEX IF NOT EXISTS "idx_postgres_pipeline_runs_last_sync_cursor" 
    ON "postgres_pipeline_runs"("last_sync_cursor") 
    WHERE "last_sync_cursor" IS NOT NULL;

-- Migration complete
-- The postgres_pipeline_runs table now serves as the AUTHORITATIVE source of truth for:
-- - Resolved destination table (locked during setup)
-- - Job execution state
-- - Migration progress and cursor

