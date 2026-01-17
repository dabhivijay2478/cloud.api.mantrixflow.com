-- Pipeline Lifecycle Migration
-- Adds new lifecycle status fields and checkpoint support

-- First, we need to update the pipeline_status enum with new values
-- This requires dropping and recreating the enum (Postgres limitation)

-- Step 1: Create a temporary column to preserve existing status values
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS status_backup varchar(50);
UPDATE pipelines SET status_backup = status::text WHERE status IS NOT NULL;

-- Step 2: Drop the column using the old enum
ALTER TABLE pipelines DROP COLUMN IF EXISTS status;

-- Step 3: Drop the old enum and create the new one
DROP TYPE IF EXISTS pipeline_status;
CREATE TYPE pipeline_status AS ENUM (
  'idle',
  'initializing', 
  'running',
  'listing',
  'listening',
  'paused',
  'failed',
  'completed'
);

-- Step 4: Add the status column back with the new enum
ALTER TABLE pipelines ADD COLUMN status pipeline_status DEFAULT 'idle';

-- Step 5: Migrate old status values to new ones
UPDATE pipelines SET status = 
  CASE 
    WHEN status_backup = 'active' THEN 'idle'::pipeline_status
    WHEN status_backup = 'paused' THEN 'paused'::pipeline_status
    WHEN status_backup = 'error' THEN 'failed'::pipeline_status
    ELSE 'idle'::pipeline_status
  END
WHERE status_backup IS NOT NULL;

-- Step 6: Drop the backup column
ALTER TABLE pipelines DROP COLUMN IF EXISTS status_backup;

-- Add new lifecycle fields
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS checkpoint jsonb;
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS last_sync_at timestamp;
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS polling_interval_seconds integer DEFAULT 300;
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS polling_config jsonb;

-- Add comments for documentation
COMMENT ON COLUMN pipelines.status IS 'Current lifecycle status: idle, initializing, running, listing, listening, paused, failed, completed';
COMMENT ON COLUMN pipelines.checkpoint IS 'Checkpoint data for resumable syncs (cursor, offset, WAL position, etc.)';
COMMENT ON COLUMN pipelines.last_sync_at IS 'Timestamp of last successful sync';
COMMENT ON COLUMN pipelines.polling_interval_seconds IS 'Interval in seconds between polls when in LISTING mode';
COMMENT ON COLUMN pipelines.polling_config IS 'Configuration for polling behavior (batch size, backoff, etc.)';

-- Create an index for faster scheduled pipeline lookups
CREATE INDEX IF NOT EXISTS idx_pipelines_scheduled_sync ON pipelines (organization_id, status, sync_frequency, next_sync_at)
WHERE deleted_at IS NULL AND sync_frequency != 'manual';
