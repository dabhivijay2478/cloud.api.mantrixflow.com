-- Migration: Pipeline Incremental Sync Fixes
-- Adds pause_timestamp, PGMQ setup, pg_cron polling, and NOTIFY triggers
--
-- Changes:
-- 1. Add pause_timestamp column to pipelines table
-- 2. Ensure PGMQ extension is available (assumes already installed)
-- 3. Create pg_cron polling function for automated incremental syncs
-- 4. Create NOTIFY triggers for real-time updates
-- 5. Schedule pg_cron job for polling

BEGIN;

-- ============================================================================
-- STEP 1: Add pause_timestamp column
-- ============================================================================

ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS pause_timestamp timestamp;

COMMENT ON COLUMN pipelines.pause_timestamp IS 'Timestamp when pipeline was paused (for delta calculation on resume)';

-- ============================================================================
-- STEP 2: Ensure PGMQ extension is available
-- ============================================================================
-- Note: This assumes PGMQ is already installed in the database
-- If not installed, run: CREATE EXTENSION IF NOT EXISTS pgmq;

-- Create PGMQ queue for incremental sync jobs (if not exists)
-- PGMQ queues are created automatically on first send, but we can verify
DO $$
BEGIN
  -- Verify PGMQ is available
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgmq') THEN
    RAISE WARNING 'PGMQ extension not found. Please install it: CREATE EXTENSION IF NOT EXISTS pgmq;';
  END IF;
END $$;

-- ============================================================================
-- STEP 3: Create polling function for pg_cron
-- ============================================================================

CREATE OR REPLACE FUNCTION pipeline_polling_function()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  pipeline_record RECORD;
  checkpoint_data jsonb;
  last_sync_value text;
  watermark_field text;
  changes_detected boolean;
  job_id bigint;
BEGIN
  -- Loop through all active pipelines in LISTING mode
  FOR pipeline_record IN
    SELECT 
      p.id,
      p.organization_id,
      p.status,
      p.sync_mode,
      p.incremental_column,
      p.last_sync_value,
      p.checkpoint,
      p.source_schema_id,
      p.destination_schema_id
    FROM pipelines p
    WHERE p.status = 'listing'
      AND p.sync_mode = 'incremental'
      AND p.incremental_column IS NOT NULL
      AND p.deleted_at IS NULL
  LOOP
    -- Skip if pipeline is paused
    IF pipeline_record.status = 'paused' THEN
      CONTINUE;
    END IF;

    -- Get checkpoint data
    checkpoint_data := pipeline_record.checkpoint;
    last_sync_value := COALESCE(
      checkpoint_data->>'lastSyncValue',
      pipeline_record.last_sync_value
    );
    watermark_field := pipeline_record.incremental_column;

    -- If no last_sync_value, skip (needs full sync first)
    IF last_sync_value IS NULL THEN
      CONTINUE;
    END IF;

    -- Check for changes by querying source schema
    -- This is a simplified check - actual delta detection happens in the handler
    -- For now, we'll always enqueue if we have a checkpoint (handler will do the filtering)
    changes_detected := true; -- Simplified: always check (handler filters)

    -- If changes detected, enqueue incremental sync job via PGMQ
    IF changes_detected THEN
      BEGIN
        -- Send job to PGMQ queue
        -- Note: PGMQ queue 'incremental-jobs' will be created automatically on first send
        -- PGMQ send signature: pgmq.send(queue_name, msg)
        SELECT pgmq.send(
          'incremental-jobs',
          jsonb_build_object(
            'pipeline_id', pipeline_record.id,
            'organization_id', pipeline_record.organization_id,
            'trigger_type', 'polling',
            'checkpoint', checkpoint_data,
            'watermark_field', watermark_field,
            'last_sync_value', last_sync_value,
            'created_at', now()
          )
        ) INTO job_id;

        -- Log the enqueue (optional)
        RAISE NOTICE 'Enqueued incremental sync job for pipeline % (job_id: %)', pipeline_record.id, job_id;
      EXCEPTION
        WHEN OTHERS THEN
          -- Log error but don't fail the entire polling cycle
          RAISE WARNING 'Failed to enqueue job for pipeline %: %', pipeline_record.id, SQLERRM;
      END;
    END IF;
  END LOOP;
END;
$$;

COMMENT ON FUNCTION pipeline_polling_function() IS 'Polling function for pg_cron that checks active pipelines and enqueues incremental sync jobs via PGMQ';

-- ============================================================================
-- STEP 4: Schedule pg_cron job (runs every 1 minute)
-- ============================================================================
-- Note: This assumes pg_cron extension is installed
-- If not installed, run: CREATE EXTENSION IF NOT EXISTS pg_cron;

DO $$
BEGIN
  -- Remove existing job if it exists
  PERFORM cron.unschedule('pipeline-polling-job');

  -- Schedule new job to run every 1 minute
  PERFORM cron.schedule(
    job_name := 'pipeline-polling-job',
    schedule := '*/1 * * * *', -- Every 1 minute
    command := 'SELECT pipeline_polling_function();'
  );

  RAISE NOTICE 'Scheduled pg_cron job: pipeline-polling-job (runs every 1 minute)';
EXCEPTION
  WHEN OTHERS THEN
    RAISE WARNING 'Failed to schedule pg_cron job. Ensure pg_cron extension is installed: %', SQLERRM;
END $$;

-- ============================================================================
-- STEP 5: Create NOTIFY triggers for real-time updates
-- ============================================================================

-- Function to notify on pipeline status/checkpoint changes
CREATE OR REPLACE FUNCTION notify_pipeline_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  payload jsonb;
BEGIN
  -- Build notification payload
  payload := jsonb_build_object(
    'pipeline_id', NEW.id,
    'organization_id', NEW.organization_id,
    'status', NEW.status,
    'last_run_status', NEW.last_run_status,
    'last_run_at', NEW.last_run_at,
    'total_rows_processed', NEW.total_rows_processed,
    'last_sync_at', NEW.last_sync_at,
    'checkpoint', NEW.checkpoint,
    'updated_at', NEW.updated_at
  );

  -- Send NOTIFY
  PERFORM pg_notify(
    'pipeline_updates',
    payload::text
  );

  RETURN NEW;
END;
$$;

-- Create trigger on pipelines table
DROP TRIGGER IF EXISTS pipeline_update_notify ON pipelines;
CREATE TRIGGER pipeline_update_notify
  AFTER UPDATE OF status, last_run_status, last_run_at, total_rows_processed, last_sync_at, checkpoint, pause_timestamp
  ON pipelines
  FOR EACH ROW
  WHEN (OLD.status IS DISTINCT FROM NEW.status 
     OR OLD.last_run_status IS DISTINCT FROM NEW.last_run_status
     OR OLD.total_rows_processed IS DISTINCT FROM NEW.total_rows_processed
     OR OLD.checkpoint IS DISTINCT FROM NEW.checkpoint)
  EXECUTE FUNCTION notify_pipeline_update();

COMMENT ON FUNCTION notify_pipeline_update() IS 'Sends NOTIFY on pipeline status/checkpoint changes for real-time updates via Socket.io';

-- Function to notify on pipeline run changes
CREATE OR REPLACE FUNCTION notify_pipeline_run_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  payload jsonb;
BEGIN
  -- Build notification payload
  payload := jsonb_build_object(
    'run_id', NEW.id,
    'pipeline_id', NEW.pipeline_id,
    'organization_id', NEW.organization_id,
    'status', NEW.status,
    'rows_read', NEW.rows_read,
    'rows_written', NEW.rows_written,
    'rows_skipped', NEW.rows_skipped,
    'rows_failed', NEW.rows_failed,
    'duration_seconds', NEW.duration_seconds,
    'error_message', NEW.error_message,
    'updated_at', NEW.updated_at
  );

  -- Send NOTIFY
  PERFORM pg_notify(
    'pipeline_run_updates',
    payload::text
  );

  RETURN NEW;
END;
$$;

-- Create trigger on pipeline_runs table
DROP TRIGGER IF EXISTS pipeline_run_update_notify ON pipeline_runs;
CREATE TRIGGER pipeline_run_update_notify
  AFTER UPDATE OF status, rows_read, rows_written, rows_skipped, rows_failed, duration_seconds, error_message
  ON pipeline_runs
  FOR EACH ROW
  WHEN (OLD.status IS DISTINCT FROM NEW.status 
     OR OLD.rows_read IS DISTINCT FROM NEW.rows_read
     OR OLD.rows_written IS DISTINCT FROM NEW.rows_written)
  EXECUTE FUNCTION notify_pipeline_run_update();

COMMENT ON FUNCTION notify_pipeline_run_update() IS 'Sends NOTIFY on pipeline run progress updates for real-time updates via Socket.io';

-- ============================================================================
-- STEP 6: Create indexes for performance
-- ============================================================================

-- Index for polling queries (LISTING status pipelines)
CREATE INDEX IF NOT EXISTS idx_pipelines_listing_polling 
ON pipelines (organization_id, status, sync_mode, incremental_column, deleted_at)
WHERE status = 'listing' AND sync_mode = 'incremental' AND deleted_at IS NULL;

-- Index for pause_timestamp queries
CREATE INDEX IF NOT EXISTS idx_pipelines_pause_timestamp 
ON pipelines (pause_timestamp)
WHERE pause_timestamp IS NOT NULL;

COMMIT;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Verify pause_timestamp column
-- SELECT column_name, data_type FROM information_schema.columns 
-- WHERE table_name = 'pipelines' AND column_name = 'pause_timestamp';

-- Verify polling function exists
-- SELECT proname FROM pg_proc WHERE proname = 'pipeline_polling_function';

-- Verify pg_cron job is scheduled
-- SELECT * FROM cron.job WHERE jobname = 'pipeline-polling-job';

-- Verify triggers exist
-- SELECT trigger_name FROM information_schema.triggers 
-- WHERE event_object_table = 'pipelines' AND trigger_name LIKE '%notify%';
