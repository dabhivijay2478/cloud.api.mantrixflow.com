-- Migration: Add etl_jobs table + pgmq queue + pg_cron for async ETL
-- MANTrixFlow: pgmq + pg_cron async ETL (NO Redis, NO BullMQ)

-- 1. Create etl_job_status enum
DO $$ BEGIN
  CREATE TYPE etl_job_status AS ENUM (
    'pending',
    'queued',
    'running',
    'completed',
    'failed'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

-- 2. Create etl_jobs table
CREATE TABLE IF NOT EXISTS etl_jobs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  pipeline_id uuid NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
  org_id uuid NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  status etl_job_status NOT NULL DEFAULT 'pending',
  sync_mode text,
  direction text,
  source_connection_id uuid REFERENCES data_source_connections(id) ON DELETE SET NULL,
  dest_connection_id uuid REFERENCES data_source_connections(id) ON DELETE SET NULL,
  state_id text,
  meltano_job_id text,
  pgmq_msg_id bigint,
  rows_synced integer DEFAULT 0,
  bytes_processed bigint DEFAULT 0,
  error_message text,
  user_message text,
  started_at timestamptz,
  completed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  payload jsonb
);

COMMENT ON TABLE etl_jobs IS 'Async ETL job records; status pushed via Supabase Realtime';

-- 3. Add to supabase_realtime publication (Supabase only; skip if not exists)
DO $$ BEGIN
  ALTER PUBLICATION supabase_realtime ADD TABLE etl_jobs;
EXCEPTION
  WHEN undefined_object THEN NULL;  -- publication doesn't exist
  WHEN duplicate_object THEN NULL; -- table already in publication
END $$;

-- 4. Indexes
CREATE INDEX IF NOT EXISTS idx_etl_jobs_pipeline_created ON etl_jobs(pipeline_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_etl_jobs_status ON etl_jobs(status) WHERE status IN ('pending', 'queued', 'running');

-- 5. RLS
ALTER TABLE etl_jobs ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS etl_jobs_org_select ON etl_jobs;
CREATE POLICY etl_jobs_org_select ON etl_jobs
  FOR SELECT
  USING (org_id IN (
    SELECT organization_id FROM organization_members
    WHERE user_id = auth.uid()
  ));

-- 6. Ensure pgmq extension
CREATE EXTENSION IF NOT EXISTS pgmq;

-- 7. Create pgmq queues (ignore if already exist)
DO $$ BEGIN
  PERFORM pgmq.create('etl_jobs');
EXCEPTION WHEN OTHERS THEN NULL;
END $$;
DO $$ BEGIN
  PERFORM pgmq.create('etl_jobs_dlq');
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- 8. pg_cron: call NestJS /internal/process-etl-jobs every minute
-- Requires pg_cron + pg_net. Configure via:
--   ALTER DATABASE postgres SET app.nestjs_internal_url = 'https://api.example.com';
--   ALTER DATABASE postgres SET app.internal_token = 'your-secret-token';
-- Then run manually: SELECT cron.schedule('process-etl-queue', '* * * * *', ...);
-- Or use NestJS @nestjs/schedule to poll every minute (no pg_net needed).

-- Rollback (commented out):
-- ALTER PUBLICATION supabase_realtime DROP TABLE etl_jobs;
-- DROP POLICY IF EXISTS etl_jobs_org_select ON etl_jobs;
-- DROP TABLE IF EXISTS etl_jobs;
-- DROP TYPE IF EXISTS etl_job_status;
-- SELECT cron.unschedule('process-etl-queue');
-- SELECT pgmq.drop_queue('etl_jobs');
-- SELECT pgmq.drop_queue('etl_jobs_dlq');
