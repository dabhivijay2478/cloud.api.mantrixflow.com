-- Migration: Allow backend (direct connection) to read etl_jobs
-- When NestJS connects via DATABASE_URL (no JWT), auth.uid() is NULL.
-- The existing policy blocks reads. This policy allows backend service reads.
-- Supabase only: auth schema must exist.

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'auth') THEN
    DROP POLICY IF EXISTS etl_jobs_backend_read ON etl_jobs;
    CREATE POLICY etl_jobs_backend_read ON etl_jobs
      FOR SELECT
      USING (auth.uid() IS NULL);
  END IF;
END $$;
