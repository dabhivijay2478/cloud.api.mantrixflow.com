-- Migration: Allow backend to INSERT and UPDATE etl_jobs
-- Backend (auth.uid() IS NULL) needs to insert new jobs and update status.

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'auth') THEN
    DROP POLICY IF EXISTS etl_jobs_backend_insert ON etl_jobs;
    CREATE POLICY etl_jobs_backend_insert ON etl_jobs
      FOR INSERT
      WITH CHECK (auth.uid() IS NULL);

    DROP POLICY IF EXISTS etl_jobs_backend_update ON etl_jobs;
    CREATE POLICY etl_jobs_backend_update ON etl_jobs
      FOR UPDATE
      USING (auth.uid() IS NULL)
      WITH CHECK (auth.uid() IS NULL);
  END IF;
END $$;
