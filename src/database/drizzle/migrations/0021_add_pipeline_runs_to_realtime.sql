-- Add pipeline_runs and pipelines to Supabase Realtime publication for live run updates.
-- Required for usePipelineRealtime hook and PipelineUpdatesGateway to receive postgres_changes.
-- Safe to run: adds tables only if publication exists and tables not already present.
-- Note: supabase_realtime is created by Supabase; for self-hosted Postgres, create it manually first.

DO $$
BEGIN
  -- Only proceed if supabase_realtime publication exists
  IF EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'supabase_realtime') THEN
    -- Add pipelines if not already in publication
    IF NOT EXISTS (
      SELECT 1 FROM pg_publication_tables
      WHERE pubname = 'supabase_realtime' AND schemaname = 'public' AND tablename = 'pipelines'
    ) THEN
      ALTER PUBLICATION supabase_realtime ADD TABLE public.pipelines;
    END IF;

    -- Add pipeline_runs if not already in publication
    IF NOT EXISTS (
      SELECT 1 FROM pg_publication_tables
      WHERE pubname = 'supabase_realtime' AND schemaname = 'public' AND tablename = 'pipeline_runs'
    ) THEN
      ALTER PUBLICATION supabase_realtime ADD TABLE public.pipeline_runs;
    END IF;
  END IF;
END
$$;
