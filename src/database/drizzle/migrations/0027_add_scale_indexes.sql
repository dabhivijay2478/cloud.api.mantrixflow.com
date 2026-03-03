-- Scale indexes for 1M pipelines, 2k runs/day, 1000 users
-- Enables efficient list-by-org, recent runs, and Supabase Realtime filtering

-- pipelines: list by organization (paginated queries)
CREATE INDEX IF NOT EXISTS idx_pipelines_organization_id_created_at
  ON pipelines (organization_id, created_at DESC)
  WHERE deleted_at IS NULL;

-- pipeline_runs: recent runs by organization (Realtime, list views)
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_organization_id_created_at
  ON pipeline_runs (organization_id, created_at DESC);

-- pipeline_runs: runs by pipeline (detail page, run history)
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline_id_created_at
  ON pipeline_runs (pipeline_id, created_at DESC);
