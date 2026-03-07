-- Drop unused columns from pipeline_runs and pipelines tables.
-- These columns were part of the old dlt-based ETL and are no longer
-- read or written by any service after the Singer migration.

ALTER TABLE "pipeline_runs" DROP COLUMN IF EXISTS "resolved_destination_schema";
ALTER TABLE "pipeline_runs" DROP COLUMN IF EXISTS "resolved_destination_table";
ALTER TABLE "pipeline_runs" DROP COLUMN IF EXISTS "resolved_column_mappings";
ALTER TABLE "pipeline_runs" DROP COLUMN IF EXISTS "destination_table_was_created";
ALTER TABLE "pipeline_runs" DROP COLUMN IF EXISTS "last_sync_cursor";
ALTER TABLE "pipelines" DROP COLUMN IF EXISTS "polling_config";
