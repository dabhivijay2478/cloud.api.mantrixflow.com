-- Singer ETL Migration
-- Adds columns for Singer-based tap-postgres / target-postgres architecture.

-- pipelines: Singer state, replication slot, collection method overrides
ALTER TABLE "pipelines" ADD COLUMN IF NOT EXISTS "singer_state" jsonb;
ALTER TABLE "pipelines" ADD COLUMN IF NOT EXISTS "full_refresh_completed_at" timestamptz;
ALTER TABLE "pipelines" ADD COLUMN IF NOT EXISTS "replication_slot_name" varchar(63);
ALTER TABLE "pipelines" ADD COLUMN IF NOT EXISTS "collection_method_override" text;
ALTER TABLE "pipelines" ADD COLUMN IF NOT EXISTS "emit_method" text;
ALTER TABLE "pipelines" ADD COLUMN IF NOT EXISTS "dest_schema_override" text;
ALTER TABLE "pipelines" ADD COLUMN IF NOT EXISTS "transform_type" text;

-- pipeline_runs: Singer run metadata
ALTER TABLE "pipeline_runs" ADD COLUMN IF NOT EXISTS "collection_method_used" text;
ALTER TABLE "pipeline_runs" ADD COLUMN IF NOT EXISTS "emit_method_used" text;
ALTER TABLE "pipeline_runs" ADD COLUMN IF NOT EXISTS "rows_deleted" integer DEFAULT 0;
ALTER TABLE "pipeline_runs" ADD COLUMN IF NOT EXISTS "lsn_start" bigint;
ALTER TABLE "pipeline_runs" ADD COLUMN IF NOT EXISTS "lsn_end" bigint;
ALTER TABLE "pipeline_runs" ADD COLUMN IF NOT EXISTS "source_tool" text DEFAULT 'tap-postgres';
ALTER TABLE "pipeline_runs" ADD COLUMN IF NOT EXISTS "dest_tool" text DEFAULT 'target-postgres';

-- data_source_connections: CDC slot tracking
ALTER TABLE "data_source_connections" ADD COLUMN IF NOT EXISTS "collection_method" varchar(50) DEFAULT 'full_refresh';
ALTER TABLE "data_source_connections" ADD COLUMN IF NOT EXISTS "replication_slot_name" varchar(63);
ALTER TABLE "data_source_connections" ADD COLUMN IF NOT EXISTS "cdc_slot_health" jsonb;
ALTER TABLE "data_source_connections" ADD COLUMN IF NOT EXISTS "schema_evolution_log" jsonb;
