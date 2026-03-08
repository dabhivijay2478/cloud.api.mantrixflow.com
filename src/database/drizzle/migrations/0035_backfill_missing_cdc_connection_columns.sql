-- Backfill control-plane CDC metadata columns that were added in untracked
-- migrations. These columns belong to the app metadata database only; they do
-- not modify any client source databases.

ALTER TABLE "data_source_connections"
  ADD COLUMN IF NOT EXISTS "cdc_prerequisites_status" jsonb;

ALTER TABLE "data_source_connections"
  ADD COLUMN IF NOT EXISTS "publication_name" varchar(255);
