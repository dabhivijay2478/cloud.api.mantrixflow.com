-- Defensive migration: ensure all data_source_connections columns exist
-- Idempotent (ADD COLUMN IF NOT EXISTS) for DBs where 0031/0033 were partially applied or skipped

ALTER TABLE "data_source_connections" ADD COLUMN IF NOT EXISTS "collection_method" varchar(50) DEFAULT 'full_refresh';
ALTER TABLE "data_source_connections" ADD COLUMN IF NOT EXISTS "replication_slot_name" varchar(63);
ALTER TABLE "data_source_connections" ADD COLUMN IF NOT EXISTS "cdc_slot_health" jsonb;
ALTER TABLE "data_source_connections" ADD COLUMN IF NOT EXISTS "schema_evolution_log" jsonb;
ALTER TABLE "data_source_connections" ADD COLUMN IF NOT EXISTS "cdc_prerequisites_status" jsonb;
ALTER TABLE "data_source_connections" ADD COLUMN IF NOT EXISTS "publication_name" varchar(255);
