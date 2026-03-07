-- CDC Setup Guide: Add cdc_prerequisites_status and publication_name to data_source_connections
-- cdc_prerequisites_status: JSONB with overall, checked_at, wal_level_ok, wal2json_ok, replication_role_ok, replication_test_ok, provider_selected, last_error
-- publication_name: Reserved for future use

ALTER TABLE "data_source_connections" ADD COLUMN IF NOT EXISTS "cdc_prerequisites_status" jsonb;
ALTER TABLE "data_source_connections" ADD COLUMN IF NOT EXISTS "publication_name" varchar(255);
