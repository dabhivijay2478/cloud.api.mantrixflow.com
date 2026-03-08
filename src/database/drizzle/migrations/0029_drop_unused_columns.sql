-- Drop unused columns (verified: no NestJS, FE, or ETL usage)

-- pipeline_destination_schemas
ALTER TABLE pipeline_destination_schemas DROP COLUMN IF EXISTS column_definitions;
ALTER TABLE pipeline_destination_schemas DROP COLUMN IF EXISTS primary_keys;
ALTER TABLE pipeline_destination_schemas DROP COLUMN IF EXISTS indexes;

-- pipeline_source_schemas
ALTER TABLE pipeline_source_schemas DROP COLUMN IF EXISTS foreign_keys;
ALTER TABLE pipeline_source_schemas DROP COLUMN IF EXISTS size_mb;

-- data_source_connections
ALTER TABLE data_source_connections DROP COLUMN IF EXISTS schema_cache;
ALTER TABLE data_source_connections DROP COLUMN IF EXISTS schema_cached_at;
