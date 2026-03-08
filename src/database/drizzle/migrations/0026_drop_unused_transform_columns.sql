-- Drop unused columns (field mappings and Python script removed; only dbt customSql used)
ALTER TABLE "pipeline_destination_schemas" DROP COLUMN IF EXISTS "field_mappings";
ALTER TABLE "pipeline_destination_schemas" DROP COLUMN IF EXISTS "transform_script";
