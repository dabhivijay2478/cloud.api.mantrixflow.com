-- Add field_mappings for column-map based transformations
-- When transform_type is 'field_mappings', use this instead of dbt_model

ALTER TABLE "pipeline_destination_schemas"
  ADD COLUMN IF NOT EXISTS "field_mappings" jsonb;

COMMENT ON COLUMN "pipeline_destination_schemas"."field_mappings" IS
  'Column mappings [{source, destination}] when transform_type is field_mappings';
