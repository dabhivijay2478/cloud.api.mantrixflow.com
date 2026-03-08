-- Add custom_sql for user-defined SQL transforms from the FE
-- When provided, runs SQL against raw_input instead of dbt model files

ALTER TABLE "pipeline_destination_schemas"
  ADD COLUMN IF NOT EXISTS "custom_sql" text;

COMMENT ON COLUMN "pipeline_destination_schemas"."custom_sql" IS
  'Custom SQL from FE - runs against raw_input when transformType is dbt';
