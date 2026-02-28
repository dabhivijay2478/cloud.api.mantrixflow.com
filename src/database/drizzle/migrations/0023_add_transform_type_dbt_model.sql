-- Add transform_type and dbt_model columns for dbt-based transformations
-- transform_type: 'dbt' | 'rules' | 'none'
-- dbt_model: dbt model name when transformType is 'dbt'

ALTER TABLE "pipeline_destination_schemas"
  ADD COLUMN IF NOT EXISTS "transform_type" varchar(50) DEFAULT 'dbt';

ALTER TABLE "pipeline_destination_schemas"
  ADD COLUMN IF NOT EXISTS "dbt_model" varchar(255);

COMMENT ON COLUMN "pipeline_destination_schemas"."transform_type" IS
  'Transform type: dbt, rules, or none';
COMMENT ON COLUMN "pipeline_destination_schemas"."dbt_model" IS
  'dbt model name when transform_type is dbt (e.g. stg_company_role_combined)';
