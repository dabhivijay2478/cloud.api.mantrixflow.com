-- Migration: Add dbt_models column to pipeline_destination_schemas
-- Stores selected dbt model names for pipeline execution (empty/null = run all)

ALTER TABLE pipeline_destination_schemas
ADD COLUMN IF NOT EXISTS dbt_models jsonb;

COMMENT ON COLUMN pipeline_destination_schemas.dbt_models IS 'Selected dbt model names; empty or null means run all models';
