-- Migration: Re-add transform_script column to pipeline_destination_schemas
-- 0026 dropped it; Script mode requires it for Python row-level transforms

ALTER TABLE "pipeline_destination_schemas"
  ADD COLUMN IF NOT EXISTS "transform_script" text;

COMMENT ON COLUMN "pipeline_destination_schemas"."transform_script" IS
  'Python transform script - defines def transform(row) -> dict. Used when transformType is script.';
