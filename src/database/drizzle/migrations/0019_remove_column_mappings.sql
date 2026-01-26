-- Migration: Remove column_mappings column from pipeline_destination_schemas
-- Column mappings are replaced by transform_script for all transformations

-- Drop column_mappings column if it exists
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'pipeline_destination_schemas'
    AND column_name = 'column_mappings'
  ) THEN
    ALTER TABLE pipeline_destination_schemas
    DROP COLUMN column_mappings;
  END IF;
END $$;
