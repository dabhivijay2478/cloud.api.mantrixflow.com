-- Migration: Add transform_script column to pipeline_destination_schemas
-- Adds support for custom Python transform scripts

-- Add transform_script column if it doesn't exist
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 
    FROM information_schema.columns 
    WHERE table_name = 'pipeline_destination_schemas' 
    AND column_name = 'transform_script'
  ) THEN
    ALTER TABLE pipeline_destination_schemas 
    ADD COLUMN transform_script text;
    
    -- Add comment
    COMMENT ON COLUMN pipeline_destination_schemas.transform_script IS 
      'Custom Python transform script that defines transform(record) function. Preferred over column_mappings for complex transformations.';
  END IF;
END $$;
