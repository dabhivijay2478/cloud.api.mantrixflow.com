-- Migration: Remove Duplicate Schema Columns
-- This migration removes the source_schema and destination_schema columns
-- from postgres_pipelines table since they are now in separate schema tables

-- Drop the old columns that are no longer needed
-- These were moved to pipeline_source_schemas and pipeline_destination_schemas tables

ALTER TABLE "postgres_pipelines" DROP COLUMN IF EXISTS "source_schema";
ALTER TABLE "postgres_pipelines" DROP COLUMN IF EXISTS "destination_schema";

-- Note: The source_schema_id and destination_schema_id columns were already added
-- in migration 0002_separate_source_destination_schemas.sql
-- This migration just cleans up the old duplicate columns

