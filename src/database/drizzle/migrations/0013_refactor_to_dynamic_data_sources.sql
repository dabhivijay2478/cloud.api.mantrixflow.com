-- Migration: Refactor to Dynamic Data Sources Architecture
-- This migration refactors the database from static PostgreSQL-only connections
-- to a flexible, multi-source data source architecture.
--
-- Changes:
-- 1. Create new tables: data_sources, data_source_connections
-- 2. Migrate postgres_connections to new structure
-- 3. Update organizations table (make owner_user_id NOT NULL)
-- 4. Update pipeline_source_schemas (replace source_connection_id with data_source_id)
-- 5. Update pipeline_destination_schemas (replace destination_connection_id with data_source_id)
-- 6. Rename postgres_pipelines to pipelines
-- 7. Rename postgres_pipeline_runs to pipeline_runs
-- 8. Rename postgres_query_logs to query_logs
-- 9. Drop old tables: organization_owners, postgres_connections, postgres_sync_jobs

BEGIN;

-- ============================================================================
-- STEP 1: Create new enums
-- ============================================================================

-- Connection status enum (if not exists)
DO $$ BEGIN
  CREATE TYPE connection_status_new AS ENUM ('active', 'inactive', 'error', 'testing');
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;

-- ============================================================================
-- STEP 2: Create new tables
-- ============================================================================

-- Create data_sources table
CREATE TABLE IF NOT EXISTS "data_sources" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
  "organization_id" uuid NOT NULL,
  "name" varchar(255) NOT NULL,
  "description" text,
  "source_type" varchar(100) NOT NULL,
  "is_active" boolean DEFAULT true NOT NULL,
  "metadata" jsonb,
  "created_by" uuid NOT NULL,
  "created_at" timestamp DEFAULT now() NOT NULL,
  "updated_at" timestamp DEFAULT now() NOT NULL,
  "deleted_at" timestamp,
  CONSTRAINT "data_sources_organization_id_organizations_id_fk" 
    FOREIGN KEY ("organization_id") REFERENCES "organizations"("id") ON DELETE CASCADE,
  CONSTRAINT "data_sources_created_by_users_id_fk" 
    FOREIGN KEY ("created_by") REFERENCES "users"("id") ON DELETE RESTRICT
);

-- Create indexes for data_sources
CREATE INDEX IF NOT EXISTS "data_sources_organization_id_idx" ON "data_sources"("organization_id");
CREATE INDEX IF NOT EXISTS "data_sources_source_type_idx" ON "data_sources"("source_type");
CREATE INDEX IF NOT EXISTS "data_sources_is_active_idx" ON "data_sources"("is_active") WHERE "deleted_at" IS NULL;

-- Create data_source_connections table
CREATE TABLE IF NOT EXISTS "data_source_connections" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
  "data_source_id" uuid NOT NULL,
  "connection_type" varchar(100) NOT NULL,
  "config" jsonb NOT NULL,
  "status" connection_status_new DEFAULT 'inactive' NOT NULL,
  "last_connected_at" timestamp,
  "last_error" text,
  "test_result" jsonb,
  "schema_cache" jsonb,
  "schema_cached_at" timestamp,
  "created_at" timestamp DEFAULT now() NOT NULL,
  "updated_at" timestamp DEFAULT now() NOT NULL,
  CONSTRAINT "data_source_connections_data_source_id_data_sources_id_fk" 
    FOREIGN KEY ("data_source_id") REFERENCES "data_sources"("id") ON DELETE CASCADE,
  CONSTRAINT "data_source_connections_data_source_id_unique" UNIQUE("data_source_id")
);

-- Create indexes for data_source_connections
CREATE INDEX IF NOT EXISTS "data_source_connections_data_source_id_idx" ON "data_source_connections"("data_source_id");
CREATE INDEX IF NOT EXISTS "data_source_connections_connection_type_idx" ON "data_source_connections"("connection_type");
CREATE INDEX IF NOT EXISTS "data_source_connections_status_idx" ON "data_source_connections"("status");

-- ============================================================================
-- STEP 3: Migrate data from postgres_connections to new structure
-- ============================================================================

-- Migrate postgres_connections to data_sources and data_source_connections
INSERT INTO "data_sources" (
  "id",
  "organization_id",
  "name",
  "description",
  "source_type",
  "is_active",
  "metadata",
  "created_by",
  "created_at",
  "updated_at"
)
SELECT 
  gen_random_uuid(), -- Generate new ID
  "org_id",
  "name",
  NULL, -- No description in old table
  'postgres',
  CASE WHEN "status" = 'active' THEN true ELSE false END,
  NULL,
  "user_id",
  "created_at",
  "updated_at"
FROM "postgres_connections"
ON CONFLICT DO NOTHING;

-- Create mapping table for old connection IDs to new data source IDs
CREATE TEMP TABLE connection_mapping AS
SELECT 
  pc.id AS old_connection_id,
  ds.id AS new_data_source_id
FROM "postgres_connections" pc
JOIN "data_sources" ds ON 
  ds.organization_id = pc.org_id 
  AND ds.name = pc.name 
  AND ds.source_type = 'postgres'
  AND ds.created_by = pc.user_id;

-- Migrate connection details to data_source_connections
INSERT INTO "data_source_connections" (
  "data_source_id",
  "connection_type",
  "config",
  "status",
  "last_connected_at",
  "last_error",
  "schema_cache",
  "schema_cached_at",
  "created_at",
  "updated_at"
)
SELECT 
  cm.new_data_source_id,
  'postgres',
  jsonb_build_object(
    'host', pc.host,
    'port', pc.port,
    'database', pc.database,
    'username', pc.username,
    'password', pc.password,
    'ssl', jsonb_build_object(
      'enabled', pc.ssl_enabled,
      'ca_cert', pc.ssl_ca_cert
    ),
    'ssh_tunnel', jsonb_build_object(
      'enabled', pc.ssh_tunnel_enabled,
      'host', pc.ssh_host,
      'port', pc.ssh_port,
      'username', pc.ssh_username,
      'private_key', pc.ssh_private_key
    ),
    'pool', jsonb_build_object(
      'size', pc.connection_pool_size,
      'timeout_seconds', pc.query_timeout_seconds
    )
  ),
  CASE 
    WHEN pc.status = 'active' THEN 'active'::connection_status_new
    WHEN pc.status = 'inactive' THEN 'inactive'::connection_status_new
    ELSE 'error'::connection_status_new
  END,
  pc.last_connected_at,
  pc.last_error,
  pc.schema_cache,
  pc.schema_cached_at,
  pc.created_at,
  pc.updated_at
FROM "postgres_connections" pc
JOIN connection_mapping cm ON pc.id = cm.old_connection_id;

-- ============================================================================
-- STEP 4: Update organizations table
-- ============================================================================

-- Ensure all organizations have owner_user_id populated
-- If owner_user_id is NULL, try to get it from organization_owners table
UPDATE "organizations" o
SET "owner_user_id" = (
  SELECT oo.user_id 
  FROM "organization_owners" oo 
  WHERE oo.organization_id = o.id 
  LIMIT 1
)
WHERE o.owner_user_id IS NULL;

-- If still NULL, use the first organization member with OWNER role
UPDATE "organizations" o
SET "owner_user_id" = (
  SELECT om.user_id 
  FROM "organization_members" om 
  WHERE om.organization_id = o.id 
    AND om.role = 'OWNER'
  LIMIT 1
)
WHERE o.owner_user_id IS NULL;

-- Make owner_user_id NOT NULL
ALTER TABLE "organizations" 
  ALTER COLUMN "owner_user_id" SET NOT NULL,
  ALTER COLUMN "owner_user_id" DROP DEFAULT;

-- Update foreign key constraint
ALTER TABLE "organizations"
  DROP CONSTRAINT IF EXISTS "organizations_owner_user_id_users_id_fk",
  ADD CONSTRAINT "organizations_owner_user_id_users_id_fk" 
    FOREIGN KEY ("owner_user_id") REFERENCES "users"("id") ON DELETE RESTRICT;

-- ============================================================================
-- STEP 5: Update pipeline_source_schemas table
-- ============================================================================

-- Add new columns
ALTER TABLE "pipeline_source_schemas"
  ADD COLUMN IF NOT EXISTS "data_source_id" uuid,
  ADD COLUMN IF NOT EXISTS "organization_id" uuid;

-- Migrate data: map old source_connection_id to new data_source_id
UPDATE "pipeline_source_schemas" pss
SET 
  "data_source_id" = cm.new_data_source_id,
  "organization_id" = pss.org_id
FROM connection_mapping cm
WHERE pss.source_connection_id = cm.old_connection_id;

-- Make organization_id NOT NULL and add foreign key
UPDATE "pipeline_source_schemas"
SET "organization_id" = "org_id"
WHERE "organization_id" IS NULL;

ALTER TABLE "pipeline_source_schemas"
  ALTER COLUMN "organization_id" SET NOT NULL,
  ADD CONSTRAINT "pipeline_source_schemas_organization_id_organizations_id_fk"
    FOREIGN KEY ("organization_id") REFERENCES "organizations"("id") ON DELETE CASCADE;

-- Add foreign key for data_source_id
ALTER TABLE "pipeline_source_schemas"
  ADD CONSTRAINT "pipeline_source_schemas_data_source_id_data_sources_id_fk"
    FOREIGN KEY ("data_source_id") REFERENCES "data_sources"("id") ON DELETE CASCADE;

-- Drop old columns
ALTER TABLE "pipeline_source_schemas"
  DROP COLUMN IF EXISTS "source_connection_id",
  DROP COLUMN IF EXISTS "org_id",
  DROP COLUMN IF EXISTS "user_id";

-- ============================================================================
-- STEP 6: Update pipeline_destination_schemas table
-- ============================================================================

-- Add new columns
ALTER TABLE "pipeline_destination_schemas"
  ADD COLUMN IF NOT EXISTS "data_source_id" uuid,
  ADD COLUMN IF NOT EXISTS "organization_id" uuid;

-- Migrate data: map old destination_connection_id to new data_source_id
UPDATE "pipeline_destination_schemas" pds
SET 
  "data_source_id" = cm.new_data_source_id,
  "organization_id" = pds.org_id
FROM connection_mapping cm
WHERE pds.destination_connection_id = cm.old_connection_id;

-- Make organization_id NOT NULL and add foreign key
UPDATE "pipeline_destination_schemas"
SET "organization_id" = "org_id"
WHERE "organization_id" IS NULL;

ALTER TABLE "pipeline_destination_schemas"
  ALTER COLUMN "organization_id" SET NOT NULL,
  ALTER COLUMN "data_source_id" SET NOT NULL,
  ADD CONSTRAINT "pipeline_destination_schemas_organization_id_organizations_id_fk"
    FOREIGN KEY ("organization_id") REFERENCES "organizations"("id") ON DELETE CASCADE,
  ADD CONSTRAINT "pipeline_destination_schemas_data_source_id_data_sources_id_fk"
    FOREIGN KEY ("data_source_id") REFERENCES "data_sources"("id") ON DELETE CASCADE;

-- Drop old columns
ALTER TABLE "pipeline_destination_schemas"
  DROP COLUMN IF EXISTS "destination_connection_id",
  DROP COLUMN IF EXISTS "org_id",
  DROP COLUMN IF EXISTS "user_id";

-- ============================================================================
-- STEP 7: Rename postgres_pipelines to pipelines
-- ============================================================================

-- Rename table
ALTER TABLE "postgres_pipelines" RENAME TO "pipelines";

-- Rename columns
ALTER TABLE "pipelines"
  RENAME COLUMN "org_id" TO "organization_id",
  RENAME COLUMN "user_id" TO "created_by";

-- Update foreign key constraints
ALTER TABLE "pipelines"
  DROP CONSTRAINT IF EXISTS "postgres_pipelines_org_id_organizations_id_fk",
  ADD CONSTRAINT "pipelines_organization_id_organizations_id_fk"
    FOREIGN KEY ("organization_id") REFERENCES "organizations"("id") ON DELETE CASCADE;

ALTER TABLE "pipelines"
  DROP CONSTRAINT IF EXISTS "postgres_pipelines_user_id_users_id_fk",
  ADD CONSTRAINT "pipelines_created_by_users_id_fk"
    FOREIGN KEY ("created_by") REFERENCES "users"("id") ON DELETE RESTRICT;

-- Update references to source and destination schemas
ALTER TABLE "pipelines"
  DROP CONSTRAINT IF EXISTS "postgres_pipelines_source_schema_id_pipeline_source_schemas_id_fk",
  DROP CONSTRAINT IF EXISTS "postgres_pipelines_destination_schema_id_pipeline_destination_schemas_id_fk";

-- Add new foreign keys if source_schema_id and destination_schema_id exist
DO $$ 
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns 
              WHERE table_name = 'pipelines' AND column_name = 'source_schema_id') THEN
    ALTER TABLE "pipelines"
      ADD CONSTRAINT "pipelines_source_schema_id_pipeline_source_schemas_id_fk"
        FOREIGN KEY ("source_schema_id") REFERENCES "pipeline_source_schemas"("id") ON DELETE RESTRICT;
  END IF;
  
  IF EXISTS (SELECT 1 FROM information_schema.columns 
              WHERE table_name = 'pipelines' AND column_name = 'destination_schema_id') THEN
    ALTER TABLE "pipelines"
      ADD CONSTRAINT "pipelines_destination_schema_id_pipeline_destination_schemas_id_fk"
        FOREIGN KEY ("destination_schema_id") REFERENCES "pipeline_destination_schemas"("id") ON DELETE RESTRICT;
  END IF;
END $$;

-- ============================================================================
-- STEP 8: Rename postgres_pipeline_runs to pipeline_runs
-- ============================================================================

-- Rename table
ALTER TABLE "postgres_pipeline_runs" RENAME TO "pipeline_runs";

-- Rename columns
ALTER TABLE "pipeline_runs"
  RENAME COLUMN "org_id" TO "organization_id";

-- Update foreign key constraints
ALTER TABLE "pipeline_runs"
  DROP CONSTRAINT IF EXISTS "postgres_pipeline_runs_pipeline_id_postgres_pipelines_id_fk",
  ADD CONSTRAINT "pipeline_runs_pipeline_id_pipelines_id_fk"
    FOREIGN KEY ("pipeline_id") REFERENCES "pipelines"("id") ON DELETE CASCADE;

ALTER TABLE "pipeline_runs"
  DROP CONSTRAINT IF EXISTS "postgres_pipeline_runs_org_id_organizations_id_fk",
  ADD CONSTRAINT "pipeline_runs_organization_id_organizations_id_fk"
    FOREIGN KEY ("organization_id") REFERENCES "organizations"("id") ON DELETE CASCADE;

-- Add job_state column if it doesn't exist
ALTER TABLE "pipeline_runs"
  ADD COLUMN IF NOT EXISTS "job_state" varchar(50) DEFAULT 'pending';

-- Create job_state enum if needed
DO $$ BEGIN
  CREATE TYPE job_state AS ENUM ('pending', 'queued', 'running', 'completed', 'failed');
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;

-- Update job_state column type if enum exists
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'job_state') THEN
    ALTER TABLE "pipeline_runs" 
      ALTER COLUMN "job_state" TYPE job_state USING job_state::job_state;
  END IF;
END $$;

-- ============================================================================
-- STEP 9: Rename postgres_query_logs to query_logs
-- ============================================================================

-- Rename table
ALTER TABLE "postgres_query_logs" RENAME TO "query_logs";

-- Add data_source_id column
ALTER TABLE "query_logs"
  ADD COLUMN IF NOT EXISTS "data_source_id" uuid;

-- Migrate data: map old connection_id to new data_source_id
UPDATE "query_logs" ql
SET "data_source_id" = cm.new_data_source_id
FROM connection_mapping cm
WHERE ql.connection_id = cm.old_connection_id;

-- Make data_source_id NOT NULL and add foreign key
ALTER TABLE "query_logs"
  ALTER COLUMN "data_source_id" SET NOT NULL,
  ADD CONSTRAINT "query_logs_data_source_id_data_sources_id_fk"
    FOREIGN KEY ("data_source_id") REFERENCES "data_sources"("id") ON DELETE CASCADE;

-- Drop old connection_id column
ALTER TABLE "query_logs"
  DROP COLUMN IF EXISTS "connection_id";

-- ============================================================================
-- STEP 10: Drop old tables
-- ============================================================================

-- Drop organization_owners table (ownership is now in organizations.owner_user_id)
DROP TABLE IF EXISTS "organization_owners" CASCADE;

-- Drop postgres_sync_jobs table (consolidated into pipelines)
DROP TABLE IF EXISTS "postgres_sync_jobs" CASCADE;

-- Drop postgres_connections table (migrated to data_sources + data_source_connections)
-- Note: Keep this commented out initially for safety, uncomment after verification
-- DROP TABLE IF EXISTS "postgres_connections" CASCADE;

-- Drop temporary mapping table
DROP TABLE IF EXISTS connection_mapping;

COMMIT;

-- ============================================================================
-- Post-migration verification queries
-- ============================================================================

-- Verify data migration
-- SELECT COUNT(*) as data_sources_count FROM data_sources;
-- SELECT COUNT(*) as connections_count FROM data_source_connections;
-- SELECT COUNT(*) as old_connections_count FROM postgres_connections;

-- Verify pipeline updates
-- SELECT COUNT(*) as pipelines_count FROM pipelines;
-- SELECT COUNT(*) as pipeline_runs_count FROM pipeline_runs;
-- SELECT COUNT(*) as query_logs_count FROM query_logs;
