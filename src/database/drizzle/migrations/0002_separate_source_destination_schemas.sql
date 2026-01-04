-- Migration: Separate Source and Destination Schemas
-- This migration creates separate tables for source and destination schemas
-- and updates the postgres_pipelines table to reference them

-- Step 1: Create pipeline_source_schemas table
CREATE TABLE IF NOT EXISTS "pipeline_source_schemas" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"org_id" uuid NOT NULL,
	"user_id" uuid NOT NULL,
	"source_type" varchar(100) NOT NULL,
	"source_connection_id" uuid,
	"source_config" jsonb,
	"source_schema" varchar(255),
	"source_table" varchar(255),
	"source_query" text,
	"discovered_columns" jsonb,
	"primary_keys" jsonb,
	"foreign_keys" jsonb,
	"estimated_row_count" jsonb,
	"size_mb" jsonb,
	"validation_result" jsonb,
	"name" varchar(255),
	"is_active" boolean DEFAULT true,
	"last_discovered_at" timestamp,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	"deleted_at" timestamp
);

-- Step 2: Create pipeline_destination_schemas table
CREATE TABLE IF NOT EXISTS "pipeline_destination_schemas" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"org_id" uuid NOT NULL,
	"user_id" uuid NOT NULL,
	"destination_connection_id" uuid NOT NULL,
	"destination_schema" varchar(255) DEFAULT 'public',
	"destination_table" varchar(255) NOT NULL,
	"destination_table_exists" boolean DEFAULT false,
	"column_definitions" jsonb,
	"primary_keys" jsonb,
	"indexes" jsonb,
	"column_mappings" jsonb,
	"write_mode" varchar(50) DEFAULT 'append',
	"upsert_key" jsonb,
	"validation_result" jsonb,
	"last_validated_at" timestamp,
	"last_synced_at" timestamp,
	"name" varchar(255),
	"is_active" boolean DEFAULT true,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	"deleted_at" timestamp
);

-- Step 3: Add foreign key constraints
ALTER TABLE "pipeline_source_schemas" ADD CONSTRAINT "pipeline_source_schemas_source_connection_id_postgres_connections_id_fk" 
	FOREIGN KEY ("source_connection_id") REFERENCES "public"."postgres_connections"("id") ON DELETE cascade ON UPDATE no action;

ALTER TABLE "pipeline_destination_schemas" ADD CONSTRAINT "pipeline_destination_schemas_destination_connection_id_postgres_connections_id_fk" 
	FOREIGN KEY ("destination_connection_id") REFERENCES "public"."postgres_connections"("id") ON DELETE cascade ON UPDATE no action;

-- Step 4: Add new columns to postgres_pipelines (nullable initially for data migration)
ALTER TABLE "postgres_pipelines" ADD COLUMN IF NOT EXISTS "source_schema_id" uuid;
ALTER TABLE "postgres_pipelines" ADD COLUMN IF NOT EXISTS "destination_schema_id" uuid;

-- Step 5: Migrate existing data from postgres_pipelines to new schema tables
-- This creates source and destination schema records from existing pipeline data
INSERT INTO "pipeline_source_schemas" (
	"org_id",
	"user_id",
	"source_type",
	"source_connection_id",
	"source_config",
	"source_schema",
	"source_table",
	"source_query",
	"name",
	"created_at",
	"updated_at"
)
SELECT DISTINCT
	"org_id",
	"user_id",
	"source_type",
	"source_connection_id",
	"source_config",
	"source_schema",
	"source_table",
	"source_query",
	'Migrated from pipeline: ' || "name" as "name",
	"created_at",
	"updated_at"
FROM "postgres_pipelines"
WHERE "source_type" IS NOT NULL
	AND "deleted_at" IS NULL
ON CONFLICT DO NOTHING;

INSERT INTO "pipeline_destination_schemas" (
	"org_id",
	"user_id",
	"destination_connection_id",
	"destination_schema",
	"destination_table",
	"destination_table_exists",
	"column_mappings",
	"write_mode",
	"upsert_key",
	"name",
	"created_at",
	"updated_at"
)
SELECT DISTINCT
	"org_id",
	"user_id",
	"destination_connection_id",
	COALESCE("destination_schema", 'public'),
	"destination_table",
	COALESCE("destination_table_exists", false),
	"column_mappings",
	COALESCE("write_mode"::text, 'append'),
	"upsert_key",
	'Migrated from pipeline: ' || "name" as "name",
	"created_at",
	"updated_at"
FROM "postgres_pipelines"
WHERE "destination_connection_id" IS NOT NULL
	AND "destination_table" IS NOT NULL
	AND "deleted_at" IS NULL
ON CONFLICT DO NOTHING;

-- Step 6: Update postgres_pipelines to reference the new schema tables
UPDATE "postgres_pipelines" p
SET "source_schema_id" = (
	SELECT s.id
	FROM "pipeline_source_schemas" s
	WHERE s."org_id" = p."org_id"
		AND s."source_type" = p."source_type"
		AND (s."source_connection_id" = p."source_connection_id" OR (s."source_connection_id" IS NULL AND p."source_connection_id" IS NULL))
		AND (s."source_schema" = p."source_schema" OR (s."source_schema" IS NULL AND p."source_schema" IS NULL))
		AND (s."source_table" = p."source_table" OR (s."source_table" IS NULL AND p."source_table" IS NULL))
		AND s."deleted_at" IS NULL
	LIMIT 1
)
WHERE p."source_schema_id" IS NULL
	AND p."deleted_at" IS NULL;

UPDATE "postgres_pipelines" p
SET "destination_schema_id" = (
	SELECT d.id
	FROM "pipeline_destination_schemas" d
	WHERE d."org_id" = p."org_id"
		AND d."destination_connection_id" = p."destination_connection_id"
		AND d."destination_schema" = COALESCE(p."destination_schema", 'public')
		AND d."destination_table" = p."destination_table"
		AND d."deleted_at" IS NULL
	LIMIT 1
)
WHERE p."destination_schema_id" IS NULL
	AND p."deleted_at" IS NULL;

-- Step 7: Make the new columns NOT NULL (after data migration)
ALTER TABLE "postgres_pipelines" ALTER COLUMN "source_schema_id" SET NOT NULL;
ALTER TABLE "postgres_pipelines" ALTER COLUMN "destination_schema_id" SET NOT NULL;

-- Step 8: Add foreign key constraints
ALTER TABLE "postgres_pipelines" ADD CONSTRAINT "postgres_pipelines_source_schema_id_pipeline_source_schemas_id_fk" 
	FOREIGN KEY ("source_schema_id") REFERENCES "public"."pipeline_source_schemas"("id") ON DELETE restrict ON UPDATE no action;

ALTER TABLE "postgres_pipelines" ADD CONSTRAINT "postgres_pipelines_destination_schema_id_pipeline_destination_schemas_id_fk" 
	FOREIGN KEY ("destination_schema_id") REFERENCES "public"."pipeline_destination_schemas"("id") ON DELETE restrict ON UPDATE no action;

-- Step 9: Drop old columns from postgres_pipelines (optional - can be done in a later migration)
-- Uncomment these if you want to remove the old columns immediately
-- ALTER TABLE "postgres_pipelines" DROP COLUMN IF EXISTS "source_type";
-- ALTER TABLE "postgres_pipelines" DROP COLUMN IF EXISTS "source_connection_id";
-- ALTER TABLE "postgres_pipelines" DROP COLUMN IF EXISTS "source_config";
-- ALTER TABLE "postgres_pipelines" DROP COLUMN IF EXISTS "source_schema";
-- ALTER TABLE "postgres_pipelines" DROP COLUMN IF EXISTS "source_table";
-- ALTER TABLE "postgres_pipelines" DROP COLUMN IF EXISTS "source_query";
-- ALTER TABLE "postgres_pipelines" DROP COLUMN IF EXISTS "destination_connection_id";
-- ALTER TABLE "postgres_pipelines" DROP COLUMN IF EXISTS "destination_schema";
-- ALTER TABLE "postgres_pipelines" DROP COLUMN IF EXISTS "destination_table";
-- ALTER TABLE "postgres_pipelines" DROP COLUMN IF EXISTS "destination_table_exists";
-- ALTER TABLE "postgres_pipelines" DROP COLUMN IF EXISTS "column_mappings";
-- ALTER TABLE "postgres_pipelines" DROP COLUMN IF EXISTS "write_mode";
-- ALTER TABLE "postgres_pipelines" DROP COLUMN IF EXISTS "upsert_key";

