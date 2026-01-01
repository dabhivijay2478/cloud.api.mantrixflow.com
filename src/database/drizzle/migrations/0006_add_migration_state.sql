-- Migration: Add Migration State to Postgres Pipelines
-- This migration adds a migration_state enum and column to track pipeline migration status

-- Step 1: Create migration_state enum
DO $$ BEGIN
 CREATE TYPE "migration_state" AS ENUM('pending', 'running', 'listing');
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;

-- Step 2: Add migration_state column to postgres_pipelines table
ALTER TABLE "postgres_pipelines" ADD COLUMN IF NOT EXISTS "migration_state" "migration_state" DEFAULT 'pending';

-- Step 3: Update existing pipelines to have 'pending' state if null
UPDATE "postgres_pipelines" SET "migration_state" = 'pending' WHERE "migration_state" IS NULL;

-- Step 4: Set NOT NULL constraint after updating existing rows
ALTER TABLE "postgres_pipelines" ALTER COLUMN "migration_state" SET NOT NULL;
ALTER TABLE "postgres_pipelines" ALTER COLUMN "migration_state" SET DEFAULT 'pending';

