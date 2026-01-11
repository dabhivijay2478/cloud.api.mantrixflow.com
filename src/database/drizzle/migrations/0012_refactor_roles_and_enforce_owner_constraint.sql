-- Migration: Refactor Roles and Enforce ONE OWNER Constraint
-- This migration:
-- 1. Updates the role enum to use uppercase: OWNER, ADMIN, EDITOR, VIEWER
-- 2. Removes 'guest' and 'member' roles (mapped to EDITOR)
-- 3. Enforces ONE OWNER per organization constraint
-- 4. Migrates existing data to new role system

-- Step 1: Create new enum type with uppercase roles
DO $$ BEGIN
  CREATE TYPE "organization_member_role_new" AS ENUM('OWNER', 'ADMIN', 'EDITOR', 'VIEWER');
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;

-- Step 2: Add temporary column with new enum type
ALTER TABLE "organization_members" 
ADD COLUMN "role_new" "organization_member_role_new";

-- Step 3: Migrate existing data
-- Map old roles to new roles:
-- 'owner' -> 'OWNER'
-- 'admin' -> 'ADMIN'
-- 'member' -> 'EDITOR' (member becomes editor)
-- 'viewer' -> 'VIEWER'
-- 'guest' -> 'VIEWER' (guest becomes viewer)
UPDATE "organization_members"
SET "role_new" = CASE
  WHEN "role"::text = 'owner' THEN 'OWNER'::organization_member_role_new
  WHEN "role"::text = 'admin' THEN 'ADMIN'::organization_member_role_new
  WHEN "role"::text = 'member' THEN 'EDITOR'::organization_member_role_new
  WHEN "role"::text = 'viewer' THEN 'VIEWER'::organization_member_role_new
  WHEN "role"::text = 'guest' THEN 'VIEWER'::organization_member_role_new
  ELSE 'VIEWER'::organization_member_role_new
END;

-- Step 4: Ensure exactly ONE OWNER per organization
-- If multiple owners exist, keep the first one (by created_at) and demote others to ADMIN
DO $$
DECLARE
  org_record RECORD;
  owner_count INTEGER;
  first_owner_id UUID;
BEGIN
  FOR org_record IN SELECT DISTINCT "organization_id" FROM "organization_members" LOOP
    -- Count owners for this organization
    SELECT COUNT(*) INTO owner_count
    FROM "organization_members"
    WHERE "organization_id" = org_record.organization_id
      AND "role_new" = 'OWNER';
    
    -- If more than one owner, keep the first and demote others
    IF owner_count > 1 THEN
      -- Get the first owner (by created_at)
      SELECT "id" INTO first_owner_id
      FROM "organization_members"
      WHERE "organization_id" = org_record.organization_id
        AND "role_new" = 'OWNER'
      ORDER BY "created_at" ASC
      LIMIT 1;
      
      -- Demote other owners to ADMIN
      UPDATE "organization_members"
      SET "role_new" = 'ADMIN'::organization_member_role_new
      WHERE "organization_id" = org_record.organization_id
        AND "role_new" = 'OWNER'
        AND "id" != first_owner_id;
    END IF;
  END LOOP;
END $$;

-- Step 5: Set default for any NULL values (shouldn't happen, but safety check)
UPDATE "organization_members"
SET "role_new" = 'VIEWER'::organization_member_role_new
WHERE "role_new" IS NULL;

-- Step 6: Make new column NOT NULL
ALTER TABLE "organization_members"
ALTER COLUMN "role_new" SET NOT NULL;

-- Step 7: Drop old column and rename new column
ALTER TABLE "organization_members"
DROP COLUMN "role";

ALTER TABLE "organization_members"
RENAME COLUMN "role_new" TO "role";

-- Step 8: Drop old enum type and rename new one
-- First, drop the old enum (this will fail if still referenced, but we've already migrated)
DROP TYPE IF EXISTS "organization_member_role" CASCADE;

-- Rename new enum to the original name
ALTER TYPE "organization_member_role_new" RENAME TO "organization_member_role";

-- Step 9: Update default value
ALTER TABLE "organization_members"
ALTER COLUMN "role" SET DEFAULT 'VIEWER'::organization_member_role;

-- Step 10: Create unique constraint to enforce ONE OWNER per organization
-- This constraint ensures that only one member per organization can have the OWNER role
CREATE UNIQUE INDEX IF NOT EXISTS "organization_members_one_owner_per_org"
ON "organization_members"("organization_id")
WHERE "role" = 'OWNER' AND "status" IN ('active', 'accepted');

-- Step 11: Add comment on the index (not constraint, since it's a unique index)
-- This is enforced at the application level, but adding a comment for documentation
COMMENT ON INDEX "organization_members_one_owner_per_org" IS 
'Enforces exactly ONE OWNER per organization. Only one active/accepted member can have OWNER role per organization.';
