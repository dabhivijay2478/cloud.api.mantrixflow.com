-- Migration: Add Organization Owners Table
-- This migration creates the organization_owners table to clearly separate ownership from membership
-- A user can own multiple organizations, and ownership is distinct from membership

-- Create organization_owners table
CREATE TABLE IF NOT EXISTS "organization_owners" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"organization_id" uuid NOT NULL,
	"user_id" uuid NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "organization_owners_organization_id_organizations_id_fk" FOREIGN KEY ("organization_id") REFERENCES "organizations"("id") ON DELETE CASCADE,
	CONSTRAINT "organization_owners_user_id_users_id_fk" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE
);

-- Create unique constraint to ensure one owner record per organization-user pair
CREATE UNIQUE INDEX IF NOT EXISTS "organization_owners_organization_user_unique" ON "organization_owners"("organization_id", "user_id");

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS "organization_owners_organization_id_idx" ON "organization_owners"("organization_id");
CREATE INDEX IF NOT EXISTS "organization_owners_user_id_idx" ON "organization_owners"("user_id");

-- Migrate existing ownership data from organizations.owner_user_id to organization_owners table
-- This ensures existing organizations have proper ownership records
INSERT INTO "organization_owners" ("organization_id", "user_id", "created_at", "updated_at")
SELECT 
    o.id AS organization_id,
    o.owner_user_id AS user_id,
    o.created_at,
    o.updated_at
FROM "organizations" o
WHERE o.owner_user_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM "organization_owners" oo 
    WHERE oo.organization_id = o.id AND oo.user_id = o.owner_user_id
  )
ON CONFLICT DO NOTHING;
