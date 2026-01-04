-- Migration: Add Organization Members Table
-- This migration creates the organization_members table for tracking organization membership and invites

-- Create organization_member_status enum
DO $$ BEGIN
 CREATE TYPE "organization_member_status" AS ENUM('invited', 'accepted', 'active', 'inactive');
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;

-- Create organization_member_role enum
DO $$ BEGIN
 CREATE TYPE "organization_member_role" AS ENUM('owner', 'admin', 'member', 'viewer', 'guest');
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;

-- Create organization_members table
CREATE TABLE IF NOT EXISTS "organization_members" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"organization_id" uuid NOT NULL,
	"user_id" uuid,
	"email" varchar(255) NOT NULL,
	"role" "organization_member_role" DEFAULT 'member' NOT NULL,
	"status" "organization_member_status" DEFAULT 'invited' NOT NULL,
	"invited_by" uuid,
	"invited_at" timestamp DEFAULT now() NOT NULL,
	"accepted_at" timestamp,
	"agent_panel_access" boolean DEFAULT false NOT NULL,
	"allowed_models" jsonb DEFAULT '[]'::jsonb,
	"metadata" jsonb,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "organization_members_organization_id_organizations_id_fk" FOREIGN KEY ("organization_id") REFERENCES "organizations"("id") ON DELETE CASCADE,
	CONSTRAINT "organization_members_user_id_users_id_fk" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE,
	CONSTRAINT "organization_members_invited_by_users_id_fk" FOREIGN KEY ("invited_by") REFERENCES "users"("id") ON DELETE SET NULL
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS "organization_members_organization_id_idx" ON "organization_members"("organization_id");
CREATE INDEX IF NOT EXISTS "organization_members_user_id_idx" ON "organization_members"("user_id");
CREATE INDEX IF NOT EXISTS "organization_members_email_idx" ON "organization_members"("email");
CREATE INDEX IF NOT EXISTS "organization_members_status_idx" ON "organization_members"("status");
CREATE INDEX IF NOT EXISTS "organization_members_organization_email_idx" ON "organization_members"("organization_id", "email");

-- Create unique constraint: one active/invited member per email per organization
-- This prevents duplicate invites while allowing inactive members to be re-invited
CREATE UNIQUE INDEX IF NOT EXISTS "organization_members_org_email_active_unique" 
ON "organization_members"("organization_id", "email") 
WHERE "status" IN ('invited', 'accepted', 'active');

