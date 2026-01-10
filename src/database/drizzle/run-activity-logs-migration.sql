-- Run this SQL directly if the migration didn't create the table
-- Migration: Add Activity Logs Table

-- Create activity_logs table
CREATE TABLE IF NOT EXISTS "activity_logs" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"organization_id" uuid NOT NULL,
	"user_id" uuid,
	"action_type" text NOT NULL,
	"entity_type" text NOT NULL,
	"entity_id" uuid,
	"message" text NOT NULL,
	"metadata" jsonb,
	"created_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "activity_logs_organization_id_organizations_id_fk" FOREIGN KEY ("organization_id") REFERENCES "organizations"("id") ON DELETE CASCADE,
	CONSTRAINT "activity_logs_user_id_users_id_fk" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE SET NULL
);

-- Create indexes for efficient queries
CREATE INDEX IF NOT EXISTS "activity_logs_organization_id_idx" ON "activity_logs"("organization_id");
CREATE INDEX IF NOT EXISTS "activity_logs_org_entity_type_idx" ON "activity_logs"("organization_id", "entity_type");
CREATE INDEX IF NOT EXISTS "activity_logs_org_action_type_idx" ON "activity_logs"("organization_id", "action_type");
CREATE INDEX IF NOT EXISTS "activity_logs_user_id_idx" ON "activity_logs"("user_id");
CREATE INDEX IF NOT EXISTS "activity_logs_entity_idx" ON "activity_logs"("entity_type", "entity_id");
CREATE INDEX IF NOT EXISTS "activity_logs_created_at_idx" ON "activity_logs"("created_at");
