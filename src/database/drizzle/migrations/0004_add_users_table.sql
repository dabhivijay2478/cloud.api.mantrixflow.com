-- Migration: Add Users Table
-- This migration creates the users table for managing user data synced from Supabase Auth

-- Create user_status enum
DO $$ BEGIN
 CREATE TYPE "user_status" AS ENUM('active', 'inactive', 'suspended');
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;

-- Create users table
CREATE TABLE IF NOT EXISTS "users" (
	"id" uuid PRIMARY KEY NOT NULL,
	"email" varchar(255) NOT NULL,
	"first_name" varchar(100),
	"last_name" varchar(100),
	"full_name" varchar(200),
	"avatar_url" text,
	"supabase_user_id" varchar(255) NOT NULL,
	"email_verified" boolean DEFAULT false NOT NULL,
	"metadata" jsonb,
	"status" "user_status" DEFAULT 'active' NOT NULL,
	"current_org_id" uuid,
	"onboarding_completed" boolean DEFAULT false NOT NULL,
	"onboarding_step" varchar(50),
	"last_login_at" timestamp,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "users_email_unique" UNIQUE("email"),
	CONSTRAINT "users_supabase_user_id_unique" UNIQUE("supabase_user_id")
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS "users_email_idx" ON "users"("email");
CREATE INDEX IF NOT EXISTS "users_supabase_user_id_idx" ON "users"("supabase_user_id");
CREATE INDEX IF NOT EXISTS "users_current_org_id_idx" ON "users"("current_org_id");
CREATE INDEX IF NOT EXISTS "users_status_idx" ON "users"("status");
