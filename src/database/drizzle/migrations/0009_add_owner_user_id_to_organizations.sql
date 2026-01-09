-- Migration: Add owner_user_id to organizations table
-- This migration adds the owner_user_id field to track which user created the organization

-- Add owner_user_id column to organizations table
ALTER TABLE "organizations" 
ADD COLUMN IF NOT EXISTS "owner_user_id" uuid;

-- Add foreign key constraint (only if it doesn't exist)
DO $$ 
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint 
    WHERE conname = 'organizations_owner_user_id_users_id_fk'
  ) THEN
    ALTER TABLE "organizations"
    ADD CONSTRAINT "organizations_owner_user_id_users_id_fk" 
    FOREIGN KEY ("owner_user_id") REFERENCES "users"("id") ON DELETE SET NULL;
  END IF;
END $$;

-- Create index for better query performance
CREATE INDEX IF NOT EXISTS "organizations_owner_user_id_idx" ON "organizations"("owner_user_id");
