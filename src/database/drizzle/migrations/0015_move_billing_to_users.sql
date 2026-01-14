-- Migration: Move billing from organization-level to user-level
-- Billing is now user-scoped (one user can have billing for multiple organizations)

-- Step 1: Add billing fields to users table
ALTER TABLE users
ADD COLUMN IF NOT EXISTS billing_provider VARCHAR(50),
ADD COLUMN IF NOT EXISTS billing_customer_id VARCHAR(255),
ADD COLUMN IF NOT EXISTS billing_subscription_id VARCHAR(255),
ADD COLUMN IF NOT EXISTS billing_plan_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS billing_status VARCHAR(50) DEFAULT 'incomplete',
ADD COLUMN IF NOT EXISTS billing_current_period_end TIMESTAMP;

-- Step 2: Create indexes for user billing fields
CREATE INDEX IF NOT EXISTS idx_users_billing_customer_id ON users(billing_customer_id);
CREATE INDEX IF NOT EXISTS idx_users_billing_subscription_id ON users(billing_subscription_id);
CREATE INDEX IF NOT EXISTS idx_users_billing_provider ON users(billing_provider);

-- Step 3: Migrate existing billing data from organizations to users
-- For each organization with billing, find the owner user and copy billing data
UPDATE users
SET 
  billing_provider = org.billing_provider,
  billing_customer_id = org.billing_customer_id,
  billing_subscription_id = org.billing_subscription_id,
  billing_plan_id = org.billing_plan_id,
  billing_status = org.billing_status,
  billing_current_period_end = org.billing_current_period_end
FROM organizations org
WHERE users.id = org.owner_user_id
  AND org.billing_customer_id IS NOT NULL;

-- Step 4: Update subscriptions table to reference user_id instead of organization_id
-- First, add user_id column to subscriptions
ALTER TABLE subscriptions
ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES users(id) ON DELETE CASCADE;

-- Make organization_id nullable (billing is user-scoped, org is optional reference)
ALTER TABLE subscriptions
ALTER COLUMN organization_id DROP NOT NULL;

-- Migrate subscription user_id from organization owner
UPDATE subscriptions s
SET user_id = o.owner_user_id
FROM organizations o
WHERE s.organization_id = o.id
  AND o.owner_user_id IS NOT NULL
  AND s.user_id IS NULL;

-- Step 5: Remove billing fields from organizations table (after migration)
-- Note: We keep organization_id in subscriptions for reference, but billing is user-scoped
ALTER TABLE organizations
DROP COLUMN IF EXISTS billing_provider,
DROP COLUMN IF EXISTS billing_customer_id,
DROP COLUMN IF EXISTS billing_subscription_id,
DROP COLUMN IF EXISTS billing_plan_id,
DROP COLUMN IF EXISTS billing_status,
DROP COLUMN IF EXISTS billing_current_period_end;

-- Step 6: Update subscription_events table to include user_id
-- First check if subscription_events table exists, if not create it
CREATE TABLE IF NOT EXISTS subscription_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID REFERENCES users(id) ON DELETE CASCADE,
  organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,
  provider VARCHAR(50) NOT NULL,
  event_type VARCHAR(100) NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT now()
);

-- Add user_id column if table already exists but column doesn't
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns 
    WHERE table_name = 'subscription_events' AND column_name = 'user_id'
  ) THEN
    ALTER TABLE subscription_events
    ADD COLUMN user_id UUID REFERENCES users(id) ON DELETE CASCADE;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_subscription_events_user_id ON subscription_events(user_id);

-- Add comment
COMMENT ON TABLE subscriptions IS 'User-scoped subscriptions. One user can have subscriptions for multiple organizations.';
COMMENT ON COLUMN subscriptions.user_id IS 'User who owns this subscription (billing is user-scoped)';
COMMENT ON COLUMN subscriptions.organization_id IS 'Organization this subscription applies to (for reference only)';
