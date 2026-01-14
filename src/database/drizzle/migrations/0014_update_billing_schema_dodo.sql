-- Migration: Update billing schema for Dodo Payments
-- This migration updates organizations table for Dodo Payments integration

-- Update billing fields in organizations table (if not already added)
ALTER TABLE organizations
ADD COLUMN IF NOT EXISTS billing_provider VARCHAR(50) DEFAULT 'dodo',
ADD COLUMN IF NOT EXISTS billing_customer_id VARCHAR(255),
ADD COLUMN IF NOT EXISTS billing_subscription_id VARCHAR(255),
ADD COLUMN IF NOT EXISTS billing_plan_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS billing_status VARCHAR(50) DEFAULT 'incomplete',
ADD COLUMN IF NOT EXISTS billing_current_period_end TIMESTAMP;

-- Rename columns to be provider-agnostic (if they exist with old names)
-- Note: These ALTER statements will only run if columns don't exist
DO $$
BEGIN
  -- Rename dodo_customer_id to billing_customer_id if it exists
  IF EXISTS (SELECT 1 FROM information_schema.columns 
             WHERE table_name = 'organizations' AND column_name = 'dodo_customer_id') THEN
    ALTER TABLE organizations RENAME COLUMN dodo_customer_id TO billing_customer_id;
  END IF;
  
  -- Rename dodo_subscription_id to billing_subscription_id if it exists
  IF EXISTS (SELECT 1 FROM information_schema.columns 
             WHERE table_name = 'organizations' AND column_name = 'dodo_subscription_id') THEN
    ALTER TABLE organizations RENAME COLUMN dodo_subscription_id TO billing_subscription_id;
  END IF;
END $$;

-- Create indexes for billing fields
CREATE INDEX IF NOT EXISTS idx_organizations_billing_customer_id ON organizations(billing_customer_id);
CREATE INDEX IF NOT EXISTS idx_organizations_billing_subscription_id ON organizations(billing_subscription_id);
CREATE INDEX IF NOT EXISTS idx_organizations_billing_provider ON organizations(billing_provider);

-- Create subscription_events table for webhook audit log
CREATE TABLE IF NOT EXISTS subscription_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,
  provider VARCHAR(50) NOT NULL, -- 'dodo'
  event_type VARCHAR(100) NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create indexes for subscription_events table
CREATE INDEX IF NOT EXISTS idx_subscription_events_organization_id ON subscription_events(organization_id);
CREATE INDEX IF NOT EXISTS idx_subscription_events_provider ON subscription_events(provider);
CREATE INDEX IF NOT EXISTS idx_subscription_events_event_type ON subscription_events(event_type);
CREATE INDEX IF NOT EXISTS idx_subscription_events_created_at ON subscription_events(created_at);

-- Add comment
COMMENT ON TABLE subscription_events IS 'Audit log for billing webhook events. Stores raw payloads for debugging and compliance.';
