-- Migration: Update billing schema for provider-agnostic billing (Razorpay)
-- This migration updates organizations table and creates subscriptions table

-- Add billing fields to organizations table
ALTER TABLE organizations
ADD COLUMN IF NOT EXISTS billing_provider VARCHAR(50),
ADD COLUMN IF NOT EXISTS billing_customer_id VARCHAR(255),
ADD COLUMN IF NOT EXISTS billing_subscription_id VARCHAR(255),
ADD COLUMN IF NOT EXISTS billing_plan_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS billing_status VARCHAR(50) DEFAULT 'incomplete',
ADD COLUMN IF NOT EXISTS billing_current_period_end TIMESTAMP;

-- Create indexes for billing fields
CREATE INDEX IF NOT EXISTS idx_organizations_billing_customer_id ON organizations(billing_customer_id);
CREATE INDEX IF NOT EXISTS idx_organizations_billing_subscription_id ON organizations(billing_subscription_id);
CREATE INDEX IF NOT EXISTS idx_organizations_billing_provider ON organizations(billing_provider);

-- Drop old billing_subscriptions table if it exists (from Stripe implementation)
DROP TABLE IF EXISTS billing_subscriptions CASCADE;

-- Create new subscriptions table (provider-agnostic)
CREATE TABLE IF NOT EXISTS subscriptions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  provider VARCHAR(50) NOT NULL, -- 'razorpay' | 'stripe'
  plan_id VARCHAR(100) NOT NULL, -- 'free' | 'pro' | 'scale'
  provider_subscription_id VARCHAR(255) NOT NULL UNIQUE,
  status VARCHAR(50) NOT NULL DEFAULT 'incomplete',
  current_period_start TIMESTAMP,
  current_period_end TIMESTAMP,
  cancel_at_period_end BOOLEAN DEFAULT false,
  amount DECIMAL(10, 2),
  currency VARCHAR(10) DEFAULT 'INR',
  metadata JSONB,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create indexes for subscriptions table
CREATE INDEX IF NOT EXISTS idx_subscriptions_organization_id ON subscriptions(organization_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_provider_subscription_id ON subscriptions(provider_subscription_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_provider ON subscriptions(provider);
CREATE INDEX IF NOT EXISTS idx_subscriptions_status ON subscriptions(status);

-- Add comment
COMMENT ON TABLE subscriptions IS 'Provider-agnostic subscription records. Supports Razorpay and future providers like Stripe.';
