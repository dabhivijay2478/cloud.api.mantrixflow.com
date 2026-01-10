-- Create billing_status enum
CREATE TYPE billing_status AS ENUM (
  'active',
  'trialing',
  'past_due',
  'canceled',
  'unpaid',
  'incomplete',
  'incomplete_expired',
  'paused'
);

-- Create billing_subscriptions table
CREATE TABLE IF NOT EXISTS billing_subscriptions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE UNIQUE,
  stripe_customer_id VARCHAR(255) NOT NULL UNIQUE,
  stripe_subscription_id VARCHAR(255),
  plan_id VARCHAR(100),
  billing_status billing_status NOT NULL DEFAULT 'incomplete',
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_billing_subscriptions_organization_id ON billing_subscriptions(organization_id);
CREATE INDEX IF NOT EXISTS idx_billing_subscriptions_stripe_customer_id ON billing_subscriptions(stripe_customer_id);
CREATE INDEX IF NOT EXISTS idx_billing_subscriptions_stripe_subscription_id ON billing_subscriptions(stripe_subscription_id);

-- Add comment
COMMENT ON TABLE billing_subscriptions IS 'Stores Stripe billing references for organizations. One Stripe Customer per organization.';
