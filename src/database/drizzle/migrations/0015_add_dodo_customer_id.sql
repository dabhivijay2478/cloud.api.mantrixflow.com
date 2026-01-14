-- Add dodo_customer_id column to subscriptions table
-- This allows us to track Dodo Payments customer IDs for easier data retrieval

ALTER TABLE subscriptions
ADD COLUMN IF NOT EXISTS dodo_customer_id VARCHAR(255);

-- Add index for faster lookups by customer ID
CREATE INDEX IF NOT EXISTS idx_subscriptions_dodo_customer_id ON subscriptions(dodo_customer_id);
