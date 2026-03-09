-- MantrixFlow Email System
-- email_send_log: Cooldowns and deduplication
-- email_preferences: User opt-outs
-- email_suppression_sync: UnoSend bounces/unsubscribes
-- Organization, user, pipeline columns for billing and engagement

-- email_send_log
CREATE TABLE IF NOT EXISTS "email_send_log" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
  "org_id" uuid REFERENCES "organizations"("id") ON DELETE CASCADE,
  "user_id" uuid REFERENCES "users"("id") ON DELETE CASCADE,
  "email_type" varchar(100) NOT NULL,
  "recipient_email" varchar(255) NOT NULL,
  "pipeline_id" uuid REFERENCES "pipelines"("id") ON DELETE CASCADE,
  "connection_id" uuid REFERENCES "data_source_connections"("id") ON DELETE CASCADE,
  "sent_at" timestamp with time zone NOT NULL DEFAULT now(),
  "unosend_message_id" varchar(255)
);
CREATE INDEX IF NOT EXISTS "idx_email_send_log_pipeline_type" ON "email_send_log" ("pipeline_id", "email_type");
CREATE INDEX IF NOT EXISTS "idx_email_send_log_sent_at" ON "email_send_log" ("sent_at");

-- email_preferences
CREATE TABLE IF NOT EXISTS "email_preferences" (
  "user_id" uuid PRIMARY KEY REFERENCES "users"("id") ON DELETE CASCADE NOT NULL,
  "weekly_digest_enabled" boolean NOT NULL DEFAULT true,
  "pipeline_failure_emails" boolean NOT NULL DEFAULT true,
  "marketing_emails" boolean NOT NULL DEFAULT true,
  "updated_at" timestamp with time zone NOT NULL DEFAULT now()
);

-- email_suppression_sync
CREATE TABLE IF NOT EXISTS "email_suppression_sync" (
  "email" varchar(255) PRIMARY KEY NOT NULL,
  "suppression_reason" varchar(50),
  "suppressed_at" timestamp with time zone NOT NULL DEFAULT now()
);

-- Organization schema additions (billing)
ALTER TABLE "organizations" ADD COLUMN IF NOT EXISTS "trial_ends_at" timestamp with time zone;
ALTER TABLE "organizations" ADD COLUMN IF NOT EXISTS "subscription_status" varchar(50);
ALTER TABLE "organizations" ADD COLUMN IF NOT EXISTS "plan_name" varchar(100);
ALTER TABLE "organizations" ADD COLUMN IF NOT EXISTS "plan_run_limit" integer;

-- User schema additions (engagement)
ALTER TABLE "users" ADD COLUMN IF NOT EXISTS "onboarding_nudge_1_sent_at" timestamp with time zone;
ALTER TABLE "users" ADD COLUMN IF NOT EXISTS "onboarding_nudge_2_sent_at" timestamp with time zone;

-- Pipeline schema additions (cooldown)
ALTER TABLE "pipelines" ADD COLUMN IF NOT EXISTS "last_failure_email_sent_at" timestamp with time zone;
