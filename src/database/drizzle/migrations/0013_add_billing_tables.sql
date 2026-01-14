-- Migration: Add Billing Tables
-- This migration creates the subscriptions and subscription_events tables for billing

-- Create subscription_status enum
DO $$ BEGIN
  CREATE TYPE "subscription_status" AS ENUM('active', 'on_hold', 'failed', 'canceled', 'trialing');
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;

-- Create subscription_plan enum
DO $$ BEGIN
  CREATE TYPE "subscription_plan" AS ENUM('basic', 'pro', 'enterprise');
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;

-- Create subscription_event_type enum
DO $$ BEGIN
  CREATE TYPE "subscription_event_type" AS ENUM(
    'subscription.created',
    'subscription.activated',
    'subscription.updated',
    'subscription.on_hold',
    'subscription.renewed',
    'subscription.canceled',
    'subscription.failed',
    'subscription.trial_started',
    'subscription.trial_ended'
  );
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;

-- Create subscriptions table
CREATE TABLE IF NOT EXISTS "subscriptions" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
  "user_id" uuid NOT NULL UNIQUE,
  "dodo_subscription_id" varchar(255) UNIQUE,
  "plan_id" "subscription_plan" NOT NULL,
  "status" "subscription_status" DEFAULT 'trialing' NOT NULL,
  "current_period_start" timestamp,
  "current_period_end" timestamp,
  "trial_start" timestamp,
  "trial_end" timestamp,
  "canceled_at" timestamp,
  "cancel_at_period_end" timestamp,
  "metadata" jsonb,
  "created_at" timestamp DEFAULT now() NOT NULL,
  "updated_at" timestamp DEFAULT now() NOT NULL,
  CONSTRAINT "subscriptions_user_id_users_id_fk" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE
);

-- Create subscription_events table
CREATE TABLE IF NOT EXISTS "subscription_events" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
  "subscription_id" uuid NOT NULL,
  "event_type" "subscription_event_type" NOT NULL,
  "dodo_event_id" varchar(255) UNIQUE,
  "payload" jsonb NOT NULL,
  "processed" timestamp DEFAULT now(),
  "error" jsonb,
  "created_at" timestamp DEFAULT now() NOT NULL,
  CONSTRAINT "subscription_events_subscription_id_subscriptions_id_fk" FOREIGN KEY ("subscription_id") REFERENCES "subscriptions"("id") ON DELETE CASCADE
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS "subscriptions_user_id_idx" ON "subscriptions"("user_id");
CREATE INDEX IF NOT EXISTS "subscriptions_dodo_subscription_id_idx" ON "subscriptions"("dodo_subscription_id");
CREATE INDEX IF NOT EXISTS "subscriptions_status_idx" ON "subscriptions"("status");
CREATE INDEX IF NOT EXISTS "subscription_events_subscription_id_idx" ON "subscription_events"("subscription_id");
CREATE INDEX IF NOT EXISTS "subscription_events_dodo_event_id_idx" ON "subscription_events"("dodo_event_id");
CREATE INDEX IF NOT EXISTS "subscription_events_event_type_idx" ON "subscription_events"("event_type");
