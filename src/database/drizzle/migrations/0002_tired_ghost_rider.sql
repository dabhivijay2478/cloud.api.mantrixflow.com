-- Convert column to text first to allow value updates
ALTER TABLE "dodo_subscriptions" ALTER COLUMN "plan_id" SET DATA TYPE text;--> statement-breakpoint
-- Update any existing 'basic' values to 'free'
UPDATE "dodo_subscriptions" SET "plan_id" = 'free' WHERE "plan_id" = 'basic';--> statement-breakpoint
-- Drop old enum type
DROP TYPE "public"."subscription_plan";--> statement-breakpoint
-- Create new enum type with updated values
CREATE TYPE "public"."subscription_plan" AS ENUM('free', 'pro', 'scale', 'enterprise');--> statement-breakpoint
-- Convert column back to enum type
ALTER TABLE "dodo_subscriptions" ALTER COLUMN "plan_id" SET DATA TYPE "public"."subscription_plan" USING "plan_id"::"public"."subscription_plan";