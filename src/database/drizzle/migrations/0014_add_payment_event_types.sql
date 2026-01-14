-- Migration: Add Payment Event Types to subscription_event_type enum
-- This migration adds payment event types to the existing enum

-- Add new enum values to subscription_event_type
DO $$ BEGIN
  -- Add payment.succeeded
  IF NOT EXISTS (SELECT 1 FROM pg_enum WHERE enumlabel = 'payment.succeeded' AND enumtypid = (SELECT oid FROM pg_type WHERE typname = 'subscription_event_type')) THEN
    ALTER TYPE "subscription_event_type" ADD VALUE 'payment.succeeded';
  END IF;
  
  -- Add payment.failed
  IF NOT EXISTS (SELECT 1 FROM pg_enum WHERE enumlabel = 'payment.failed' AND enumtypid = (SELECT oid FROM pg_type WHERE typname = 'subscription_event_type')) THEN
    ALTER TYPE "subscription_event_type" ADD VALUE 'payment.failed';
  END IF;
  
  -- Add payment.processing
  IF NOT EXISTS (SELECT 1 FROM pg_enum WHERE enumlabel = 'payment.processing' AND enumtypid = (SELECT oid FROM pg_type WHERE typname = 'subscription_event_type')) THEN
    ALTER TYPE "subscription_event_type" ADD VALUE 'payment.processing';
  END IF;
  
  -- Add payment.cancelled
  IF NOT EXISTS (SELECT 1 FROM pg_enum WHERE enumlabel = 'payment.cancelled' AND enumtypid = (SELECT oid FROM pg_type WHERE typname = 'subscription_event_type')) THEN
    ALTER TYPE "subscription_event_type" ADD VALUE 'payment.cancelled';
  END IF;
  
  -- Add subscription.active
  IF NOT EXISTS (SELECT 1 FROM pg_enum WHERE enumlabel = 'subscription.active' AND enumtypid = (SELECT oid FROM pg_type WHERE typname = 'subscription_event_type')) THEN
    ALTER TYPE "subscription_event_type" ADD VALUE 'subscription.active';
  END IF;
END $$;
