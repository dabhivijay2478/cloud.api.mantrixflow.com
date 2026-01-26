-- Migration: Add pipeline scheduling fields
-- Description: Adds columns for advanced scheduling configuration

-- Add schedule_type enum
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'schedule_type') THEN
        CREATE TYPE schedule_type AS ENUM ('none', 'minutes', 'hourly', 'daily', 'weekly', 'monthly', 'custom_cron');
    END IF;
END
$$;

-- Add new scheduling columns to pipelines table
ALTER TABLE pipelines
ADD COLUMN IF NOT EXISTS schedule_type schedule_type DEFAULT 'none',
ADD COLUMN IF NOT EXISTS schedule_value varchar(255),
ADD COLUMN IF NOT EXISTS schedule_timezone varchar(50) DEFAULT 'UTC',
ADD COLUMN IF NOT EXISTS last_scheduled_run_at timestamp,
ADD COLUMN IF NOT EXISTS next_scheduled_run_at timestamp;

-- Update sync_frequency default if needed
COMMENT ON COLUMN pipelines.schedule_type IS 'Type of schedule: none, minutes, hourly, daily, weekly, monthly, custom_cron';
COMMENT ON COLUMN pipelines.schedule_value IS 'Schedule value: e.g. "15" for minutes, "14:30" for daily, "0 3 * * *" for cron';
COMMENT ON COLUMN pipelines.schedule_timezone IS 'Timezone for schedule (e.g. America/New_York, Asia/Kolkata)';
COMMENT ON COLUMN pipelines.last_scheduled_run_at IS 'Timestamp of last scheduled run';
COMMENT ON COLUMN pipelines.next_scheduled_run_at IS 'Calculated next scheduled run time';
