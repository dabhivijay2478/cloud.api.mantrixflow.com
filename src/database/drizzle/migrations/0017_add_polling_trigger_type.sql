-- Migration: Add 'polling' to trigger_type enum
-- Adds 'polling' as a valid trigger type for pipeline runs

-- Note: ALTER TYPE ... ADD VALUE cannot be run inside a transaction block
-- PostgreSQL doesn't support IF NOT EXISTS for ALTER TYPE ADD VALUE
-- So we check if the value exists first, then add it if needed

DO $$
BEGIN
  -- Check if 'polling' already exists in the enum
  IF NOT EXISTS (
    SELECT 1 
    FROM pg_enum 
    WHERE enumlabel = 'polling' 
    AND enumtypid = (SELECT oid FROM pg_type WHERE typname = 'trigger_type')
  ) THEN
    -- Add 'polling' to the trigger_type enum
    ALTER TYPE trigger_type ADD VALUE 'polling';
  END IF;
END $$;

-- Verify the enum now includes 'polling'
-- SELECT unnest(enum_range(NULL::trigger_type));
