-- Migration 0036: Fix job_state enum (add 'queued') and trigger_type enum (add 'api')
--
-- Root cause of scheduler crash:
--   The public.job_state enum lost the 'queued' value at some point (likely via an
--   external pgboss/Airbyte migration that replaced the enum values). The NestJS
--   scheduled-pipeline-worker inserts jobState: 'queued' on every scheduled run,
--   causing "invalid input value for enum job_state: 'queued'" on every poll cycle.
--
-- Secondary fix:
--   The public.trigger_type enum is missing 'api' (has 'webhook' instead).
--   pipeline.service.ts uses triggerType: 'api' which would crash any API-triggered run.
--
-- Note: ALTER TYPE ... ADD VALUE cannot run inside a transaction block in PostgreSQL,
-- so each addition is wrapped in a DO block with an existence guard. The migration
-- runner must execute this outside an explicit BEGIN/COMMIT wrapper.

-- ============================================================
-- Fix 1: Add missing 'queued' to public.job_state enum
-- ============================================================
-- Uses a schema-qualified OID lookup to avoid collision with
-- pgboss.job_state which also lives in this database.

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM   pg_enum     e
    JOIN   pg_type     t ON e.enumtypid   = t.oid
    JOIN   pg_namespace n ON t.typnamespace = n.oid
    WHERE  n.nspname   = 'public'
      AND  t.typname   = 'job_state'
      AND  e.enumlabel = 'queued'
  ) THEN
    ALTER TYPE public.job_state ADD VALUE 'queued';
  END IF;
END $$;

-- ============================================================
-- Fix 2: Add missing 'api' to public.trigger_type enum
-- ============================================================

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM   pg_enum     e
    JOIN   pg_type     t ON e.enumtypid   = t.oid
    JOIN   pg_namespace n ON t.typnamespace = n.oid
    WHERE  n.nspname   = 'public'
      AND  t.typname   = 'trigger_type'
      AND  e.enumlabel = 'api'
  ) THEN
    ALTER TYPE public.trigger_type ADD VALUE 'api';
  END IF;
END $$;

-- ============================================================
-- Fix 3: Reset stale next_scheduled_run_at on stuck pipelines
-- ============================================================
-- Pipelines whose next_scheduled_run_at is far in the past
-- (because every run attempt failed before the fix) would
-- trigger a flood of catch-up runs immediately after the enum
-- is fixed. We advance any stale timestamp to ~1 minute from
-- now so each pipeline gets exactly one clean trigger on the
-- next poll cycle.

UPDATE pipelines
SET
  next_scheduled_run_at = NOW() + INTERVAL '1 minute',
  updated_at            = NOW()
WHERE
  schedule_type        != 'none'
  AND deleted_at        IS NULL
  AND next_scheduled_run_at < NOW() - INTERVAL '5 minutes';
