# Pipeline Architecture Refactor - Complete Documentation

## Overview

This document describes the complete architectural refactor of the PostgreSQL pipeline execution system. This refactor fixes **systemic design problems** that caused incorrect table creation and broken job tracking.

## Problems Fixed

### Issue 1: Wrong Destination Table Used
**Before:** Even when a user selected an existing destination table, the system created a NEW table instead of using it.

**After:** Destination table is resolved ONCE during setup and LOCKED in the run record. Migration execution reads from the run record and cannot override or recreate tables.

### Issue 2: Broken Job Tracking
**Before:** `postgres_pipeline_runs` table existed but didn't store meaningful job state, progress, or cursor information.

**After:** Run record is the AUTHORITATIVE source of truth for:
- Resolved destination table (locked during setup)
- Job execution state
- Migration progress and cursor
- Column mappings

### Issue 3: Scattered Table Creation Logic
**Before:** Table creation happened in multiple places (pipeline execution, emitters, sync logic), causing new tables to be created even when existing ones were selected.

**After:** Table creation happens ONLY in `resolveAndPrepareDestinationTable()` during setup phase. Execution phase NEVER creates tables.

## New Architecture

### Phase 1: Setup (ONE TIME)

**Purpose:** Resolve destination table and lock it in run record.

**Steps:**
1. Create pipeline run record
2. Extract field mappings from transformers (SINGLE source of truth)
3. Call `resolveAndPrepareDestinationTable()`:
   - Checks if table exists in database
   - Determines if table should be created or used
   - Creates table if needed (ONLY in setup phase)
   - Returns resolved table information
4. **LOCK resolved table in run record:**
   - `resolved_destination_schema`
   - `resolved_destination_table`
   - `resolved_column_mappings`
   - `destination_table_was_created`
   - `job_state = 'running'`

**Rules:**
- If `destinationTableExists = true` → MUST use existing table, NEVER create
- If `destinationTableExists = false` → Create new table ONLY if auto-generated name
- Field mappings are the SINGLE source of truth for schema creation

### Phase 2: Migration Execution

**Purpose:** Read data, transform it, and write to destination.

**Steps:**
1. **Read resolved table from run record (AUTHORITATIVE)**
   - Cannot be overridden
   - Ensures correct table is used
2. Read from source (using cursor from run record if incremental)
3. Transform data (apply field mappings - only mapped fields)
4. Write to destination:
   - Uses resolved table from run record
   - Filters to only mapped columns
   - Uses upsert for UUID primary keys
   - **NEVER creates tables**
5. Update run record:
   - `job_state = 'completed'`
   - `last_sync_cursor` (for incremental sync)
   - Execution statistics

## Database Schema Changes

### Extended `postgres_pipeline_runs` Table

New fields added:

```sql
-- Resolved destination table (AUTHORITATIVE - locked during setup)
resolved_destination_schema varchar(255)
resolved_destination_table varchar(255)
destination_table_was_created varchar(10) -- 'true' | 'false'
resolved_column_mappings jsonb

-- Job state tracking (AUTHORITATIVE - drives migration behavior)
job_state job_state DEFAULT 'pending'
last_sync_cursor text -- Authoritative cursor for incremental sync
job_state_updated_at timestamp

-- Updated timestamp
updated_at timestamp DEFAULT now() NOT NULL
```

### New Enum: `job_state`

```sql
CREATE TYPE "job_state" AS ENUM (
    'pending',
    'setup',
    'running',
    'paused',
    'listing',
    'stopped',
    'completed',
    'error'
);
```

## Primary Key & Upsert Rules

- **Only UUID is allowed as primary key**
- **Same UUID → UPDATE existing row**
- **New UUID → INSERT new row**
- This logic works identically for:
  - Existing tables
  - Newly created tables

## Field Mappings as Single Source of Truth

Field mappings determine:
- **Which columns are migrated** (only mapped fields)
- **Schema creation** (if applicable - only mapped columns)
- **Insert/update payload structure** (only mapped fields included)

## Migration File

The migration SQL file is located at:
```
apps/api/src/database/drizzle/migrations/0007_add_pipeline_run_job_tracking.sql
```

**To apply:**
```bash
# Option 1: Using psql
psql $DATABASE_URL -f apps/api/src/database/drizzle/migrations/0007_add_pipeline_run_job_tracking.sql

# Option 2: Using Drizzle migrate (after updating journal)
bun run db:migrate
```

## Key Methods

### `resolveAndPrepareDestinationTable()`
- **Purpose:** Resolve destination table ONCE during setup
- **When:** Called during setup phase, BEFORE migration
- **Returns:** Resolved table information
- **Side Effect:** Creates table if needed (ONLY in setup phase)

### `writeToDestination()`
- **Purpose:** Write data to destination
- **When:** Called during migration phase
- **Input:** Resolved table from run record (AUTHORITATIVE)
- **Behavior:** ONLY writes data, NEVER creates tables

## Benefits

1. **Deterministic:** Table resolution happens once and is locked
2. **Authoritative:** Run record is the single source of truth
3. **Safe:** Execution cannot override or recreate tables
4. **Trackable:** Job state, progress, and cursor are properly tracked
5. **Maintainable:** Clear separation of setup and execution phases

## Testing Checklist

- [ ] Existing table selected → uses existing table (no new table created)
- [ ] New table selected → creates new table (only in setup)
- [ ] Field mappings respected → only mapped fields migrated
- [ ] Run record stores resolved table correctly
- [ ] Migration execution reads from run record
- [ ] Job state tracked correctly
- [ ] Cursor stored in run record for incremental sync
- [ ] UUID primary key upsert works correctly

