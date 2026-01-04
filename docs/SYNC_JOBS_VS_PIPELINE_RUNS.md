# Sync Jobs vs Pipeline Runs - System Separation

## Overview

There are **TWO SEPARATE SYSTEMS** for job tracking in this codebase:

1. **`postgres_sync_jobs`** - For data source sync jobs (legacy system)
2. **`postgres_pipeline_runs`** - For pipeline execution runs (new system)

## System 1: Data Source Sync Jobs (`postgres_sync_jobs`)

**Purpose:** Tracks synchronization jobs from PostgreSQL data sources to destinations (legacy system).

**Used By:**
- `PostgresDataSourceService.createSyncJob()`
- `PostgresSyncService.startSync()`
- Data source sync operations

**Current State Tracking:**
- ✅ Status (pending, running, success, failed)
- ✅ Progress (rowsSynced) - updated during execution
- ✅ Cursor (lastSyncValue) - updated during incremental sync
- ✅ Destination table (destinationTable)
- ✅ Timestamps (startedAt, completedAt, updatedAt)
- ✅ Error tracking (errorMessage)

**Location:** `apps/api/src/modules/data-sources/postgres/services/postgres-sync.service.ts`

## System 2: Pipeline Runs (`postgres_pipeline_runs`)

**Purpose:** Tracks pipeline execution runs with authoritative state management.

**Used By:**
- `PostgresPipelineService.executePipeline()`
- Pipeline execution operations

**State Tracking (After Refactor):**
- ✅ Status (pending, running, success, failed, cancelled)
- ✅ Job state (pending, setup, running, paused, listing, stopped, completed, error)
- ✅ Resolved destination table (locked during setup)
- ✅ Resolved column mappings (SINGLE source of truth)
- ✅ Progress (rowsRead, rowsWritten, rowsSkipped, rowsFailed)
- ✅ Cursor (lastSyncCursor) - authoritative for incremental sync
- ✅ Timestamps (startedAt, completedAt, jobStateUpdatedAt, updatedAt)
- ✅ Error tracking (errorMessage, errorCode, errorStack)

**Location:** `apps/api/src/modules/data-pipelines/postgres-pipeline.service.ts`

## Key Differences

| Feature | `postgres_sync_jobs` | `postgres_pipeline_runs` |
|---------|---------------------|-------------------------|
| **System** | Data source sync (legacy) | Pipeline execution (new) |
| **Destination Table** | Stored but not locked | Resolved and locked during setup |
| **Column Mappings** | Not stored | Stored (SINGLE source of truth) |
| **Job State** | Basic status only | Comprehensive job state enum |
| **Cursor Tracking** | `lastSyncValue` | `lastSyncCursor` (authoritative) |
| **Table Creation** | Not handled | Resolved during setup phase |

## Migration Status

The pipeline system has been refactored to use `postgres_pipeline_runs` as the authoritative source of truth. The `postgres_sync_jobs` table remains for the legacy data source sync system.

## Recommendations

1. **For Pipelines:** Use `postgres_pipeline_runs` (already refactored)
2. **For Data Source Sync:** `postgres_sync_jobs` is properly used and tracks state
3. **Future:** Consider consolidating both systems if needed, but they serve different purposes

