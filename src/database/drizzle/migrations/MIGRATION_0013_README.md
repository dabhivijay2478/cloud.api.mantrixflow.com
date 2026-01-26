# Migration 0013: Refactor to Dynamic Data Sources Architecture

## Overview

This migration refactors the database from a static PostgreSQL-only connection architecture to a flexible, multi-source data source architecture that supports PostgreSQL, MySQL, MongoDB, S3, APIs, and other data source types.

## Key Changes

### New Tables Created

1. **`data_sources`** - Organization-level data source registry
   - Stores metadata about data sources (name, description, source_type)
   - Organization-centric (belongs to organizations, not users)
   - Supports soft delete via `deleted_at`

2. **`data_source_connections`** - Dynamic connection credentials storage
   - Uses JSONB `config` field to store connection details for any data source type
   - 1:1 relationship with `data_sources`
   - Supports connection testing, schema caching, and status tracking

### Tables Modified

1. **`organizations`**
   - `owner_user_id` is now NOT NULL (required)
   - Ownership is determined by this field, not a separate table

2. **`pipeline_source_schemas`**
   - Replaced `source_connection_id` with `data_source_id`
   - Renamed `org_id` to `organization_id`
   - Removed `user_id` column (organization-level, not user-level)

3. **`pipeline_destination_schemas`**
   - Replaced `destination_connection_id` with `data_source_id`
   - Renamed `org_id` to `organization_id`
   - Removed `user_id` column

4. **`pipelines`** (renamed from `postgres_pipelines`)
   - Renamed `org_id` to `organization_id`
   - Renamed `user_id` to `created_by`
   - Now references `pipeline_source_schemas` and `pipeline_destination_schemas`

5. **`pipeline_runs`** (renamed from `postgres_pipeline_runs`)
   - Renamed `org_id` to `organization_id`
   - Added `job_state` column
   - Updated foreign key to reference `pipelines` table

6. **`query_logs`** (renamed from `postgres_query_logs`)
   - Replaced `connection_id` with `data_source_id`
   - Updated to reference `data_sources` table

### Tables Dropped

1. **`organization_owners`** - Ownership is now in `organizations.owner_user_id`
2. **`postgres_sync_jobs`** - Consolidated into `pipelines` table
3. **`postgres_connections`** - Migrated to `data_sources` + `data_source_connections`

## Data Migration

### Step 1: Migrate postgres_connections

For each row in `postgres_connections`:
- Create entry in `data_sources` with `source_type = 'postgres'`
- Create entry in `data_source_connections` with all connection fields stored in `config` JSONB
- Map: host, port, database, username, password, ssl settings, ssh tunnel, pool settings

### Step 2: Update Organizations

- Ensure all organizations have `owner_user_id` populated
- If NULL, try to get from `organization_owners` table
- If still NULL, use first organization member with OWNER role
- Make `owner_user_id` NOT NULL

### Step 3: Update Pipeline Schemas

- Map old `source_connection_id` / `destination_connection_id` to new `data_source_id`
- Rename `org_id` to `organization_id`
- Remove `user_id` columns

### Step 4: Rename Tables

- `postgres_pipelines` → `pipelines`
- `postgres_pipeline_runs` → `pipeline_runs`
- `postgres_query_logs` → `query_logs`

## Migration Safety

- Migration uses transactions (BEGIN/COMMIT)
- Old tables are NOT dropped immediately (commented out in migration)
- Temporary mapping table created for data migration
- All foreign keys properly updated
- Indexes created for performance

## Rollback Plan

If migration fails:
1. Restore database from backup
2. Old tables remain intact (not dropped)
3. Use feature flag to switch between old/new schema
4. Test thoroughly in staging environment first

## Post-Migration Tasks

1. Verify data migration:
   ```sql
   SELECT COUNT(*) FROM data_sources;
   SELECT COUNT(*) FROM data_source_connections;
   ```

2. Update application code to use new schema names
3. Remove old schema files after verification
4. Update API endpoints to use new structure
5. Update frontend components

## Breaking Changes

- All references to `postgres_connections` must use `data_sources` + `data_source_connections`
- All references to `postgres_pipelines` must use `pipelines`
- All references to `postgres_pipeline_runs` must use `pipeline_runs`
- All references to `postgres_query_logs` must use `query_logs`
- Column names changed: `org_id` → `organization_id`, `user_id` → `created_by` (in pipelines)
- `organization_owners` table no longer exists

## Benefits

1. ✅ Multi-source support: Easy to add MySQL, MongoDB, S3, APIs, etc.
2. ✅ Organization-centric: Data sources belong to organizations, not users
3. ✅ Cleaner ownership: Single `owner_user_id` field instead of separate table
4. ✅ Flexible: Generic connection storage supports any database type
5. ✅ Scalable: Multiple data sources per organization
