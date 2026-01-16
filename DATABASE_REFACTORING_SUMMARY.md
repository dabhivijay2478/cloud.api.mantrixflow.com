# Database Refactoring Summary

## Overview

This document summarizes the comprehensive database refactoring from a static PostgreSQL-only architecture to a flexible, multi-source data source architecture.

## Completed Changes

### ✅ 1. New Schema Files Created

**Data Sources:**
- `/apps/api/src/database/schemas/data-sources/data-sources.schema.ts`
  - Defines `data_sources` table (organization-level data source registry)
  - Fields: id, organization_id, name, description, source_type, is_active, metadata, created_by, timestamps, deleted_at

- `/apps/api/src/database/schemas/data-sources/data-source-connections.schema.ts`
  - Defines `data_source_connections` table (dynamic connection storage)
  - Uses JSONB `config` field to store connection details for any data source type
  - Includes TypeScript interfaces for different config types (PostgreSQL, MySQL, MongoDB, S3, API, BigQuery, Snowflake)

- `/apps/api/src/database/schemas/data-sources/query-logs.schema.ts`
  - Defines `query_logs` table (replaces `postgres_query_logs`)
  - References `data_sources` instead of connections

**Pipelines:**
- `/apps/api/src/database/schemas/data-pipelines/pipelines.schema.ts`
  - Defines `pipelines` table (renamed from `postgres_pipelines`)
  - Uses `organization_id` instead of `org_id`
  - Uses `created_by` instead of `user_id`
  - References `pipeline_source_schemas` and `pipeline_destination_schemas`

- `/apps/api/src/database/schemas/data-pipelines/pipeline-runs.schema.ts`
  - Defines `pipeline_runs` table (renamed from `postgres_pipeline_runs`)
  - Uses `organization_id` instead of `org_id`
  - Added `job_state` column

### ✅ 2. Updated Schema Files

**Organizations:**
- `/apps/api/src/database/schemas/organizations/organizations.schema.ts`
  - `owner_user_id` is now NOT NULL (required)
  - Foreign key constraint updated to RESTRICT on delete

**Pipeline Schemas:**
- `/apps/api/src/database/schemas/data-pipelines/source-schemas/pipeline-source-schemas.schema.ts`
  - Replaced `source_connection_id` with `data_source_id`
  - Renamed `org_id` to `organization_id`
  - Removed `user_id` column

- `/apps/api/src/database/schemas/data-pipelines/destination-schemas/pipeline-destination-schemas.schema.ts`
  - Replaced `destination_connection_id` with `data_source_id`
  - Renamed `org_id` to `organization_id`
  - Removed `user_id` column

### ✅ 3. Migration SQL

**File:** `/apps/api/src/database/drizzle/migrations/0013_refactor_to_dynamic_data_sources.sql`

**Migration Steps:**
1. Creates new enums (`connection_status_new`)
2. Creates `data_sources` and `data_source_connections` tables
3. Migrates data from `postgres_connections` to new structure
4. Updates `organizations` table (makes `owner_user_id` NOT NULL)
5. Updates `pipeline_source_schemas` (replaces connection_id with data_source_id)
6. Updates `pipeline_destination_schemas` (replaces connection_id with data_source_id)
7. Renames `postgres_pipelines` to `pipelines`
8. Renames `postgres_pipeline_runs` to `pipeline_runs`
9. Renames `postgres_query_logs` to `query_logs`
10. Drops old tables (`organization_owners`, `postgres_sync_jobs`)
11. Note: `postgres_connections` drop is commented out for safety

### ✅ 4. Updated Service Files

**Dashboard Service:**
- `/apps/api/src/modules/dashboard/dashboard.service.ts`
  - Updated to use `pipelineRuns` instead of `postgresPipelineRuns`
  - Updated column references: `orgId` → `organizationId`

**Search Handlers:**
- `/apps/api/src/modules/search/handlers/pipeline-search.handler.ts`
  - Updated to use `pipelines` instead of `postgresPipelines`
  - Updated column references: `orgId` → `organizationId`

- `/apps/api/src/modules/search/handlers/data-source-search.handler.ts`
  - Updated to use `dataSources` and `dataSourceConnections`
  - Joins data sources with connections to get config details
  - Uses `isNull` for soft delete check

- `/apps/api/src/modules/search/handlers/connector-search.handler.ts`
  - Updated to use `dataSources` and `dataSourceConnections`
  - Same changes as data-source-search handler

### ✅ 5. Updated Index Files

- `/apps/api/src/database/schemas/data-sources/index.ts`
  - Exports new schemas
  - Keeps legacy exports for backward compatibility

- `/apps/api/src/database/schemas/data-pipelines/index.ts`
  - Exports new schemas
  - Keeps legacy exports for backward compatibility

- `/apps/api/src/database/drizzle/schema/index.ts`
  - Exports from new unified schema structure
  - Keeps legacy exports for backward compatibility

## Schema Changes Summary

### Tables Created
- ✅ `data_sources`
- ✅ `data_source_connections`

### Tables Renamed
- ✅ `postgres_pipelines` → `pipelines`
- ✅ `postgres_pipeline_runs` → `pipeline_runs`
- ✅ `postgres_query_logs` → `query_logs`

### Tables Dropped
- ✅ `organization_owners` (merged into `organizations.owner_user_id`)
- ✅ `postgres_sync_jobs` (consolidated into `pipelines`)
- ⚠️ `postgres_connections` (migration commented out - drop after verification)

### Column Changes
- ✅ `organizations.owner_user_id` - Now NOT NULL
- ✅ `pipeline_source_schemas.source_connection_id` → `data_source_id`
- ✅ `pipeline_source_schemas.org_id` → `organization_id`
- ✅ `pipeline_source_schemas.user_id` - Removed
- ✅ `pipeline_destination_schemas.destination_connection_id` → `data_source_id`
- ✅ `pipeline_destination_schemas.org_id` → `organization_id`
- ✅ `pipeline_destination_schemas.user_id` - Removed
- ✅ `pipelines.org_id` → `organization_id`
- ✅ `pipelines.user_id` → `created_by`
- ✅ `pipeline_runs.org_id` → `organization_id`
- ✅ `query_logs.connection_id` → `data_source_id`

## Next Steps

### Before Running Migration

1. **Backup Database**
   ```bash
   pg_dump -h localhost -U postgres -d your_database > backup_before_migration.sql
   ```

2. **Review Migration SQL**
   - Check `/apps/api/src/database/drizzle/migrations/0013_refactor_to_dynamic_data_sources.sql`
   - Verify data mapping logic
   - Ensure all foreign keys are correct

3. **Test in Staging**
   - Run migration in staging environment first
   - Verify data migration correctness
   - Test application functionality

### After Running Migration

1. **Verify Data Migration**
   ```sql
   -- Check data source counts
   SELECT COUNT(*) FROM data_sources;
   SELECT COUNT(*) FROM data_source_connections;
   
   -- Verify pipeline updates
   SELECT COUNT(*) FROM pipelines;
   SELECT COUNT(*) FROM pipeline_runs;
   SELECT COUNT(*) FROM query_logs;
   
   -- Check for orphaned records
   SELECT COUNT(*) FROM pipeline_source_schemas WHERE data_source_id IS NULL;
   SELECT COUNT(*) FROM pipeline_destination_schemas WHERE data_source_id IS NULL;
   ```

2. **Update Application Code**
   - Update all repository files to use new schema names
   - Update all service files that reference old schemas
   - Update DTOs if needed
   - Update API endpoints

3. **Remove Legacy Code**
   - After verification, remove old schema files:
     - `postgres-connectors.schema.ts`
     - `postgres-pipeline.schema.ts`
     - Legacy pipeline schema files
   - Remove legacy exports from index files
   - Update any remaining references

4. **Drop Old Tables** (After Verification)
   ```sql
   DROP TABLE IF EXISTS "postgres_connections" CASCADE;
   ```

## Breaking Changes

### For Developers

1. **Schema Imports**
   ```typescript
   // OLD
   import { postgresConnections } from './database/schemas';
   import { postgresPipelines } from './database/schemas';
   
   // NEW
   import { dataSources, dataSourceConnections } from './database/schemas';
   import { pipelines } from './database/schemas';
   ```

2. **Column Names**
   ```typescript
   // OLD
   eq(postgresPipelines.orgId, orgId)
   
   // NEW
   eq(pipelines.organizationId, orgId)
   ```

3. **Connection Access**
   ```typescript
   // OLD
   const connection = await db.select().from(postgresConnections).where(...);
   
   // NEW
   const dataSource = await db
     .select()
     .from(dataSources)
     .leftJoin(dataSourceConnections, eq(dataSources.id, dataSourceConnections.dataSourceId))
     .where(...);
   ```

## Benefits

1. ✅ **Multi-source Support**: Easy to add MySQL, MongoDB, S3, APIs, BigQuery, Snowflake, etc.
2. ✅ **Organization-centric**: Data sources belong to organizations, not users
3. ✅ **Cleaner Ownership**: Single `owner_user_id` field instead of separate table
4. ✅ **Flexible**: Generic connection storage supports any database type via JSONB
5. ✅ **Scalable**: Multiple data sources per organization
6. ✅ **Better Separation**: Data sources separate from connection credentials

## Files Modified

### Schema Files
- ✅ `data-sources/data-sources.schema.ts` (NEW)
- ✅ `data-sources/data-source-connections.schema.ts` (NEW)
- ✅ `data-sources/query-logs.schema.ts` (NEW)
- ✅ `data-pipelines/pipelines.schema.ts` (NEW)
- ✅ `data-pipelines/pipeline-runs.schema.ts` (NEW)
- ✅ `organizations/organizations.schema.ts` (UPDATED)
- ✅ `data-pipelines/source-schemas/pipeline-source-schemas.schema.ts` (UPDATED)
- ✅ `data-pipelines/destination-schemas/pipeline-destination-schemas.schema.ts` (UPDATED)

### Migration Files
- ✅ `migrations/0013_refactor_to_dynamic_data_sources.sql` (NEW)
- ✅ `migrations/MIGRATION_0013_README.md` (NEW)

### Service Files
- ✅ `modules/dashboard/dashboard.service.ts` (UPDATED)
- ✅ `modules/search/handlers/pipeline-search.handler.ts` (UPDATED)
- ✅ `modules/search/handlers/data-source-search.handler.ts` (UPDATED)
- ✅ `modules/search/handlers/connector-search.handler.ts` (UPDATED)

### Index Files
- ✅ `schemas/data-sources/index.ts` (UPDATED)
- ✅ `schemas/data-pipelines/index.ts` (UPDATED)
- ✅ `drizzle/schema/index.ts` (UPDATED)

## Testing Checklist

- [ ] Run migration in staging environment
- [ ] Verify all data migrated correctly
- [ ] Test creating new data sources
- [ ] Test creating pipelines with new data sources
- [ ] Test pipeline runs
- [ ] Test search functionality
- [ ] Test dashboard overview
- [ ] Verify no orphaned records
- [ ] Check all foreign key constraints
- [ ] Verify soft delete functionality
- [ ] Test with multiple data source types

## Rollback Plan

If migration fails:

1. Restore database from backup
2. Old tables remain intact (not dropped)
3. Application can continue using old schema
4. Fix migration issues and retry

## Support

For questions or issues:
- Review migration SQL: `0013_refactor_to_dynamic_data_sources.sql`
- Check migration README: `MIGRATION_0013_README.md`
- Review schema documentation in each schema file
