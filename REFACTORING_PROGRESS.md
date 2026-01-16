# Database Refactoring Progress

## ✅ Completed

### 1. Schema Files
- ✅ Created `data-sources.schema.ts`
- ✅ Created `data-source-connections.schema.ts`
- ✅ Created `query-logs.schema.ts`
- ✅ Created `pipelines.schema.ts`
- ✅ Created `pipeline-runs.schema.ts`
- ✅ Updated `organizations.schema.ts` (owner_user_id NOT NULL)
- ✅ Updated `pipeline-source-schemas.schema.ts` (data_source_id, organization_id)
- ✅ Updated `pipeline-destination-schemas.schema.ts` (data_source_id, organization_id)
- ✅ Removed all legacy schema files

### 2. Migration
- ✅ Generated migration `0000_closed_centennial.sql`
- ✅ Migration executed successfully
- ✅ All tables created with proper relationships

### 3. Activity Logging
- ✅ Enhanced `ActivityLogService` with helper methods
- ✅ Added new action types for all operations
- ✅ Added helper methods: logDataSourceAction, logConnectionAction, logPipelineAction, etc.

### 4. Organization Services
- ✅ Updated `OrganizationService` - removed organization_owners references
- ✅ Updated `OrganizationRepository` - added findByOwnerUserId, isOwner methods
- ✅ Updated `OrganizationRoleService` - uses owner_user_id
- ✅ Updated `OrganizationRoleGuard` - uses owner_user_id
- ✅ Updated `OrganizationMemberService` - replaced console.error with Logger
- ✅ Updated `OrganizationModule` - removed OrganizationOwnerRepository

### 5. Data Source Services (NEW)
- ✅ Created `DataSourceService` - CRUD operations with activity logging
- ✅ Created `ConnectionService` - connection management with encryption
- ✅ Created `DataSourceRepository`
- ✅ Created `DataSourceConnectionRepository`

## ⏳ In Progress / Pending

### 6. Pipeline Services
- ⏳ Update `PostgresPipelineRepository` - use pipelines/pipeline_runs, organization_id
- ⏳ Update `PostgresPipelineService` - use new schema, add activity logging
- ⏳ Update `PipelineSourceSchemaRepository` - use data_source_id
- ⏳ Update `PipelineDestinationSchemaRepository` - use data_source_id

### 7. Controllers
- ⏳ Update `OrganizationController` - new endpoints, activity logging
- ⏳ Create `DataSourceController` - new endpoints for data sources
- ⏳ Create `ConnectionController` - connection management endpoints
- ⏳ Update `DataPipelineController` - use new schema, activity logging
- ⏳ Update `PostgresDataSourceController` - migrate to new structure

### 8. Remove Console Logs
- ⏳ Search and replace all console.log/error/warn/info/debug
- ⏳ Replace with Logger from @nestjs/common
- ⏳ Files to update: main.ts, user.service.ts, and others

### 9. Update Database Queries
- ⏳ Replace all `orgId` with `organizationId`
- ⏳ Replace all `postgresConnections` with `dataSources` + `dataSourceConnections`
- ⏳ Update all joins to use new relationships
- ⏳ Update all foreign key references

### 10. Update Repositories
- ⏳ Update all repositories to use new table names
- ⏳ Update column references (org_id → organization_id)
- ⏳ Update relationship queries

## Critical Files Still Needing Updates

1. **PostgresPipelineService** (2764 lines) - Major refactoring needed
   - Replace PostgresConnectionRepository with DataSourceConnectionRepository
   - Update all orgId to organizationId
   - Add activity logging throughout
   - Remove console.log statements

2. **PostgresPipelineRepository** - Update table names and columns
   - postgresPipelines → pipelines
   - postgresPipelineRuns → pipeline_runs
   - orgId → organizationId

3. **PostgresConnectionRepository** - Needs complete rewrite
   - Should work with data_source_connections table
   - Use JSONB config instead of individual fields

4. **All Controllers** - Update endpoints and add activity logging

5. **Search Handlers** - Already updated ✅

6. **Dashboard Service** - Already updated ✅

## Next Steps

1. Update PostgresPipelineRepository completely
2. Update PostgresPipelineService (large file, needs careful refactoring)
3. Create DataSourceController
4. Create ConnectionController
5. Update OrganizationController
6. Update DataPipelineController
7. Remove all console.log statements
8. Update all remaining queries
