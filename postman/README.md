# Postman Collections for MantrixFlow API

This directory contains Postman collections for testing the MantrixFlow API endpoints.

## Collections

### 1. Data Sources - PostgreSQL API
**File:** `Data-Sources-PostgreSQL-API.postman_collection.json`

Complete collection for PostgreSQL data source management including:
- **Connection Management**: Test, create, list, update, and delete connections
- **Schema Discovery**: Discover databases, schemas, tables, and table schemas
- **Query Execution**: Execute queries and explain query plans
- **Sync Jobs**: Create, list, manage, and schedule data synchronization jobs
- **Monitoring**: Check connection health, view query logs, and get metrics

**Base URL:** `/api/data-sources/postgres`

### 2. Data Pipelines API
**File:** `Data-Pipelines-API.postman_collection.json`

Complete collection for data pipeline management including:
- **Pipeline Management**: Create, list, get, update, and delete pipelines
- **Pipeline Execution**: Execute, dry-run, and validate pipelines
- **Pipeline Control**: Pause and resume pipelines
- **Pipeline Configuration**: Auto-map columns
- **Pipeline Monitoring**: View run history, run details, and statistics

**Base URL:** `/api/data-pipelines`

## Setup Instructions

### 1. Import Collections

1. Open Postman
2. Click **Import** button
3. Select both collection files:
   - `Data-Sources-PostgreSQL-API.postman_collection.json`
   - `Data-Pipelines-API.postman_collection.json`

### 2. Configure Environment Variables

Create a Postman environment or use the collection variables:

**Required Variables:**
- `base_url`: API base URL (default: `http://localhost:3000`)
- `auth_token`: JWT authentication token
- `org_id`: Organization ID (UUID)

**Data Sources Variables:**
- `connection_id`: Connection ID (set automatically after creating a connection)
- `sync_job_id`: Sync job ID (set automatically after creating a sync job)

**Data Pipelines Variables:**
- `pipeline_id`: Pipeline ID (set automatically after creating a pipeline)
- `pipeline_run_id`: Pipeline run ID (set automatically after executing a pipeline)
- `source_connection_id`: Source connection ID for pipeline
- `destination_connection_id`: Destination connection ID for pipeline

### 3. Authentication

1. Obtain a JWT token from your authentication endpoint
2. Set the `auth_token` variable in your Postman environment
3. All requests include the token in the `Authorization` header automatically

### 4. Testing Workflow

#### Data Sources Workflow:
1. **Test Connection** - Verify PostgreSQL connection credentials
2. **Create Connection** - Save connection (automatically sets `connection_id`)
3. **Discover Schemas** - Explore available schemas and tables
4. **Execute Query** - Test query execution
5. **Create Sync Job** - Set up data synchronization (automatically sets `sync_job_id`)
6. **Monitor** - Check health, logs, and metrics

#### Data Pipelines Workflow:
1. **Create Pipeline** - Set up a data pipeline (automatically sets `pipeline_id`)
2. **Validate Pipeline** - Verify configuration
3. **Dry Run** - Test without writing data
4. **Execute Pipeline** - Run the pipeline (automatically sets `pipeline_run_id`)
5. **Monitor** - View runs and statistics

## Collection Features

### Automatic Variable Setting
Both collections include test scripts that automatically set variables:
- Connection IDs are saved after creating connections
- Pipeline IDs are saved after creating pipelines
- Run IDs are saved after executing pipelines

### Test Scripts
Collections include automated tests for:
- Status code validation
- Response structure validation
- Automatic variable extraction

### Organized Structure
Collections are organized into logical folders:
- Connection Management
- Schema Discovery
- Query Execution
- Sync Jobs
- Monitoring
- Pipeline Management
- Pipeline Execution
- Pipeline Control
- Pipeline Configuration
- Pipeline Monitoring

## Example Requests

### Test Connection
```json
POST /api/data-sources/postgres/test-connection
{
  "host": "localhost",
  "port": 5432,
  "database": "mydb",
  "username": "postgres",
  "password": "password123",
  "ssl": {
    "enabled": false
  }
}
```

### Create Pipeline
```json
POST /api/data-pipelines
{
  "name": "Users Pipeline",
  "sourceType": "postgres",
  "sourceConnectionId": "{{source_connection_id}}",
  "sourceTable": "users",
  "destinationConnectionId": "{{destination_connection_id}}",
  "destinationTable": "users_synced",
  "writeMode": "append",
  "syncMode": "full"
}
```

## API Endpoints Summary

### Data Sources API (`/api/data-sources/postgres`)
- `POST /test-connection` - Test connection
- `POST /connections` - Create connection
- `GET /connections` - List connections
- `GET /connections/:id` - Get connection
- `PATCH /connections/:id` - Update connection
- `DELETE /connections/:id` - Delete connection
- `GET /connections/:id/databases` - Discover databases
- `GET /connections/:id/schemas` - Discover schemas
- `GET /connections/:id/tables` - Discover tables
- `GET /connections/:id/tables/:table/schema` - Get table schema
- `POST /connections/:id/refresh-schema` - Refresh schema cache
- `POST /connections/:id/query` - Execute query
- `POST /connections/:id/query/explain` - Explain query
- `POST /connections/:id/sync` - Create sync job
- `GET /connections/:id/sync-jobs` - List sync jobs
- `GET /connections/:id/sync-jobs/:jobId` - Get sync job
- `POST /connections/:id/sync-jobs/:jobId/cancel` - Cancel sync job
- `PATCH /connections/:id/sync-jobs/:jobId/schedule` - Update schedule
- `GET /connections/:id/health` - Get health
- `GET /connections/:id/query-logs` - Get query logs
- `GET /connections/:id/metrics` - Get metrics

### Data Pipelines API (`/api/data-pipelines`)
- `POST /` - Create pipeline
- `GET /` - List pipelines
- `GET /:id` - Get pipeline
- `PATCH /:id` - Update pipeline
- `DELETE /:id` - Delete pipeline
- `POST /:id/run` - Execute pipeline
- `POST /:id/dry-run` - Dry run pipeline
- `POST /:id/pause` - Pause pipeline
- `POST /:id/resume` - Resume pipeline
- `POST /:id/validate` - Validate pipeline
- `POST /:id/auto-map` - Auto-map columns
- `GET /:id/runs` - Get pipeline runs
- `GET /:id/runs/:runId` - Get pipeline run
- `GET /:id/stats` - Get pipeline statistics

## Notes

- All endpoints require authentication via Bearer token
- Organization ID (`orgId`) is required as a query parameter for most endpoints
- Connection IDs, Pipeline IDs, and Run IDs are automatically captured and stored in variables
- Collections include example request bodies with realistic data
- Test scripts validate responses and extract IDs automatically

## Support

For issues or questions:
1. Check the API documentation at `/api/docs` (Swagger UI)
2. Review the controller files in the codebase
3. Check server logs for detailed error messages

