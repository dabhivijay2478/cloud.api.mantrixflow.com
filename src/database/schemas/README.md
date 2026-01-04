# Database Schemas Organization

This directory contains all database schemas organized by domain for better code structure and maintainability.

## Structure

```
database/schemas/
├── data-sources/              # Data source connection schemas
│   ├── connections/
│   │   └── postgres-connections.schema.ts
│   ├── query-logs/
│   │   └── postgres-query-logs.schema.ts
│   ├── sync-jobs/
│   │   └── postgres-sync-jobs.schema.ts
│   └── index.ts
│
├── data-pipelines/            # Data pipeline schemas
│   ├── pipelines/
│   │   └── postgres-pipelines.schema.ts    # Source & destination config
│   ├── pipeline-runs/
│   │   └── postgres-pipeline-runs.schema.ts
│   └── index.ts
│
└── index.ts                   # Central export point
```

## Schema Files

### Data Sources

#### `data-sources/connections/postgres-connections.schema.ts`
- **Table:** `postgres_connections`
- **Purpose:** Stores PostgreSQL connection configurations with encrypted credentials
- **Exports:** `postgresConnections`, `PostgresConnection`, `NewPostgresConnection`, `connectionStatusEnum`

#### `data-sources/query-logs/postgres-query-logs.schema.ts`
- **Table:** `postgres_query_logs`
- **Purpose:** Audit log for all queries executed against PostgreSQL connections
- **Exports:** `postgresQueryLogs`, `PostgresQueryLog`, `NewPostgresQueryLog`, `queryLogStatusEnum`
- **Dependencies:** References `postgresConnections`

#### `data-sources/sync-jobs/postgres-sync-jobs.schema.ts`
- **Table:** `postgres_sync_jobs`
- **Purpose:** Tracks data synchronization jobs from PostgreSQL sources to destinations
- **Exports:** `postgresSyncJobs`, `PostgresSyncJob`, `NewPostgresSyncJob`, `syncJobStatusEnum`, `syncFrequencyEnum`, `syncModeEnum`
- **Dependencies:** References `postgresConnections`

### Data Pipelines

#### `data-pipelines/pipelines/postgres-pipelines.schema.ts`
- **Table:** `postgres_pipelines`
- **Purpose:** Stores pipeline configurations with source and destination settings
- **Key Sections:**
  - **Source Configuration:** `sourceType`, `sourceConnectionId`, `sourceConfig`, `sourceSchema`, `sourceTable`, `sourceQuery`
  - **Destination Configuration:** `destinationConnectionId`, `destinationSchema`, `destinationTable`, `destinationTableExists`
  - **Schema Mapping:** `columnMappings`, `transformations`
  - **Write Configuration:** `writeMode`, `upsertKey`
  - **Sync Configuration:** `syncMode`, `incrementalColumn`, `syncFrequency`
- **Exports:** `postgresPipelines`, `PostgresPipeline`, `NewPostgresPipeline`, `writeModeEnum`, `pipelineStatusEnum`, `SourceConfig`, `ColumnMapping`, `Transformation`
- **Dependencies:** References `postgresConnections` (for both source and destination), imports `runStatusEnum` from pipeline-runs

#### `data-pipelines/pipeline-runs/postgres-pipeline-runs.schema.ts`
- **Table:** `postgres_pipeline_runs`
- **Purpose:** Tracks individual pipeline execution runs
- **Exports:** `postgresPipelineRuns`, `PostgresPipelineRun`, `NewPostgresPipelineRun`, `runStatusEnum`, `triggerTypeEnum`, `RunMetadata`
- **Dependencies:** References `postgresPipelines`

## Import Usage

### Using Path Alias (Recommended)
```typescript
import { postgresConnections, PostgresConnection } from '@db/schema';
import { postgresPipelines, PostgresPipeline } from '@db/schema';
```

### Using Direct Paths
```typescript
// Data Sources
import { postgresConnections } from '../../../database/schemas/data-sources/connections/postgres-connections.schema';
import { postgresQueryLogs } from '../../../database/schemas/data-sources/query-logs/postgres-query-logs.schema';

// Data Pipelines
import { postgresPipelines } from '../../../database/schemas/data-pipelines/pipelines/postgres-pipelines.schema';
import { postgresPipelineRuns } from '../../../database/schemas/data-pipelines/pipeline-runs/postgres-pipeline-runs.schema';
```

### Using Index Exports
```typescript
// All data sources
import * from '../../../database/schemas/data-sources';

// All data pipelines
import * from '../../../database/schemas/data-pipelines';

// Everything
import * from '../../../database/schemas';
```

## Schema Relationships

```
postgres_connections (1)
  ├──< (many) postgres_query_logs
  ├──< (many) postgres_sync_jobs
  ├──< (many) postgres_pipelines (as source_connection_id)
  └──< (many) postgres_pipelines (as destination_connection_id)

postgres_pipelines (1)
  └──< (many) postgres_pipeline_runs
```

## Benefits of This Structure

1. **Clear Separation:** Source and destination configurations are clearly separated in the pipeline schema
2. **Domain Organization:** Schemas are organized by domain (data-sources vs data-pipelines)
3. **Modularity:** Each table has its own schema file, making it easy to find and modify
4. **Scalability:** Easy to add new schema files for new tables or domains
5. **Type Safety:** Each schema file exports TypeScript types for the table
6. **Maintainability:** Clear folder structure makes it easy to understand the database structure

## Migration

When adding new schemas:
1. Create the schema file in the appropriate domain folder
2. Export from the domain's `index.ts`
3. Update the central `schemas/index.ts` if needed
4. Run `bun run db:generate` to create migration
5. Run `bun run db:migrate` to apply migration

## Notes

- All enums are defined in the schema file where they're first used
- Shared enums (like `runStatusEnum`) are imported from their defining schema
- Foreign key relationships are defined using Drizzle's `.references()` method
- All timestamps use PostgreSQL's `timestamp` type
- UUIDs are used for all primary keys and foreign keys

