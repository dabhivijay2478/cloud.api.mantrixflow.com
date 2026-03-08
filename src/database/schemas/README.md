# Database Schemas Organization

This directory contains all database schemas organized by domain for better code structure and maintainability.

## Structure

```
database/schemas/
├── activity-logs/           # Audit logging
├── data-pipelines/          # Pipeline configurations and runs
│   ├── destination-schemas/
│   ├── source-schemas/
│   ├── pipelines.schema.ts
│   └── pipeline-runs.schema.ts
├── data-sources/            # Data source connections
│   ├── data-sources.schema.ts
│   └── data-source-connections.schema.ts
├── organizations/           # Organization and membership
└── users/                   # User management
```

## Current Tables

| Domain | Tables |
|--------|--------|
| **Core** | `organizations`, `organization_members`, `users` |
| **Data Sources** | `data_sources`, `data_source_connections` |
| **Pipelines** | `pipelines`, `pipeline_runs`, `pipeline_source_schemas`, `pipeline_destination_schemas` |
| **Audit** | `activity_logs` |

## Schema Files

### Data Sources

- **data-sources.schema.ts** – Data source metadata
- **data-source-connections.schema.ts** – Connection config (JSONB), status, schema cache

### Data Pipelines

- **pipelines.schema.ts** – Pipeline config, sync settings, scheduling
- **pipeline-runs.schema.ts** – Run history, status, metrics
- **source-schemas/** – Source schema config, discovered columns
- **destination-schemas/** – Destination config, write mode, upsert key

## Import Usage

```typescript
import { dataSources, dataSourceConnections } from '@db/schema';
import { pipelines, pipelineRuns } from '@db/schema';
```

## Migration

1. Edit schema files
2. Run `bun run db:generate` to create migration
3. Run `bun run db:migrate` to apply
