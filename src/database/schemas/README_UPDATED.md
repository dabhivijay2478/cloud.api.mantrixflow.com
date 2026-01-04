# Database Schemas - Updated Structure

## New Structure with Separate Source/Destination Schemas

```
database/schemas/
├── data-sources/                    # Data source connection schemas
│   ├── connections/
│   │   └── postgres-connections.schema.ts
│   ├── query-logs/
│   │   └── postgres-query-logs.schema.ts
│   ├── sync-jobs/
│   │   └── postgres-sync-jobs.schema.ts
│   └── index.ts
│
├── data-pipelines/                  # Data pipeline schemas
│   ├── source-schemas/              # ✨ NEW: Source schema tables
│   │   └── pipeline-source-schemas.schema.ts
│   ├── destination-schemas/        # ✨ NEW: Destination schema tables
│   │   └── pipeline-destination-schemas.schema.ts
│   ├── pipelines/
│   │   └── postgres-pipelines.schema.ts    # Updated to reference schemas
│   ├── pipeline-runs/
│   │   └── postgres-pipeline-runs.schema.ts
│   └── index.ts
│
└── index.ts
```

## Key Changes

### 1. New Tables

#### `pipeline_source_schemas`
- Stores source configuration separately
- Includes discovered schema information
- Can be reused across multiple pipelines

#### `pipeline_destination_schemas`
- Stores destination configuration separately
- Includes column definitions and mappings
- Can be reused across multiple pipelines

### 2. Updated `postgres_pipelines`

**Removed columns:**
- `sourceType`, `sourceConnectionId`, `sourceConfig`, `sourceSchema`, `sourceTable`, `sourceQuery`
- `destinationConnectionId`, `destinationSchema`, `destinationTable`, `destinationTableExists`
- `columnMappings`, `writeMode`, `upsertKey`

**Added columns:**
- `sourceSchemaId` (FK → `pipeline_source_schemas.id`)
- `destinationSchemaId` (FK → `pipeline_destination_schemas.id`)

## Migration

See `SCHEMA_SEPARATION_MIGRATION.md` for detailed migration instructions.

## Usage

### Creating a Pipeline

```typescript
// 1. Create source schema
const sourceSchema = await createSourceSchema({
  orgId,
  userId,
  sourceType: 'postgres',
  sourceConnectionId: '...',
  sourceSchema: 'public',
  sourceTable: 'users',
});

// 2. Create destination schema
const destSchema = await createDestinationSchema({
  orgId,
  userId,
  destinationConnectionId: '...',
  destinationSchema: 'public',
  destinationTable: 'users_dest',
  columnMappings: [...],
  writeMode: 'append',
});

// 3. Create pipeline
const pipeline = await createPipeline({
  name: 'My Pipeline',
  sourceSchemaId: sourceSchema.id,
  destinationSchemaId: destSchema.id,
});
```

### Querying with Joins

```typescript
const pipelineWithSchemas = await db
  .select({
    pipeline: postgresPipelines,
    source: pipelineSourceSchemas,
    destination: pipelineDestinationSchemas,
  })
  .from(postgresPipelines)
  .leftJoin(pipelineSourceSchemas, eq(postgresPipelines.sourceSchemaId, pipelineSourceSchemas.id))
  .leftJoin(pipelineDestinationSchemas, eq(postgresPipelines.destinationSchemaId, pipelineDestinationSchemas.id))
  .where(eq(postgresPipelines.id, pipelineId));
```

## Benefits

1. **Separation of Concerns**: Source and destination configs are separate
2. **Reusability**: Schemas can be shared across pipelines
3. **Better Organization**: Clear structure for understanding data flow
4. **Easier Management**: Update schemas without touching pipelines
5. **Better Validation**: Validate schemas independently

