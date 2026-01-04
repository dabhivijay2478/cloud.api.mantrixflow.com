# Source and Destination Schema Separation - Migration Guide

## Overview

The database schema has been reorganized to separate source and destination schemas into their own tables. This provides better organization, reusability, and clearer data flow.

## New Schema Structure

### New Tables

1. **`pipeline_source_schemas`** - Stores source schema configurations
   - Source connection, type, schema, table, query
   - Discovered columns, primary keys, foreign keys
   - Validation results

2. **`pipeline_destination_schemas`** - Stores destination schema configurations
   - Destination connection, schema, table
   - Column definitions, mappings, write mode
   - Validation results

### Updated Table

**`postgres_pipelines`** - Now references source and destination schemas
- `source_schema_id` (FK → `pipeline_source_schemas.id`)
- `destination_schema_id` (FK → `pipeline_destination_schemas.id`)
- Removed: `sourceType`, `sourceConnectionId`, `sourceConfig`, `sourceSchema`, `sourceTable`, `sourceQuery`
- Removed: `destinationConnectionId`, `destinationSchema`, `destinationTable`, `destinationTableExists`
- Removed: `columnMappings`, `writeMode`, `upsertKey`

## Migration Steps

### 1. Run the Migration

The migration SQL file has been created at:
```
src/database/drizzle/migrations/0002_separate_source_destination_schemas.sql
```

**To run manually:**
```bash
# Option 1: Using psql (if available)
psql $DATABASE_URL -f src/database/drizzle/migrations/0002_separate_source_destination_schemas.sql

# Option 2: Using Drizzle migrate (after updating journal)
bun run db:migrate

# Option 3: Execute SQL directly in your database client
```

### 2. Migration Process

The migration will:
1. Create `pipeline_source_schemas` table
2. Create `pipeline_destination_schemas` table
3. Add foreign key constraints
4. Add new columns to `postgres_pipelines` (nullable initially)
5. Migrate existing data from `postgres_pipelines` to new tables
6. Update `postgres_pipelines` to reference new schema tables
7. Make new columns NOT NULL
8. Add foreign key constraints

### 3. Code Updates Required

After migration, update your code to:

#### Old Way (Before):
```typescript
const pipeline = await createPipeline({
  name: 'My Pipeline',
  sourceType: 'postgres',
  sourceConnectionId: '...',
  sourceSchema: 'public',
  sourceTable: 'users',
  destinationConnectionId: '...',
  destinationSchema: 'public',
  destinationTable: 'users_dest',
  columnMappings: [...],
  writeMode: 'append',
});
```

#### New Way (After):
```typescript
// 1. Create source schema
const sourceSchema = await createSourceSchema({
  orgId: '...',
  userId: '...',
  sourceType: 'postgres',
  sourceConnectionId: '...',
  sourceSchema: 'public',
  sourceTable: 'users',
});

// 2. Create destination schema
const destSchema = await createDestinationSchema({
  orgId: '...',
  userId: '...',
  destinationConnectionId: '...',
  destinationSchema: 'public',
  destinationTable: 'users_dest',
  columnMappings: [...],
  writeMode: 'append',
});

// 3. Create pipeline with schema references
const pipeline = await createPipeline({
  name: 'My Pipeline',
  sourceSchemaId: sourceSchema.id,
  destinationSchemaId: destSchema.id,
});
```

### 4. Accessing Schema Data

#### Old Way:
```typescript
const sourceType = pipeline.sourceType;
const sourceTable = pipeline.sourceTable;
const columnMappings = pipeline.columnMappings;
```

#### New Way:
```typescript
// Option 1: Join query
const pipelineWithSchemas = await db
  .select()
  .from(postgresPipelines)
  .leftJoin(pipelineSourceSchemas, eq(postgresPipelines.sourceSchemaId, pipelineSourceSchemas.id))
  .leftJoin(pipelineDestinationSchemas, eq(postgresPipelines.destinationSchemaId, pipelineDestinationSchemas.id))
  .where(eq(postgresPipelines.id, pipelineId));

// Option 2: Separate queries
const sourceSchema = await getSourceSchema(pipeline.sourceSchemaId);
const destSchema = await getDestinationSchema(pipeline.destinationSchemaId);

const sourceType = sourceSchema.sourceType;
const sourceTable = sourceSchema.sourceTable;
const columnMappings = destSchema.columnMappings;
```

## Files That Need Updates

### Services
- `modules/data-pipelines/postgres-pipeline.service.ts`
- `modules/connectors/postgres/services/postgres-pipeline.service.ts` (old, should be removed)

### Controllers
- `modules/data-pipelines/data-pipeline.controller.ts`
- `modules/connectors/postgres/postgres.controller.ts` (old, should be removed)

### DTOs
- `modules/data-pipelines/dto/create-pipeline.dto.ts`
- `modules/connectors/postgres/dto/create-pipeline.dto.ts` (old, should be removed)

### Repositories
- `modules/data-pipelines/repositories/postgres-pipeline.repository.ts`

## Benefits

1. **Reusability**: Source and destination schemas can be reused across multiple pipelines
2. **Better Organization**: Clear separation of concerns
3. **Easier Management**: Schema changes don't require pipeline updates
4. **Better Validation**: Schemas can be validated independently
5. **Audit Trail**: Schema discovery and validation history

## Rollback

If you need to rollback:

1. Restore old columns to `postgres_pipelines`
2. Migrate data back from schema tables
3. Drop new tables
4. Revert code changes

## Next Steps

1. ✅ Schema files created
2. ✅ Migration SQL created
3. ⏳ Run migration (manual step)
4. ⏳ Update code to use new schema structure
5. ⏳ Update DTOs and services
6. ⏳ Remove old connector module files
7. ⏳ Test pipeline creation and execution

