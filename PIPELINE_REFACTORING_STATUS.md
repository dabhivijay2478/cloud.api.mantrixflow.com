# Pipeline Refactoring Status

## Completed ✅

1. **Renamed Files:**
   - `postgres-pipeline.service.ts` → `pipeline.service.ts`
   - `postgres-pipeline.repository.ts` → `pipeline.repository.ts`

2. **Renamed Classes:**
   - `PostgresPipelineService` → `PipelineService`
   - `PostgresPipelineRepository` → `PipelineRepository`

3. **Updated All Imports:**
   - Updated all references across the codebase
   - Updated module providers and exports

4. **Created Generic Types:**
   - Created `/modules/data-pipelines/types/pipeline.types.ts`
   - Moved all common types from postgres-specific location
   - Updated all type imports

5. **Removed Postgres Module Dependencies:**
   - Removed `PostgresDataSourceModule` from `app.module.ts`
   - Removed `PostgresDataSourceModule` from `data-pipeline.module.ts`

6. **Fixed Type Issues:**
   - Added missing properties to `DryRunResult` (sourceRowCount)
   - Added missing properties to `ColumnMapping` (defaultValue)
   - Added missing properties to `SchemaValidationResult` (errors)
   - Added missing properties to `TableStats` (lastUpdated, indexCount)
   - Added missing types: `TypeMismatch`, `TypeInferenceResult`, `ValidationError`

7. **Fixed Organization Service:**
   - Added email field when creating member for ownership transfer

## Remaining Work ⚠️

### Critical: Postgres-Specific Services Still Referenced

The `PipelineService` still has dependencies on postgres-specific services that need to be abstracted:

1. **PostgresConnectionPoolService** - Used in multiple places:
   - Line 360, 363: `createPipeline` method
   - Line 1589, 1591: `readFromSource` method
   - Line 1694, 1696: Other source reading methods
   - Line 1821, 1828: `readFromSource` method
   - Line 2071, 2076: `resolveAndPrepareDestinationTable` method
   - Line 2340, 2345: Other methods
   - Line 2671, 2673: `deletePipeline` method

2. **PostgresQueryExecutorService** - Referenced but may not be actively used

### Recommended Next Steps

1. **Create Connection Pool Abstraction:**
   - Create a generic `ConnectionPoolService` interface
   - Implement postgres-specific version in `data-sources/postgres/services/`
   - Use dependency injection to provide the correct implementation based on data source type

2. **Organize by Data Source Type:**
   - Move postgres-specific services to `data-sources/postgres/services/`
   - Create generic interfaces in `data-pipelines/interfaces/`
   - Use factory pattern to get the right service based on data source type

3. **Update Pipeline Service:**
   - Inject connection pool service based on destination data source type
   - Use strategy pattern for different data source types
   - Abstract away postgres-specific logic

### Files That Still Need Postgres Services

- `pipeline.service.ts` - Main service (needs abstraction)
- `emitters/postgres-destination.service.ts` - Postgres-specific emitter (should be in postgres folder)
- `transformers/postgres-schema-mapper.service.ts` - Postgres-specific mapper (should be in postgres folder)
- `shared/jobs/postgres-pipeline.processor.ts` - Postgres-specific processor (should be in postgres folder)
- `shared/postgres-pipeline-queue.service.ts` - Queue service (can be generic)

### Folder Structure Recommendation

```
data-pipelines/
  ├── pipeline.service.ts (generic)
  ├── repositories/
  │   └── pipeline.repository.ts (generic)
  ├── types/
  │   └── pipeline.types.ts (generic types)
  ├── interfaces/
  │   ├── connection-pool.interface.ts
  │   ├── destination-service.interface.ts
  │   └── schema-mapper.interface.ts
  ├── emitters/
  │   └── destination.service.ts (generic interface)
  ├── transformers/
  │   └── schema-mapper.service.ts (generic interface)
  └── shared/
      ├── pipeline-queue.service.ts (generic)
      └── jobs/
          └── pipeline.processor.ts (generic)

data-sources/
  └── postgres/
      └── services/
          ├── postgres-connection-pool.service.ts
          ├── postgres-query-executor.service.ts
          ├── postgres-destination.service.ts (implementation)
          └── postgres-schema-mapper.service.ts (implementation)
```

## Current Build Status

The build currently fails because:
1. `PostgresConnectionPoolService` and `PostgresQueryExecutorService` are not found
2. These services need to be created or the code needs to be refactored to not use them directly

## Next Actions

1. Create postgres-specific services in `data-sources/postgres/services/`
2. Create generic interfaces for connection pool and query executor
3. Refactor `PipelineService` to use dependency injection for these services
4. Move postgres-specific implementations to appropriate folders
5. Update module to provide correct implementations based on data source type
