# Source Handlers

This directory contains the Strategy Pattern implementation for data source handling.

## Architecture

Each data source type has its own handler that implements the `ISourceHandler` interface:

```typescript
interface ISourceHandler {
  readonly type: DataSourceType;
  testConnection(connectionConfig: any): Promise<ConnectionTestResult>;
  discoverSchema(sourceSchema, connectionConfig): Promise<SchemaInfo>;
  collect(sourceSchema, connectionConfig, params): Promise<CollectResult>;
  collectStream?(sourceSchema, connectionConfig, params): AsyncIterable<any[]>;
}
```

## Current Handlers

| Handler         | File                  | Data Source Types              |
| --------------- | --------------------- | ------------------------------ |
| PostgresHandler | `postgres.handler.ts` | PostgreSQL, PgVector, Supabase |
| MySQLHandler    | `mysql.handler.ts`    | MySQL, MariaDB                 |
| MongoDBHandler  | `mongodb.handler.ts`  | MongoDB, DocumentDB            |
| S3Handler       | `s3.handler.ts`       | AWS S3 (CSV/JSON files)        |
| APIHandler      | `api.handler.ts`      | REST APIs                      |

## Adding a New Source Handler

### 1. Create the Handler File

Create a new file: `{source-name}.handler.ts`

```typescript
import { Logger } from '@nestjs/common';
import { DataSourceType } from '../../types/common.types';
import {
  BaseSourceHandler,
  CollectParams,
  CollectResult,
  ConnectionTestResult,
  PipelineSourceSchemaWithConfig,
  SchemaInfo,
} from '../../types/source-handler.types';

export class NewSourceHandler extends BaseSourceHandler {
  readonly type = DataSourceType.NEW_SOURCE; // Add to enum first
  private readonly logger = new Logger(NewSourceHandler.name);

  async testConnection(connectionConfig: any): Promise<ConnectionTestResult> {
    // Implementation
  }

  async discoverSchema(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
  ): Promise<SchemaInfo> {
    // Implementation
  }

  async collect(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    params: CollectParams,
  ): Promise<CollectResult> {
    // Implementation
  }
}
```

### 2. Add to DataSourceType Enum

In `types/common.types.ts`:

```typescript
export enum DataSourceType {
  // ... existing types
  NEW_SOURCE = 'new_source',
}
```

### 3. Export from Index

In `handlers/index.ts`:

```typescript
export { NewSourceHandler } from './new-source.handler';
```

### 4. Register in Handler Registry

In `handlers/handler-registry.ts`:

```typescript
import { NewSourceHandler } from './new-source.handler';

export function createHandlerRegistry(httpService: HttpService): SourceHandlerRegistry {
  const handlers: ISourceHandler[] = [
    // ... existing handlers
    new NewSourceHandler(),
  ];
  // ...
}
```

### 5. Add Frontend Form Fields

In `apps/app/components/data-sources/constants.ts`, add the connection schema for the new source type.

## Testing

Each handler should have unit tests:

```typescript
describe('NewSourceHandler', () => {
  let handler: NewSourceHandler;

  beforeEach(() => {
    handler = new NewSourceHandler();
  });

  describe('testConnection', () => {
    it('should return success for valid config', async () => {
      const result = await handler.testConnection({
        // mock config
      });
      expect(result.success).toBe(true);
    });
  });

  // ... more tests
});
```

## Error Handling

All handlers should:

1. Use `try/catch` blocks around external operations
2. Close connections in `finally` blocks
3. Use the `withRetry` helper for retryable operations
4. Log errors with the handler's logger
5. Throw typed errors for callers to handle

## Best Practices

1. **Connection Management**: Always close connections in `finally` blocks
2. **Streaming**: Implement `collectStream` for large datasets
3. **Type Normalization**: Use `normalizeDataType` for consistent column types
4. **Pagination**: Support cursor, offset, and page-based pagination
5. **Rate Limiting**: Respect API rate limits in `collectStream`
6. **Incremental Sync**: Support `incrementalColumn` and `lastSyncValue`
