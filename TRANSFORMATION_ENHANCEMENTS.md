# Data Pipeline Transformation Enhancements

## Overview
Enhanced the transformation and mapping logic to work correctly for all data sources: PostgreSQL, MySQL, MongoDB, S3, REST API, BigQuery, and Snowflake. Added visibility for mappings, improved logging, and ensured extensibility.

## Changes Made

### 1. New Source Handlers
- **BigQuery Handler** (`bigquery.handler.ts`): Handles schema discovery and data collection from Google BigQuery
- **Snowflake Handler** (`snowflake.handler.ts`): Handles schema discovery and data collection from Snowflake

### 2. Enhanced TransformerService
- Added `getMappedFieldsList()` method to extract mapping visibility
- Improved `getNestedValue()` to handle all data source types (S3 CSV, API JSON, BigQuery/Snowflake query results)
- Enhanced logging to show sample transformed data
- Better handling of nested paths using lodash as fallback

### 3. Updated PipelineService
- Added mapping visibility logging in `runPipeline()` and `dryRunPipeline()`
- Logs applied mappings with source→destination type information
- Shows sample transformed data in logs
- Returns `appliedMappings` in dry run results

### 4. Updated DTOs
- Added `appliedMappings` field to `PipelineResponseDto`
- Added `appliedMappings` and `transformedSample` to `DryRunResponseDto`
- Updated `DryRunResult` interface to include `appliedMappings`

### 5. Updated PipelineController
- `GET /:id` endpoint now includes `appliedMappings` in response
- Fetches destination data source to determine destination type

### 6. Handler Updates
- All handlers now set `isRelational`, `sourceType`, and `entityName` in `SchemaInfo`:
  - PostgreSQL: `isRelational: true, sourceType: 'postgres'`
  - MySQL: `isRelational: true, sourceType: 'mysql'`
  - MongoDB: `isRelational: false, sourceType: 'mongodb'`
  - S3: `isRelational: false, sourceType: 's3'`
  - API: `isRelational: false, sourceType: 'api'`
  - BigQuery: `isRelational: true, sourceType: 'bigquery'`
  - Snowflake: `isRelational: true, sourceType: 'snowflake'`

### 7. Mock Data Files
Created test mock data files in `apps/api/test/mocks/`:
- `mock-postgres-rows.json`: Flat relational data
- `mock-mongodb-docs.json`: Nested documents with arrays
- `mock-s3-csv-rows.json`: CSV-like parsed rows
- `mock-api-response.json`: Paginated JSON API response
- `mock-bigquery-rows.json`: BigQuery-style event data
- `mock-snowflake-rows.json`: Snowflake-style customer data

## Key Features

### Mapping Visibility
- Mappings are now logged with format: `sourcePath → destPath`
- Sample transformed data is logged for debugging
- API responses include `appliedMappings` field

### Extensibility
- All handlers follow the same pattern
- Adding a new source type doesn't break existing functionality
- Handler registry automatically includes new handlers

### Bidirectional Support
- MongoDB ↔ PostgreSQL transformations work correctly
- Nested objects are flattened for SQL destinations
- Arrays are unwound with foreign key references
- SQL data is embedded into nested structures for NoSQL

## Testing
Use the mock data files to test transformations:
```typescript
// Example: Test MongoDB → PostgreSQL
const mongoDocs = require('./test/mocks/mock-mongodb-docs.json');
const mappings = [
  {
    sourcePath: 'address.city',
    destPath: 'city',
    transformation: 'flattenObject'
  },
  {
    sourcePath: 'orders',
    destPath: 'order_items',
    isArray: true,
    foreignKey: 'user_id'
  }
];
```

## Logging Examples
```
Mapping applied to mongodb→postgres: 5 fields transformed
Mapped fields: email → user_email, address.city → city, orders → order_items, ...
Sample transformed row (first): { "user_email": "...", "city": "...", ... }
```

## API Response Example
```json
{
  "id": "pipeline-123",
  "name": "MongoDB to PostgreSQL",
  "appliedMappings": [
    { "sourcePath": "email", "destPath": "user_email" },
    { "sourcePath": "address.city", "destPath": "city" }
  ]
}
```
