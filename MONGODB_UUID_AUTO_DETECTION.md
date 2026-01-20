# MongoDB ObjectId to UUID Auto-Detection

## Summary
Added automatic detection and conversion of MongoDB ObjectId to UUID when mapping `_id` fields to PostgreSQL. The system now automatically:
1. Detects MongoDB `_id` fields that need UUID conversion
2. Applies `objectIdToUuid` transformation automatically
3. Sets `dataType: 'uuid'` for PostgreSQL compatibility
4. Drops and recreates existing tables with bigint columns to use UUID instead

## Changes Made

### 1. Auto-Detection in Transformer Service
**File**: `apps/api/src/modules/data-pipelines/services/transformer.service.ts`

- **New Method**: `enhanceColumnMappings()` - Automatically detects MongoDB `_id` fields and adds `objectIdToUuid` transformation
- **Detection Logic**: 
  - Checks if `sourceColumn === '_id'`
  - Verifies if value looks like MongoDB ObjectId (24 hex characters)
  - Automatically sets `transformation: 'objectIdToUuid'` and `dataType: 'uuid'`
- **Applied in**: `transform()` method uses enhanced mappings

### 2. Auto-Enhancement in Pipeline Service
**File**: `apps/api/src/modules/data-pipelines/services/pipeline.service.ts`

- **Line 487-489**: Auto-enhances column mappings for MongoDB sources before pipeline execution
- Ensures enhanced mappings are used throughout the pipeline (transformation, table creation, emission)

### 3. Auto-Enhancement in Emitter Service
**File**: `apps/api/src/modules/data-pipelines/services/emitter.service.ts`

- **Line 101-105**: Auto-enhances mappings in `emit()` method before transformation
- **Line 392-431**: Checks table schema before insert and drops/recreates if bigint column needs UUID
- Ensures table schema matches enhanced mappings

### 4. Table Migration Logic
**File**: `apps/api/src/modules/data-pipelines/services/emitter.service.ts`

- **Method**: `migratePostgresColumns()` - Checks existing table columns
- **Behavior**: 
  - If table has `bigint`/`integer` column for `_id` mapping with `objectIdToUuid` transformation
  - Automatically drops and recreates the table with UUID column
  - Only happens when transformation is detected (auto or manual)

## How It Works

### Automatic Flow:
1. **Pipeline starts** → Column mappings loaded from database
2. **Auto-enhancement** → MongoDB `_id` mappings get `transformation: 'objectIdToUuid'` and `dataType: 'uuid'`
3. **Data collection** → MongoDB documents collected (ObjectId strings)
4. **Transformation** → ObjectId strings converted to UUID v5 (deterministic)
5. **Table check** → If table exists with bigint, it's dropped and recreated with UUID
6. **Emission** → UUIDs inserted into PostgreSQL UUID column

### Example:
```typescript
// Original mapping (from database)
{
  sourceColumn: "_id",
  destinationColumn: "id",
  dataType: "bigint",  // ← Wrong type
  // No transformation
}

// After auto-enhancement
{
  sourceColumn: "_id",
  destinationColumn: "id",
  dataType: "uuid",  // ← Auto-corrected
  transformation: "objectIdToUuid",  // ← Auto-added
}
```

## Expected Behavior

### Logs:
```
Auto-detected MongoDB ObjectId for _id -> id, applying objectIdToUuid transformation
Transforming 3 rows with 2 column mappings
Column mappings: _id -> id (objectIdToUuid), username -> name
Sample transformed row (first): {
  "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",  // ← UUID, not ObjectId string
  "name": "vijay"
}
Table public.users has bigint column id but needs UUID. Dropping and recreating...
Created PostgreSQL table: "public"."users"
Emitted 3 rows to postgres destination
✅ Written: 3 | Skipped: 0 | Failed: 0
```

## Migration for Existing Tables

The system now automatically handles migration:
- **If table exists with bigint**: Automatically dropped and recreated with UUID
- **If table doesn't exist**: Created with UUID column from the start
- **No manual intervention needed**: Everything happens automatically

## Files Modified

1. `apps/api/src/modules/data-pipelines/services/transformer.service.ts`
   - Added `enhanceColumnMappings()` method
   - Auto-detection logic in `transform()`

2. `apps/api/src/modules/data-pipelines/services/pipeline.service.ts`
   - Auto-enhancement of mappings for MongoDB sources

3. `apps/api/src/modules/data-pipelines/services/emitter.service.ts`
   - Auto-enhancement in `emit()` method
   - Table schema validation and auto-migration in `emitToPostgres()`
   - Updated `migratePostgresColumns()` to drop and recreate tables

## Testing

1. **Run pipeline** with MongoDB source and PostgreSQL destination
2. **Check logs** for auto-detection message
3. **Verify** transformed data shows UUID format (not ObjectId string)
4. **Verify** table is created/recreated with UUID column type
5. **Verify** all 3 MongoDB documents are successfully written
