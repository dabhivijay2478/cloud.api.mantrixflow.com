# MongoDB ObjectId to UUID Migration Guide

## Issue
When syncing MongoDB `_id` (ObjectId) to PostgreSQL, the system was trying to insert ObjectId strings into a `bigint` column, causing:
```
error: invalid input syntax for type bigint: "666ee7b2a6e3a02624d2f6fb"
```

## Solution
Convert MongoDB ObjectId to UUID using the `objectIdToUuid` transformation.

## Changes Made

### 1. Enhanced `objectIdToUuid` Transformation
- **File**: `apps/api/src/modules/data-pipelines/services/transformer.service.ts`
- **Change**: Now generates a proper UUID v5 (deterministic) from MongoDB ObjectId
- **Method**: Uses SHA-1 hashing to create a deterministic UUID from the ObjectId string
- **Result**: Same ObjectId always generates the same UUID

### 2. Transformation Applied Before Type Casting
- **File**: `apps/api/src/modules/data-pipelines/services/transformer.service.ts`
- **Change**: Transformations are now applied BEFORE `castValue()` to ensure proper conversion
- **Impact**: MongoDB ObjectId strings are converted to UUIDs before being cast to PostgreSQL UUID type

### 3. Auto-Detection of UUID Type in Table Creation
- **File**: `apps/api/src/modules/data-pipelines/services/emitter.service.ts`
- **Change**: When `transformation: 'objectIdToUuid'` is set, the table creation automatically uses `UUID` type instead of `bigint`
- **Impact**: New tables will be created with UUID columns for ObjectId mappings

## How to Use

### Step 1: Update Column Mapping
Update your destination schema's column mapping to include the transformation:

```json
{
  "sourceColumn": "_id",
  "destinationColumn": "id",
  "dataType": "uuid",
  "transformation": "objectIdToUuid",
  "nullable": false,
  "isPrimaryKey": true
}
```

### Step 2: Migrate Existing Table (if needed)

If your table already exists with a `bigint` column, you have two options:

#### Option A: Drop and Recreate Table (Recommended for empty/new tables)
```sql
DROP TABLE IF EXISTS "public"."users";
-- The pipeline will recreate it with UUID type on next run
```

#### Option B: Manual Migration (for tables with existing data)
```sql
-- Step 1: Add new UUID column
ALTER TABLE "public"."users" ADD COLUMN "id_uuid" UUID;

-- Step 2: Convert existing ObjectId strings to UUIDs (if you have them stored)
-- Note: This requires custom logic to convert each ObjectId to UUID
-- You may need to re-sync from MongoDB to populate UUIDs correctly

-- Step 3: Drop old column and rename new column
ALTER TABLE "public"."users" DROP COLUMN "id";
ALTER TABLE "public"."users" RENAME COLUMN "id_uuid" TO "id";
ALTER TABLE "public"."users" ADD PRIMARY KEY ("id");
```

**Note**: For existing data, the best approach is to:
1. Drop the existing table
2. Update the column mapping to include `transformation: 'objectIdToUuid'` and `dataType: 'uuid'`
3. Re-run the pipeline to recreate the table and sync data with UUIDs

### Step 3: Update Destination Schema via API

Update your destination schema's column mappings:

```bash
PATCH /api/organizations/{orgId}/destination-schemas/{schemaId}
{
  "columnMappings": [
    {
      "sourceColumn": "_id",
      "destinationColumn": "id",
      "dataType": "uuid",
      "transformation": "objectIdToUuid",
      "nullable": false,
      "isPrimaryKey": true
    },
    {
      "sourceColumn": "username",
      "destinationColumn": "name",
      "dataType": "text",
      "nullable": true
    }
  ]
}
```

## Expected Behavior After Fix

1. **MongoDB ObjectId** (`666ee7b2a6e3a02624d2f6fb`) 
   ↓ (transformation: objectIdToUuid)
2. **UUID** (`a1b2c3d4-e5f6-7890-abcd-ef1234567890`)
   ↓ (cast to uuid type)
3. **PostgreSQL UUID Column** (stored as UUID type)

## Example Logs After Fix

```
MongoDB collect: database=Vlearn, collection=users
MongoDB found 3 documents
Transforming 3 rows with 2 column mappings
Column mappings: _id -> id, username -> name
Sample transformed row (first): {
  "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "name": "vijay"
}
Emitted 3 rows to postgres destination
✅ Written: 3 | Skipped: 0 | Failed: 0
```

## Files Modified

1. `apps/api/src/modules/data-pipelines/services/transformer.service.ts`
   - Updated `objectIdToUuid` to generate proper UUID v5
   - Applied transformations before type casting

2. `apps/api/src/modules/data-pipelines/services/emitter.service.ts`
   - Auto-detects UUID type when `objectIdToUuid` transformation is used
   - Creates tables with UUID columns instead of bigint

## Testing

1. **Update Column Mapping**: Add `transformation: 'objectIdToUuid'` and `dataType: 'uuid'` to your `_id → id` mapping
2. **Drop Existing Table**: If table exists with bigint, drop it first
3. **Run Pipeline**: The pipeline will create the table with UUID type and sync data
4. **Verify**: Check that all 3 MongoDB records are synced with UUID primary keys
