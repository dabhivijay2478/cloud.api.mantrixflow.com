# MongoDB Collection Fix - 0 Rows Collected Issue

## Issue
When running a MongoDB → PostgreSQL pipeline, the system was collecting 0 rows even though there were 3 records in the MongoDB collection. The logs showed:
```
Collected 0 rows from mongodb source
```

## Root Cause
The MongoDB collection methods were using `connectionConfig.database` for the database name, but when the source schema is created from the UI, the database name is stored in `sourceSchema.sourceSchema` (not in the connection config).

For MongoDB:
- **Database name** is stored in `sourceSchema.sourceSchema` (e.g., "Vlearn")
- **Collection name** is stored in `sourceSchema.sourceTable` (e.g., "users")

But the code was using:
- `connectionConfig.database` for database (which might be different or undefined)
- `sourceSchema.sourceTable` for collection (this was correct)

## Fixes Applied

### 1. Fixed `collectFromMongoDB` Method (`collector.service.ts`)
- **Before**: Used `connectionConfig.database` for database name
- **After**: Uses `sourceSchema.sourceSchema || connectionConfig.database` (checks source schema first)
- Added validation to ensure both database and collection names are present
- Added logging to show which database/collection is being queried
- Fixed cursor-based pagination to properly handle ObjectId conversion

### 2. Fixed `discoverMongoDBSchema` Method (`collector.service.ts`)
- **Before**: Used `connectionConfig.database` for database name
- **After**: Uses `sourceSchema.sourceSchema || connectionConfig.database`
- Added validation and logging

### 3. Enhanced MongoDB Handler (`mongodb.handler.ts`)
- Updated `collect()` method to check both `sourceSchema.sourceSchema` and `sourceSchema.config.database`
- Updated `discoverSchema()` method similarly
- Updated `collectStream()` method similarly
- Added proper error messages when database/collection names are missing
- Fixed log message in `collect()` method (was incorrectly saying "discoverSchema")

## Code Changes

**File**: `apps/api/src/modules/data-pipelines/services/collector.service.ts`
- Line 593: Changed to use `sourceSchema.sourceSchema || connectionConfig.database`
- Line 676: Changed to use `sourceSchema.sourceSchema || connectionConfig.database`

**File**: `apps/api/src/modules/data-pipelines/services/handlers/mongodb.handler.ts`
- Lines 94-112: Enhanced database/collection name resolution
- Lines 187-205: Enhanced database/collection name resolution
- Lines 290-308: Enhanced database/collection name resolution

## Testing
To verify the fix works:

1. **Create MongoDB Source Schema**:
   - Database: "Vlearn" (stored in `sourceSchema`)
   - Collection: "users" (stored in `sourceTable`)

2. **Run Pipeline**:
   - Should now collect all 3 documents from MongoDB
   - Logs should show: `MongoDB collect: database=Vlearn, collection=users`
   - Logs should show: `MongoDB found 3 documents`

3. **Check Destination**:
   - All 3 records should be written to PostgreSQL destination
   - Fields should be mapped correctly (`_id → id`, `username → name`)

## Expected Logs After Fix
```
MongoDB collect: database=Vlearn, collection=users
MongoDB query: {}, limit=1000, skip=0
MongoDB found 3 documents
Collected 3 rows from mongodb source
```

## Files Modified
- `apps/api/src/modules/data-pipelines/services/collector.service.ts`
- `apps/api/src/modules/data-pipelines/services/handlers/mongodb.handler.ts`
