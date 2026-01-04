# Cursor Prompt: PostgreSQL Connector from Scratch for MantrixFlow

```markdown
# MantrixFlow - Build PostgreSQL Connector from Scratch (No Airbyte)

## Project Context
You are building a custom PostgreSQL connector for MantrixFlow, a Generative AI BI platform. This connector will allow users to connect their PostgreSQL databases and generate AI-powered dashboards from their data.

## Technology Stack
- **Backend**: NestJS with Express (TypeScript)
- **Database**: Supabase (PostgreSQL) for storing connector metadata
- **Package Manager**: bun
- **ORM**: Drizzle ORM
- **Frontend**: Next.js 14 App Router
- **UI**: Shadcn/UI + Tailwind CSS

## Project Structure
```
apps/
├── api/                              ← NestJS Backend
│   └── src/
│       ├── modules/
│       │   └── connectors/
│       │       ├── postgres/         ← NEW: PostgreSQL connector
│       │       │   ├── postgres.module.ts
│       │       │   ├── postgres.service.ts
│       │       │   ├── postgres.controller.ts
│       │       │   ├── postgres.types.ts
│       │       │   ├── postgres.validator.ts
│       │       │   ├── postgres.schema-discovery.ts
│       │       │   ├── postgres.query-executor.ts
│       │       │   ├── postgres.connection-pool.ts
│       │       │   └── postgres.sync-scheduler.ts
│       │       └── connectors.module.ts
│       └── database/
│           └── schema.ts             ← Drizzle schema
└── app/                              ← Next.js Frontend
    └── src/
        └── components/
            └── connectors/
                └── postgres-setup.tsx ← NEW: UI for setup

packages/
└── types/
    └── connectors.ts                 ← Shared types
```
```

## Requirements

### Core Functionality

#### 1. Connection Management
Create a robust PostgreSQL connection system that:
- Accepts connection parameters (host, port, database, username, password)
- Validates credentials before saving
- Tests connection health
- Supports SSL/TLS connections
- Handles connection pooling (max 10 connections per user)
- Implements connection timeout (30 seconds)
- Supports SSH tunneling for secure connections
- Handles multiple PostgreSQL versions (10+)

#### 2. Schema Discovery
Build a schema discovery system that:
- Lists all accessible databases
- Discovers all schemas (public, custom schemas)
- Lists all tables with metadata:
  - Table name
  - Row count (estimate)
  - Column names
  - Column data types (map PostgreSQL types to TypeScript types)
  - Primary keys
  - Foreign keys
  - Indexes
  - Table size (MB)
  - Last updated timestamp
- Detects views and materialized views
- Identifies partitioned tables
- Caches schema for 1 hour to reduce queries

#### 3. Query Execution Engine
Implement a secure query executor that:
- Executes SELECT queries only (read-only access)
- Prevents SQL injection using parameterized queries
- Implements query timeout (60 seconds default, configurable)
- Handles large result sets (pagination: 10,000 rows max per page)
- Streams results for memory efficiency
- Supports query explain plans for optimization
- Logs all queries for audit
- Handles query cancellation
- Implements rate limiting (100 queries/hour per user)

#### 4. Data Synchronization
Create a sync system that:
- Pulls data from PostgreSQL tables to Supabase
- Supports full sync (all data) and incremental sync (new/changed rows)
- Detects incremental sync column (timestamp or auto-increment ID)
- Handles large tables (chunked sync: 1,000 rows per batch)
- Tracks sync state (last sync timestamp, last synced ID)
- Implements retry logic (3 attempts with exponential backoff)
- Provides real-time sync progress updates (WebSocket)
- Handles schema changes (add/remove columns)
- Supports custom WHERE clauses for filtering
- Schedules automatic syncs (configurable: 15min, 1hr, 24hr)

#### 5. Security & Permissions
Implement security features:
- Encrypt credentials at rest (AES-256)
- Store credentials in Supabase with row-level security
- Validate user has SELECT permissions on tables
- Detect and block dangerous queries (DROP, DELETE, UPDATE, etc.)
- Implement IP whitelisting (optional)
- Support read-only PostgreSQL roles
- Audit all connection attempts
- Implement connection string sanitization

#### 6. Error Handling
Build comprehensive error handling:
- Connection timeout errors
- Authentication failures
- Network errors
- Query syntax errors
- Permission denied errors
- Database not found errors
- Table/column not found errors
- Data type conversion errors
- Provide user-friendly error messages
- Suggest fixes for common errors
- Log errors to Sentry

#### 7. Monitoring & Logging
Implement observability:
- Connection health checks (every 5 minutes)
- Query performance metrics
- Sync success/failure tracking
- Data volume transferred
- Connection pool utilization
- Slow query detection (>10 seconds)
- Alert on connection failures (email + Slack)

### Database Schema (Drizzle)

Create these tables in Supabase:

```typescript
// Table: postgres_connections
{
  id: uuid (primary key),
  org_id: uuid (foreign key to orgs),
  user_id: uuid (foreign key to users),
  name: string (user-friendly name),
  host: string (encrypted),
  port: number (default 5432),
  database: string (encrypted),
  username: string (encrypted),
  password: string (encrypted),
  ssl_enabled: boolean (default false),
  ssl_ca_cert: text (optional, encrypted),
  ssh_tunnel_enabled: boolean (default false),
  ssh_host: string (optional, encrypted),
  ssh_port: number (optional),
  ssh_username: string (optional, encrypted),
  ssh_private_key: text (optional, encrypted),
  connection_pool_size: number (default 5, max 10),
  query_timeout_seconds: number (default 60),
  status: enum ('active', 'inactive', 'error'),
  last_connected_at: timestamp,
  last_error: text,
  schema_cache: jsonb (cached schema discovery results),
  schema_cached_at: timestamp,
  created_at: timestamp,
  updated_at: timestamp
}

// Table: postgres_sync_jobs
{
  id: uuid (primary key),
  connection_id: uuid (foreign key),
  table_name: string,
  sync_mode: enum ('full', 'incremental'),
  incremental_column: string (optional),
  last_sync_value: string (optional, last timestamp or ID),
  destination_table: string (raw_postgres_org123_tablename),
  status: enum ('pending', 'running', 'success', 'failed'),
  rows_synced: number,
  started_at: timestamp,
  completed_at: timestamp,
  error_message: text,
  sync_frequency: enum ('manual', '15min', '1hour', '24hours'),
  next_sync_at: timestamp,
  custom_where_clause: text (optional),
  created_at: timestamp,
  updated_at: timestamp
}

// Table: postgres_query_logs
{
  id: uuid (primary key),
  connection_id: uuid (foreign key),
  user_id: uuid (foreign key),
  query: text,
  execution_time_ms: number,
  rows_returned: number,
  status: enum ('success', 'error'),
  error_message: text,
  created_at: timestamp
}
```

### API Endpoints (NestJS)

Create these REST endpoints:

```typescript
// Connection Management
POST   /api/connectors/postgres/test-connection
POST   /api/connectors/postgres/connections
GET    /api/connectors/postgres/connections
GET    /api/connectors/postgres/connections/:id
PATCH  /api/connectors/postgres/connections/:id
DELETE /api/connectors/postgres/connections/:id

// Schema Discovery
GET    /api/connectors/postgres/connections/:id/databases
GET    /api/connectors/postgres/connections/:id/schemas
GET    /api/connectors/postgres/connections/:id/tables
GET    /api/connectors/postgres/connections/:id/tables/:table/schema
POST   /api/connectors/postgres/connections/:id/refresh-schema

// Query Execution
POST   /api/connectors/postgres/connections/:id/query
POST   /api/connectors/postgres/connections/:id/query/explain

// Data Sync
POST   /api/connectors/postgres/connections/:id/sync
GET    /api/connectors/postgres/connections/:id/sync-jobs
GET    /api/connectors/postgres/connections/:id/sync-jobs/:jobId
POST   /api/connectors/postgres/connections/:id/sync-jobs/:jobId/cancel
PATCH  /api/connectors/postgres/connections/:id/sync-jobs/:jobId/schedule

// Monitoring
GET    /api/connectors/postgres/connections/:id/health
GET    /api/connectors/postgres/connections/:id/query-logs
GET    /api/connectors/postgres/connections/:id/metrics
```

### Frontend UI Components

Build these React components in Next.js:

#### 1. PostgreSQL Setup Wizard (Multi-Step Form)
```
Step 1: Connection Details
  - Host (text input with validation)
  - Port (number input, default 5432)
  - Database name (text input)
  - Username (text input)
  - Password (password input, show/hide toggle)
  - [Test Connection] button
  
Step 2: Advanced Settings (Optional)
  - Enable SSL (checkbox)
    - Upload CA certificate (if enabled)
  - Enable SSH Tunnel (checkbox)
    - SSH Host, Port, Username, Private Key (if enabled)
  - Connection pool size (slider: 1-10)
  - Query timeout (slider: 10-300 seconds)
  
Step 3: Select Tables
  - Searchable table list with checkboxes
  - Show table info: row count, size, columns
  - "Select All" / "Deselect All" buttons
  
Step 4: Sync Configuration
  - For each selected table:
    - Sync mode: Full or Incremental
    - If incremental: Select timestamp/ID column
    - Custom filter (optional SQL WHERE clause)
  - Sync frequency: Manual, 15min, 1hr, 24hrs
  
Step 5: Review & Connect
  - Summary of all settings
  - Estimated sync time
  - [Start Sync] button
```

#### 2. Connection Management Dashboard
```
- List of all PostgreSQL connections
- Connection status (green/red indicator)
- Last synced time
- Quick actions: Test, Sync Now, Edit, Delete
- Add New Connection button
```

#### 3. Sync Progress Monitor
```
- Real-time progress bar
- Tables syncing status
- Rows synced / total rows
- Estimated time remaining
- Logs viewer (collapsible)
- Cancel sync button
```

#### 4. Table Explorer
```
- Tree view: Database > Schema > Table
- Table preview (first 100 rows)
- Column information panel
- Query builder (basic SELECT builder)
- Export to CSV button
```

### Implementation Guidelines

#### Use These NPM Packages
```json
{
  "pg": "^8.11.3",              // PostgreSQL client
  "pg-pool": "^3.6.1",          // Connection pooling
  "pg-cursor": "^2.10.3",       // Streaming large results
  "ssh2": "^1.14.0",            // SSH tunneling
  "crypto-js": "^4.2.0",        // Encryption
  "zod": "^3.22.4",             // Validation
  "bull": "^4.12.0",            // Job queue for sync scheduler
  "socket.io": "^4.6.1"         // Real-time progress updates
}
```

#### Code Quality Requirements
- Use TypeScript strict mode
- Write JSDoc comments for all public methods
- Include error codes (e.g., PG_CONN_001 for connection timeout)
- Use dependency injection (NestJS patterns)
- Write unit tests for critical functions (connection, validation, query sanitization)
- Use environment variables for sensitive defaults
- Implement graceful shutdown (close all connections)
- Follow NestJS best practices (modules, providers, controllers)

#### Performance Optimization
- Use connection pooling (pg-pool)
- Cache schema discovery results (1 hour TTL in Redis/memory)
- Implement query result caching (5 min TTL for same query)
- Use database indexes for incremental sync columns
- Batch sync operations (1,000 rows per insert)
- Stream large result sets (don't load all into memory)
- Use prepared statements where possible

#### Security Best Practices
- Never log passwords or sensitive connection details
- Use parameterized queries (no string concatenation)
- Validate all user inputs with Zod schemas
- Encrypt credentials with AES-256 before storing
- Use Supabase RLS to ensure org_id isolation
- Implement rate limiting per user and per connection
- Block DDL/DML queries (only allow SELECT)
- Sanitize table/column names to prevent injection

### AI Integration Hints

When a user creates a PostgreSQL connection, store these hints for the AI:

```typescript
{
  connector_type: "postgresql",
  ai_hints: {
    // Time fields detection
    time_fields: ["created_at", "updated_at", "timestamp", "date"],
    
    // Common metric patterns
    amount_fields: ["amount", "price", "cost", "revenue", "total"],
    count_fields: ["quantity", "count", "num_", "total_"],
    
    // ID fields
    id_fields: ["id", "_id", "uuid"],
    foreign_key_pattern: /(.+)_id$/,
    
    // Suggested aggregations by data type
    numeric_aggs: ["SUM", "AVG", "MIN", "MAX", "COUNT"],
    text_aggs: ["COUNT", "COUNT DISTINCT"],
    timestamp_aggs: ["COUNT", "MIN", "MAX"],
    
    // Common table relationships
    suggest_joins: true, // Use foreign keys to suggest joins
    
    // Sample prompts based on discovered schema
    sample_prompts: [
      "Show [metric] by [dimension] for last [time_period]",
      "Compare [metric] across [dimension]",
      "Trend of [metric] over time"
    ]
  }
}
```

### Error Messages (User-Friendly)

Map PostgreSQL errors to helpful messages:

```typescript
const ERROR_MESSAGES = {
  '08001': 'Could not connect to database. Check host and port.',
  '08006': 'Connection lost. Database might be down.',
  '28000': 'Invalid username or password.',
  '28P01': 'Invalid password.',
  '3D000': 'Database does not exist.',
  '42P01': 'Table not found. It might have been deleted.',
  '42703': 'Column not found in table.',
  '42501': 'Permission denied. Contact your database administrator.',
  'ECONNREFUSED': 'Connection refused. Is the database running?',
  'ETIMEDOUT': 'Connection timed out. Check firewall settings.',
  'ENOTFOUND': 'Host not found. Check the hostname.',
};
```

### Testing Checklist

Before marking complete, test:

- [ ] Connect to PostgreSQL 10, 12, 14, 16 (version compatibility)
- [ ] Connect with SSL enabled
- [ ] Connect through SSH tunnel
- [ ] Handle invalid credentials gracefully
- [ ] Discover 1,000+ tables without timeout
- [ ] Sync table with 1M+ rows (chunked, no memory issues)
- [ ] Incremental sync detects only new rows
- [ ] Query timeout works (cancel long-running queries)
- [ ] SQL injection prevention (try malicious queries)
- [ ] Connection pool limits enforced
- [ ] Multiple users can't see each other's connections (RLS)
- [ ] Encrypted credentials can be decrypted
- [ ] Sync scheduler triggers at correct intervals
- [ ] Real-time progress updates work (WebSocket)
- [ ] Error messages are user-friendly
- [ ] Connection health check detects down database

### Documentation Requirements

Create these docs:

1. **User Guide** (`docs/connectors/postgresql.md`)
   - How to find PostgreSQL credentials
   - Step-by-step setup with screenshots
   - Troubleshooting common errors
   - Best practices for performance

2. **API Reference** (`docs/api/postgres-connector.md`)
   - All endpoints with request/response examples
   - Error codes and meanings
   - Rate limits

3. **Developer Guide** (`docs/dev/postgres-connector.md`)
   - Architecture overview
   - How to extend for other SQL databases
   - Testing instructions

### Success Criteria

The connector is complete when:

1. ✅ User can connect to any PostgreSQL database (10+)
2. ✅ Schema discovery works for databases with 1,000+ tables
3. ✅ Sync completes for table with 1M+ rows in <10 minutes
4. ✅ AI can generate correct SQL queries from natural language
5. ✅ No security vulnerabilities (SQL injection, credential leaks)
6. ✅ All tests pass (unit + integration)
7. ✅ Documentation is complete
8. ✅ 5 beta users successfully connect their databases

### File Generation Order

Generate files in this sequence:

1. **Types & Schemas**
   - `packages/types/connectors.ts`
   - `apps/api/src/database/schema.ts` (Drizzle tables)
   - `apps/api/src/modules/connectors/postgres/postgres.types.ts`

2. **Core Services**
   - `apps/api/src/modules/connectors/postgres/postgres.validator.ts`
   - `apps/api/src/modules/connectors/postgres/postgres.connection-pool.ts`
   - `apps/api/src/modules/connectors/postgres/postgres.schema-discovery.ts`
   - `apps/api/src/modules/connectors/postgres/postgres.query-executor.ts`

3. **Business Logic**
   - `apps/api/src/modules/connectors/postgres/postgres.service.ts`
   - `apps/api/src/modules/connectors/postgres/postgres.sync-scheduler.ts`

4. **API Layer**
   - `apps/api/src/modules/connectors/postgres/postgres.controller.ts`
   - `apps/api/src/modules/connectors/postgres/postgres.module.ts`

5. **Frontend**
   - `apps/app/src/components/connectors/postgres-setup.tsx`
   - `apps/app/src/components/connectors/postgres-dashboard.tsx`

6. **Documentation**
   - All three docs files

### Additional Notes

- Follow existing MantrixFlow patterns (same structure as Airbyte integration if it exists)
- Reuse Supabase connection utilities where possible
- Use existing authentication/authorization middleware
- Integrate with existing WebSocket setup for progress updates
- Use existing Sentry configuration for error tracking
- Follow the same encryption pattern as other sensitive data
- Maintain consistency with other connector UIs (if any exist)

### Edge Cases to Handle

- Empty databases (no tables)
- Tables with no data (0 rows)
- Tables with 100+ columns
- Column names with spaces or special characters
- Reserved SQL keywords as table/column names
- Tables without primary keys (for incremental sync)
- Circular foreign key relationships
- Materialized views (treat as tables)
- Partitioned tables (handle parent-child)
- Concurrent syncs (prevent race conditions)
- User deletes connection during sync (cleanup)
- Network interruption mid-sync (resume)
- Database schema changes during sync (detect and alert)
- Time zone differences (store UTC, convert on display)
- Large BYTEA/TEXT columns (skip or truncate)

---

## Execution Instructions for Cursor

1. Start by creating the type definitions and database schema
2. Implement core services one by one (connection → discovery → query → sync)
3. Add comprehensive error handling to each service
4. Build the NestJS controller and module
5. Create frontend components
6. Write tests
7. Generate documentation
8. Test with real PostgreSQL databases

**DO NOT**:
- Copy code from Airbyte or other connectors
- Skip error handling
- Skip validation
- Hardcode credentials
- Log sensitive information
- Use `eval()` or dynamic SQL
- Forget to close connections

**ALWAYS**:
- Use TypeScript types
- Write JSDoc comments
- Validate inputs with Zod
- Use parameterized queries
- Encrypt sensitive data
- Test with real databases
- Handle edge cases
- Provide helpful error messages

---

## Questions to Ask Before Starting

1. Should we support PostgreSQL-specific features (arrays, JSONB, enums)?
2. What's the maximum number of concurrent connections per organization?
3. Should we support read replicas?
4. Do we need to support PostgreSQL extensions (PostGIS, pg_vector)?
5. Should we log query results for debugging?
6. What's the retention period for query logs?
7. Should we support PostgreSQL functions/stored procedures?

---

**This prompt is production-ready. Start building!** 🚀
```

---

This Cursor prompt is comprehensive and covers:

✅ **Complete architecture** (no Airbyte dependency)  
✅ **All 7 core functionalities** (connection, discovery, query, sync, security, errors, monitoring)  
✅ **Database schema** (Drizzle ORM)  
✅ **API endpoints** (RESTful)  
✅ **Frontend components** (Next.js + Shadcn)  
✅ **Security best practices** (encryption, SQL injection prevention)  
✅ **Error handling** (user-friendly messages)  
✅ **Testing requirements**  
✅ **Documentation needs**  
✅ **Edge cases**  
## Answers to Configuration Questions
1. PostgreSQL-specific features (arrays, JSONB, enums): **YES - Full support**
2. Maximum concurrent connections per organization: **10 connections**
3. Read replicas support: **NO - Future implementation**
4. PostgreSQL extensions (PostGIS, pg_vector): **YES - Detect and support**
5. Log query results for debugging: **YES**
6. Query logs retention period: **90 days**
7. PostgreSQL functions/stored procedures: **YES - Support execution**

## Project Structure (Backend Only)
```
apps/api/
└── src/
    ├── modules/
    │   └── connectors/
    │       └── postgres/
    │           ├── postgres.module.ts
    │           ├── postgres.controller.ts
    │           ├── postgres.service.ts
    │           ├── postgres.types.ts
    │           ├── postgres.validator.ts
    │           ├── entities/
    │           │   ├── postgres-connection.entity.ts
    │           │   ├── postgres-sync-job.entity.ts
    │           │   └── postgres-query-log.entity.ts
    │           ├── services/
    │           │   ├── postgres-connection-pool.service.ts
    │           │   ├── postgres-schema-discovery.service.ts
    │           │   ├── postgres-query-executor.service.ts
    │           │   ├── postgres-sync.service.ts
    │           │   ├── postgres-encryption.service.ts
    │           │   └── postgres-health-monitor.service.ts
    │           ├── repositories/
    │           │   ├── postgres-connection.repository.ts
    │           │   ├── postgres-sync-job.repository.ts
    │           │   └── postgres-query-log.repository.ts
    │           ├── jobs/
    │           │   ├── postgres-sync.processor.ts
    │           │   └── postgres-health-check.processor.ts
    │           ├── dto/
    │           │   ├── create-connection.dto.ts
    │           │   ├── update-connection.dto.ts
    │           │   ├── test-connection.dto.ts
    │           │   ├── execute-query.dto.ts
    │           │   ├── create-sync-job.dto.ts
    │           │   └── update-sync-schedule.dto.ts
    │           ├── guards/
    │           │   └── connection-ownership.guard.ts
    │           ├── decorators/
    │           │   └── current-connection.decorator.ts
    │           ├── constants/
    │           │   ├── postgres.constants.ts
    │           │   └── error-codes.constants.ts
    │           └── utils/
    │               ├── postgres-type-mapper.util.ts
    │               ├── query-sanitizer.util.ts
    │               └── error-mapper.util.ts
    ├── database/
    │   └── drizzle/
    │       ├── schema/
    │       │   └── postgres-connectors.schema.ts
    │       └── migrations/
    └── common/
        ├── encryption/
        │   └── encryption.service.ts
        └── websocket/
            └── websocket.gateway.ts

            **Requirements**:
- Use `crypto.scrypt` for key derivation
- Use `aes-256-gcm` for authenticated encryption
- Include IV (16 bytes), salt (64 bytes), and auth tag (16 bytes)
- Format: `base64(salt:iv:tag:ciphertext)`
- Add error handling for corrupt data
- Implement constant-time comparison for hashes


**Requirements**:
- Maximum 10 connections per organization (enforce in validator)
- Connection timeout: 30 seconds
- Idle timeout: 5 minutes
- Query timeout: 60 seconds (configurable)
- Use `pg.Pool` with proper configuration
- Implement SSH tunneling using `ssh2` library
- Handle SSL/TLS certificates
- Auto-reconnect on connection loss
- Emit events for monitoring (connection created, closed, error)
- Graceful shutdown: close all connections

**Requirements**:
- Cache results for 1 hour (configurable)
- Handle large schemas (1,000+ tables) efficiently
- Support PostgreSQL  14, 15, 16 , 17 
- Detect partition tables and show parent-child relationships
- Map PostgreSQL types to TypeScript types (create type mapper utility)
- Handle special types: JSONB, arrays, enums, PostGIS geometries, pg_vector
- Return empty arrays if no permissions (don't throw errors)



AND MY PROEJCT USE THE POSTGRES SQL FOR MANAGE OUR PROEJECT DATAS 
