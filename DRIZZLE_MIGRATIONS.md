# Drizzle Migrations Setup

## ✅ Setup Complete

Drizzle migrations have been successfully configured and applied to your database.

## Migration Scripts

The following scripts are available in `package.json`:

### Generate Migrations
```bash
pnpm db:generate
```
Generates migration files based on schema changes. Run this after modifying the schema.

### Apply Migrations
```bash
pnpm db:migrate
```
Applies all pending migrations to the database using the custom migration runner.

### Push Schema (Direct)
```bash
pnpm db:push
```
Directly pushes schema changes to the database without generating migration files. Useful for development.

### Drizzle Studio
```bash
pnpm db:studio
```
Opens Drizzle Studio - a visual database browser and query tool.

### Check Schema
```bash
pnpm db:check
```
Checks for schema drift between your code and database.

### Drop Schema (⚠️ Dangerous)
```bash
pnpm db:drop
```
Drops all tables. Use with caution!

## Configuration

- **Config File**: `drizzle.config.ts`
- **Schema Location**: `src/database/drizzle/schema/postgres-connectors.schema.ts`
- **Migrations Folder**: `src/database/drizzle/migrations/`
- **Database URL**: Set via `DATABASE_URL` environment variable

## Generated Tables

The following tables have been created in your database:

1. **postgres_connections** - Stores encrypted PostgreSQL connection credentials
2. **postgres_sync_jobs** - Tracks data synchronization jobs
3. **postgres_query_logs** - Audit log for all queries

## Migration Files

- `0000_needy_mole_man.sql` - Initial migration with all tables and enums

## Workflow

### Making Schema Changes

1. Edit `src/database/drizzle/schema/postgres-connectors.schema.ts`
2. Generate migration: `pnpm db:generate`
3. Review the generated SQL in `src/database/drizzle/migrations/`
4. Apply migration: `pnpm db:migrate`

### Quick Development

For rapid development, you can use `pnpm db:push` to directly sync schema changes without generating migration files.

## Environment Variables

Make sure your `.env` file contains:
```env
DATABASE_URL=postgresql://user:password@host:port/database
ENCRYPTION_MASTER_KEY=your-256-bit-encryption-key-here
```

## Next Steps

1. ✅ Migrations are set up and applied
2. ⏭️ Wire up Drizzle database instance in repositories
3. ⏭️ Test the PostgreSQL connector with real connections
4. ⏭️ Set up Row-Level Security (RLS) policies in Supabase

