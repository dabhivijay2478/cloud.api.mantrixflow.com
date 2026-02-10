# Drizzle Migrations Setup

## ✅ Setup Complete

Drizzle migrations have been successfully configured and applied to your database.

## Migration Scripts

The following scripts are available in `package.json`:

### Generate Migrations
```bash
bun run db:generate
```
Generates migration files based on schema changes. Run this after modifying the schema.

### Apply Migrations
```bash
bun run db:migrate
```
Applies all pending migrations to the database using the custom migration runner.

### Push Schema (Direct)
```bash
bun run db:push
```
Directly pushes schema changes to the database without generating migration files. Useful for development.

### Drizzle Studio
```bash
bun run db:studio
```
Opens Drizzle Studio - a visual database browser and query tool.

### Check Schema
```bash
bun run db:check
```
Checks for schema drift between your code and database.

### Drop Schema (⚠️ Dangerous)
```bash
bun run db:drop
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

1. Edit `src/database/schemas/` (see `drizzle.config.ts` for schema paths)
2. Generate migration: `bun run db:generate`
3. Review the generated SQL in `src/database/drizzle/migrations/`
4. Apply migration: `bun run db:migrate`

### Deploy (Vercel / CI)

On deploy, the latest Drizzle migrations run automatically:

- **Vercel**: `buildCommand` is `bun run build:deploy`, which runs `nest build` then `bun run db:migrate`. Set `DATABASE_URL` in the Vercel project (Production and Preview) so migrations succeed. If `DATABASE_URL` is missing, the build fails so you don’t deploy without migrated schema.
- **Local / CI**: Use `bun run build:deploy` when you want “build + migrate” in one step. Use `bun run build` when you only need the Nest build (e.g. tests or image build without DB).

### Quick Development

For rapid development, you can use `bun run db:push` to directly sync schema changes without generating migration files.

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

