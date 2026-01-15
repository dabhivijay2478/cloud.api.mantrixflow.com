/**
 * Reset Database and Run Migrations
 * Drops all tables and types, then applies fresh migrations
 */

import * as path from 'node:path';
import * as dotenv from 'dotenv';
import { drizzle } from 'drizzle-orm/postgres-js';
import { migrate } from 'drizzle-orm/postgres-js/migrator';
import { sql } from 'drizzle-orm';

// Load environment variables
dotenv.config();

const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
  console.error('❌ DATABASE_URL environment variable is not set');
  process.exit(1);
}

async function resetAndMigrate() {
  const dbUrl = connectionString!;
  console.log('🔄 Starting database reset and migration...');
  console.log(`📦 Database: ${dbUrl.split('@')[1] || 'unknown'}`);

  // Import postgres using dynamic import for ESM compatibility
  const postgres = await import('postgres');
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call
  const client = postgres.default(dbUrl, { max: 1 });
  // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
  const db = drizzle(client);

  try {
    console.log('🗑️  Dropping all tables and types...');

    // Drop all tables in public schema
    const dropTablesQuery = sql`
      DO $$ DECLARE
        r RECORD;
      BEGIN
        FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
          EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(r.tablename) || ' CASCADE';
        END LOOP;
      END $$;
    `;
    await db.execute(dropTablesQuery);

    // Drop all custom types/enums in public schema
    const dropTypesQuery = sql`
      DO $$ DECLARE
        r RECORD;
      BEGIN
        FOR r IN (SELECT typname FROM pg_type WHERE typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public') AND typtype = 'e') LOOP
          EXECUTE 'DROP TYPE IF EXISTS public.' || quote_ident(r.typname) || ' CASCADE';
        END LOOP;
      END $$;
    `;
    await db.execute(dropTypesQuery);

    console.log('✅ All tables and types dropped');

    // Drop drizzle migrations table if it exists (we'll recreate it)
    await db.execute(sql`DROP TABLE IF EXISTS drizzle.__drizzle_migrations CASCADE`);
    // Also drop the drizzle schema if it exists (will be recreated)
    await db.execute(sql`DROP SCHEMA IF EXISTS drizzle CASCADE`);

    console.log('🔄 Running fresh migrations...');
    await migrate(db, {
      migrationsFolder: path.join(__dirname, 'migrations'),
    });
    console.log('✅ Migrations completed successfully!');
  } catch (error) {
    console.error('❌ Migration failed:', error);
    process.exit(1);
  } finally {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    await client.end();
  }
}

void resetAndMigrate();
