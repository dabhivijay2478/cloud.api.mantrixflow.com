/**
 * Migration Runner
 * Run this script to apply migrations to the database
 */

import * as path from 'node:path';
import * as dotenv from 'dotenv';
import { drizzle } from 'drizzle-orm/postgres-js';
import { migrate } from 'drizzle-orm/postgres-js/migrator';

// Load environment variables
dotenv.config();

// Prefer direct connection for migrations (Supabase pooler 6543 can cause DDL issues)
const connectionString =
  process.env.DATABASE_DIRECT_URL ||
  (process.env.DATABASE_URL?.includes(':6543')
    ? process.env.DATABASE_URL.replace(':6543', ':5432')
    : process.env.DATABASE_URL);

if (!connectionString) {
  console.error('❌ DATABASE_URL or DATABASE_DIRECT_URL environment variable is required');
  process.exit(1);
}

async function runMigrations() {
  const dbUrl = connectionString!;
  console.log('🔄 Starting database migrations...');
  console.log(`📦 Database: ${dbUrl.split('@')[1] || 'unknown'}`);

  // Import postgres using dynamic import for ESM compatibility
  const postgres = await import('postgres');
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call
  const client = postgres.default(dbUrl, { max: 1 });
  // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
  const db = drizzle(client);

  try {
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

void runMigrations();
