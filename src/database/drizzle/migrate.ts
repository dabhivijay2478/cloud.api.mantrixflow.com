/**
 * Migration Runner
 * Run this script to apply migrations to the database
 */

import { drizzle } from 'drizzle-orm/postgres-js';
import { migrate } from 'drizzle-orm/postgres-js/migrator';
import * as dotenv from 'dotenv';
import * as path from 'path';

// Load environment variables
dotenv.config();

const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
  console.error('❌ DATABASE_URL environment variable is not set');
  process.exit(1);
}

async function runMigrations() {
  const dbUrl = connectionString!; // We know it's defined from the check above
  console.log('🔄 Starting database migrations...');
  console.log(`📦 Database: ${dbUrl.split('@')[1] || 'unknown'}`);

  // Use require for postgres to handle CommonJS properly
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const postgres = require('postgres');
  const client = postgres(dbUrl, { max: 1 });
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
    await client.end();
  }
}

runMigrations();

