/**
 * Apply Transform Script Migrations
 * Manually applies migrations 0018 and 0019 for transform_script support
 */

import * as path from 'node:path';
import * as fs from 'node:fs';
import * as dotenv from 'dotenv';
import postgres from 'postgres';

// Load environment variables
dotenv.config();

const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
  console.error('❌ DATABASE_URL environment variable is not set');
  process.exit(1);
}

async function applyTransformMigrations() {
  const sql = postgres(connectionString!, { max: 1 });

  try {
    console.log('🔄 Applying transform script migrations (0018, 0019)...');

    // Read and apply migration 0018
    const migration18Path = path.join(__dirname, 'migrations', '0018_add_transform_script.sql');
    const migration18SQL = fs.readFileSync(migration18Path, 'utf-8');

    console.log('📄 Applying migration 0018_add_transform_script.sql...');
    await sql.unsafe(migration18SQL);
    console.log('✅ Migration 0018 applied successfully');

    // Read and apply migration 0019
    const migration19Path = path.join(__dirname, 'migrations', '0019_remove_column_mappings.sql');
    const migration19SQL = fs.readFileSync(migration19Path, 'utf-8');

    console.log('📄 Applying migration 0019_remove_column_mappings.sql...');
    await sql.unsafe(migration19SQL);
    console.log('✅ Migration 0019 applied successfully');

    console.log('✅ All transform migrations applied successfully!');
  } catch (error: any) {
    console.error('❌ Migration failed:', error.message);
    if (error.code) {
      console.error(`   Error code: ${error.code}`);
    }
    if (error.detail) {
      console.error(`   Detail: ${error.detail}`);
    }
    process.exit(1);
  } finally {
    await sql.end();
  }
}

void applyTransformMigrations();
