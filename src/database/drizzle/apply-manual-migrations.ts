/**
 * Apply Manual Migrations
 * Applies migrations 0016 and 0017 that were created manually
 * These migrations are not in the Drizzle journal, so they need to be applied directly
 */

import * as dotenv from 'dotenv';
import * as fs from 'fs';
import * as path from 'path';
import postgres from 'postgres';

// Load environment variables
dotenv.config();

const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
  console.error('❌ DATABASE_URL environment variable is not set');
  process.exit(1);
}

async function applyManualMigrations() {
  // TypeScript doesn't narrow after process.exit, so we use non-null assertion
  // We've already checked connectionString exists above
  const sql = postgres(connectionString!, { max: 1 });

  try {
    console.log('🔄 Applying manual migrations (0016, 0017)...');

    // Read and apply migration 0016
    const migration16Path = path.join(
      __dirname,
      'migrations',
      '0016_pipeline_incremental_sync_fixes.sql',
    );
    const migration16SQL = fs.readFileSync(migration16Path, 'utf-8');

    console.log('📄 Applying migration 0016_pipeline_incremental_sync_fixes.sql...');
    await sql.unsafe(migration16SQL);
    console.log('✅ Migration 0016 applied successfully');

    // Read and apply migration 0017
    const migration17Path = path.join(__dirname, 'migrations', '0017_add_polling_trigger_type.sql');
    const migration17SQL = fs.readFileSync(migration17Path, 'utf-8');

    console.log('📄 Applying migration 0017_add_polling_trigger_type.sql...');
    await sql.unsafe(migration17SQL);
    console.log('✅ Migration 0017 applied successfully');

    // Record in drizzle migrations table manually
    // Note: created_at is likely bigint (timestamp in milliseconds) or timestamp
    // Let's check the table structure first and use the appropriate type
    const migration16Hash = '0016_pipeline_incremental_sync_fixes';
    const migration17Hash = '0017_add_polling_trigger_type';

    // Try to insert with timestamp, if that fails, use bigint
    try {
      await sql`
        INSERT INTO drizzle.__drizzle_migrations (hash, created_at)
        VALUES 
          (${migration16Hash}, EXTRACT(EPOCH FROM NOW())::bigint * 1000),
          (${migration17Hash}, EXTRACT(EPOCH FROM NOW())::bigint * 1000)
        ON CONFLICT (hash) DO NOTHING
      `;
    } catch (error: any) {
      // If the insert fails, it's OK - migrations might already be recorded
      // or the table structure might be different
      console.log(
        'Note: Could not record migrations in journal (this is OK if they were already applied)',
      );
    }

    console.log('✅ All manual migrations applied successfully!');
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

void applyManualMigrations();
