/**
 * Manual Migration Runner for Pipeline Scheduling
 * Run this to apply the scheduling columns migration
 */

import * as dotenv from 'dotenv';

dotenv.config();

const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
  console.error('❌ DATABASE_URL environment variable is not set');
  process.exit(1);
}

async function runSchedulingMigration() {
  console.log('🔄 Applying pipeline scheduling migration...');

  const postgres = await import('postgres');
  const sql = postgres.default(connectionString!, { max: 1 });

  try {
    // Check if columns already exist
    const existingColumns = await sql`
      SELECT column_name 
      FROM information_schema.columns 
      WHERE table_name = 'pipelines' 
      AND column_name IN ('schedule_type', 'schedule_value', 'schedule_timezone', 'last_scheduled_run_at', 'next_scheduled_run_at')
    `;

    if (existingColumns.length > 0) {
      console.log(
        '✅ Scheduling columns already exist:',
        existingColumns.map((c: { column_name: string }) => c.column_name).join(', '),
      );
      await sql.end();
      return;
    }

    // Create the enum type if it doesn't exist
    console.log('📦 Creating schedule_type enum...');
    await sql`
      DO $$
      BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'schedule_type') THEN
          CREATE TYPE schedule_type AS ENUM ('none', 'minutes', 'hourly', 'daily', 'weekly', 'monthly', 'custom_cron');
        END IF;
      END
      $$;
    `;

    // Add the new columns
    console.log('📦 Adding scheduling columns to pipelines table...');
    await sql`
      ALTER TABLE pipelines
      ADD COLUMN IF NOT EXISTS schedule_type schedule_type DEFAULT 'none',
      ADD COLUMN IF NOT EXISTS schedule_value varchar(255),
      ADD COLUMN IF NOT EXISTS schedule_timezone varchar(50) DEFAULT 'UTC',
      ADD COLUMN IF NOT EXISTS last_scheduled_run_at timestamp,
      ADD COLUMN IF NOT EXISTS next_scheduled_run_at timestamp
    `;

    console.log('✅ Pipeline scheduling migration completed successfully!');
  } catch (error) {
    console.error('❌ Migration failed:', error);
    process.exit(1);
  } finally {
    await sql.end();
  }
}

void runSchedulingMigration();
