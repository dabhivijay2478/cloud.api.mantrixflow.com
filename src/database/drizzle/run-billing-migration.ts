/**
 * Run Billing Tables Migration Manually
 * This script manually runs the billing tables migration SQL
 */

import * as dotenv from 'dotenv';
import * as fs from 'node:fs';
import * as path from 'node:path';

// Load environment variables
dotenv.config();

const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
  console.error('❌ DATABASE_URL environment variable is not set');
  process.exit(1);
}

async function runBillingMigration() {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const postgresModule = await import('postgres');
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
  const postgres = postgresModule.default;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call
  const client = postgres(connectionString!, { max: 1 });

  try {
    console.log('🔄 Running billing tables migration...');

    // Read the migration SQL file
    const migrationPath = path.join(__dirname, 'migrations', '0013_add_billing_tables.sql');
    const migrationSQL = fs.readFileSync(migrationPath, 'utf-8');

    // Execute the migration SQL
    // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    await client.unsafe(migrationSQL);

    console.log('✅ Billing tables migration completed successfully!');
    console.log('📊 Created tables: subscriptions, subscription_events');
    console.log(
      '📊 Created enums: subscription_status, subscription_plan, subscription_event_type',
    );
  } catch (error) {
    console.error('❌ Migration failed:', error);
    process.exit(1);
  } finally {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    await client.end();
  }
}

void runBillingMigration();
