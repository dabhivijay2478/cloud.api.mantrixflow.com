/**
 * Run Billing Migration Manually
 * This script runs the billing schema migration directly
 */

import * as dotenv from 'dotenv';
import { readFileSync } from 'fs';
import { join } from 'path';
import postgres from 'postgres';

// Load environment variables
dotenv.config();

const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
  console.error('❌ DATABASE_URL environment variable is not set');
  process.exit(1);
}

async function runBillingMigration() {
  if (!connectionString) {
    console.error('❌ DATABASE_URL is not set');
    process.exit(1);
  }

  console.log('🔄 Running billing migration...');
  console.log(`📦 Database: ${connectionString.split('@')[1] || 'unknown'}`);

  const client = postgres(connectionString, { max: 1 });

  try {
    // Read the migration SQL file
    const migrationPath = join(__dirname, 'migrations', '0013_update_billing_schema_razorpay.sql');
    const migrationSQL = readFileSync(migrationPath, 'utf-8');

    console.log('📄 Executing migration SQL...');

    // Execute the migration
    await client.unsafe(migrationSQL);

    console.log('✅ Billing migration completed successfully!');
    console.log('✅ Added billing columns to organizations table');
    console.log('✅ Created subscriptions table');
  } catch (error) {
    console.error('❌ Migration failed:', error);
    process.exit(1);
  } finally {
    await client.end();
  }
}

void runBillingMigration();
