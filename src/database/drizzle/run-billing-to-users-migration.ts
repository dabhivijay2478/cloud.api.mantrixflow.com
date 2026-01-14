/**
 * Run Billing to Users Migration Manually
 * This script runs the migration to move billing from organization-level to user-level
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

async function runBillingToUsersMigration() {
  if (!connectionString) {
    console.error('❌ DATABASE_URL is not set');
    process.exit(1);
  }

  console.log('🔄 Running billing to users migration...');
  console.log(`📦 Database: ${connectionString.split('@')[1] || 'unknown'}`);

  const client = postgres(connectionString, { max: 1 });

  try {
    // Read the migration SQL file
    const migrationPath = join(__dirname, 'migrations', '0015_move_billing_to_users.sql');
    const migrationSQL = readFileSync(migrationPath, 'utf-8');

    console.log('📄 Executing migration SQL...');

    // Execute the migration
    await client.unsafe(migrationSQL);

    console.log('✅ Billing to users migration completed successfully!');
    console.log('✅ Added billing columns to users table');
    console.log('✅ Updated subscriptions table to reference user_id');
    console.log('✅ Updated subscription_events table to include user_id');
    console.log('✅ Migrated existing billing data from organizations to users');
  } catch (error) {
    console.error('❌ Migration failed:', error);
    process.exit(1);
  } finally {
    await client.end();
  }
}

void runBillingToUsersMigration();
