/**
 * Verify Billing Migration
 * Check if billing tables and columns were created successfully
 */

import * as dotenv from 'dotenv';
import postgres from 'postgres';

// Load environment variables
dotenv.config();

const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
  console.error('❌ DATABASE_URL environment variable is not set');
  process.exit(1);
}

async function verifyMigration() {
  if (!connectionString) {
    console.error('❌ DATABASE_URL is not set');
    process.exit(1);
  }

  console.log('🔍 Verifying billing migration...');

  const client = postgres(connectionString, { max: 1 });

  try {
    // Check billing columns in organizations table
    const orgColumns = await client`
      SELECT column_name, data_type, is_nullable, column_default
      FROM information_schema.columns 
      WHERE table_name = 'organizations' 
      AND column_name LIKE 'billing%'
      ORDER BY column_name;
    `;

    console.log('\n📋 Billing columns in organizations table:');
    if (orgColumns.length === 0) {
      console.log('  ❌ No billing columns found!');
    } else {
      orgColumns.forEach((col: any) => {
        console.log(`  ✅ ${col.column_name} (${col.data_type})`);
      });
    }

    // Check subscriptions table
    const subscriptionsTable = await client`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_name = 'subscriptions';
    `;

    console.log('\n📋 Subscriptions table:');
    if (subscriptionsTable.length === 0) {
      console.log('  ❌ Subscriptions table not found!');
    } else {
      console.log('  ✅ Subscriptions table exists');

      // Check subscriptions table columns
      const subColumns = await client`
        SELECT column_name, data_type
        FROM information_schema.columns 
        WHERE table_name = 'subscriptions'
        ORDER BY column_name;
      `;

      console.log('\n  Columns in subscriptions table:');
      subColumns.forEach((col: any) => {
        console.log(`    ✅ ${col.column_name} (${col.data_type})`);
      });
    }

    // Check indexes
    const indexes = await client`
      SELECT indexname, tablename
      FROM pg_indexes
      WHERE tablename IN ('organizations', 'subscriptions')
      AND indexname LIKE '%billing%' OR indexname LIKE '%subscription%'
      ORDER BY tablename, indexname;
    `;

    console.log('\n📋 Billing-related indexes:');
    if (indexes.length === 0) {
      console.log('  ⚠️  No billing indexes found');
    } else {
      indexes.forEach((idx: any) => {
        console.log(`  ✅ ${idx.indexname} on ${idx.tablename}`);
      });
    }

    console.log('\n✅ Migration verification complete!');
  } catch (error) {
    console.error('❌ Verification failed:', error);
    process.exit(1);
  } finally {
    await client.end();
  }
}

void verifyMigration();
