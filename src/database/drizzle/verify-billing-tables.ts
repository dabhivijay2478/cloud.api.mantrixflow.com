/**
 * Verify Billing Tables
 * This script checks if billing tables exist and shows their structure
 */

import * as dotenv from 'dotenv';

// Load environment variables
dotenv.config();

const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
  console.error('❌ DATABASE_URL environment variable is not set');
  process.exit(1);
}

async function verifyBillingTables() {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const postgresModule = await import('postgres');
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
  const postgres = postgresModule.default;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call
  const client = postgres(connectionString!, { max: 1 });

  try {
    console.log('🔍 Checking billing tables...\n');

    // Check subscriptions table
    // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    const subscriptionsCheck = await client`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'subscriptions'
      );
    `;

    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    const subscriptionsExists = subscriptionsCheck[0]?.exists;
    console.log(`📊 subscriptions table: ${subscriptionsExists ? '✅ EXISTS' : '❌ NOT FOUND'}`);

    if (subscriptionsExists) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
      const subscriptionsColumns = await client`
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'subscriptions'
        ORDER BY ordinal_position;
      `;
      console.log('   Columns:');
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
      subscriptionsColumns.forEach((col: any) => {
        console.log(`     - ${col.column_name} (${col.data_type})`);
      });
    }

    // Check subscription_events table
    // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    const eventsCheck = await client`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'subscription_events'
      );
    `;

    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    const eventsExists = eventsCheck[0]?.exists;
    console.log(`\n📊 subscription_events table: ${eventsExists ? '✅ EXISTS' : '❌ NOT FOUND'}`);

    if (eventsExists) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
      const eventsColumns = await client`
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'subscription_events'
        ORDER BY ordinal_position;
      `;
      console.log('   Columns:');
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
      eventsColumns.forEach((col: any) => {
        console.log(`     - ${col.column_name} (${col.data_type})`);
      });
    }

    // Check enums
    console.log('\n📊 Checking enums...');
    // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    const enums = await client`
      SELECT typname, array_agg(enumlabel ORDER BY enumsortorder) as values
      FROM pg_type t
      JOIN pg_enum e ON t.oid = e.enumtypid
      WHERE typname IN ('subscription_status', 'subscription_plan', 'subscription_event_type')
      GROUP BY typname;
    `;

    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
    enums.forEach((enumType: any) => {
      console.log(`   ✅ ${enumType.typname}: ${enumType.values.join(', ')}`);
    });

    // Count records
    if (subscriptionsExists) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
      const count = await client`SELECT COUNT(*) as count FROM subscriptions`;
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      console.log(`\n📈 Total subscriptions: ${count[0]?.count || 0}`);
    }

    if (eventsExists) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
      const count = await client`SELECT COUNT(*) as count FROM subscription_events`;
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      console.log(`📈 Total subscription events: ${count[0]?.count || 0}`);

      console.log('\n✅ Billing tables verification completed!');
    }
  } catch (error) {
    console.error('❌ Verification failed:', error);
    process.exit(1);
  } finally {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    await client.end();
  }
}

void verifyBillingTables();
