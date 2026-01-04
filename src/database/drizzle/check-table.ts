/**
 * Quick script to check if postgres_connections table exists
 */

import * as dotenv from 'dotenv';
import { drizzle } from 'drizzle-orm/postgres-js';

// Load environment variables
dotenv.config();

const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
  console.error('❌ DATABASE_URL environment variable is not set');
  process.exit(1);
}

async function checkTable() {
  console.log('🔍 Checking if postgres_connections table exists...');

  // eslint-disable-next-line @typescript-eslint/no-require-imports, @typescript-eslint/no-unsafe-assignment
  const postgres = require('postgres');
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call
  const client = postgres(connectionString, { max: 1 });
  // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
  const _db = drizzle(client);

  try {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    const result = await client`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_name = 'postgres_connections'
    `;

    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    if (result.length > 0) {
      console.log('✅ Table postgres_connections EXISTS');

      // Count rows
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
      const count = await client`SELECT COUNT(*) as count FROM postgres_connections`;
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      console.log(`📊 Row count: ${count[0].count}`);
    } else {
      console.log('❌ Table postgres_connections DOES NOT EXIST');
    }
  } catch (error) {
    console.error('❌ Error checking table:', error);
  } finally {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    await client.end();
  }
}

void checkTable();
