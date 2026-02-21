/**
 * Run ETL jobs migration (0022) standalone
 * Use when etl_jobs table is missing: bun run db:migrate:etl
 */

import * as path from 'node:path';
import * as fs from 'node:fs';
import * as dotenv from 'dotenv';

dotenv.config();

const connectionString = process.env.DATABASE_URL || process.env.DATABASE_DIRECT_URL;

if (!connectionString) {
  console.error('❌ DATABASE_URL or DATABASE_DIRECT_URL required');
  process.exit(1);
}

async function run() {
  const postgres = await import('postgres');
  const sql = postgres.default(connectionString!, { max: 1 });

  const migrations = [
    '0022_add_etl_jobs_pgmq.sql',
    '0023_etl_jobs_backend_read_policy.sql',
    '0024_etl_jobs_backend_insert_update.sql',
  ];

  console.log('🔄 Running ETL migrations...');
  console.log(`📦 Database: ${connectionString!.split('@')[1]?.split('/')[0] || 'unknown'}`);

  try {
    for (const file of migrations) {
      const migrationPath = path.join(__dirname, 'migrations', file);
      const sqlContent = fs.readFileSync(migrationPath, 'utf-8');
      console.log(`  Running ${file}...`);
      await sql.unsafe(sqlContent);
    }
    console.log('✅ ETL migrations completed successfully!');
  } catch (error) {
    console.error('❌ Migration failed:', error);
    process.exit(1);
  } finally {
    await sql.end();
  }
}

void run();
