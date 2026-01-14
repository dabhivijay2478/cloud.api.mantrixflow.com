import postgres from 'postgres';
import * as fs from 'node:fs';
import * as path from 'node:path';

/**
 * Script to manually run the customer ID migration
 * Run with: bun run db:migrate:customer-id
 */
async function runMigration() {
  const connectionString = process.env.DATABASE_URL;

  if (!connectionString) {
    console.error('❌ DATABASE_URL environment variable is not set');
    process.exit(1);
  }

  const sql = postgres(connectionString, { max: 1 });

  try {
    const migrationPath = path.join(__dirname, 'migrations', '0015_add_dodo_customer_id.sql');

    const migrationSQL = fs.readFileSync(migrationPath, 'utf-8');

    console.log('🔄 Running migration: 0015_add_dodo_customer_id.sql');
    await sql.unsafe(migrationSQL);
    console.log('✅ Migration completed successfully');
  } catch (error) {
    console.error('❌ Migration failed:', error);
    process.exit(1);
  } finally {
    await sql.end();
  }
}

void runMigration();
