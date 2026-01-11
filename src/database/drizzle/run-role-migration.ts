/**
 * Run Role Migration Script
 * Applies the role refactoring migration manually
 * 
 * Usage: bun run src/database/drizzle/run-role-migration.ts
 */

import * as path from 'node:path';
import * as fs from 'node:fs';
import * as dotenv from 'dotenv';

// Load environment variables
dotenv.config();

const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
  console.error('❌ DATABASE_URL environment variable is not set');
  process.exit(1);
}

async function runRoleMigration() {
  // TypeScript assertion: connectionString is guaranteed to be defined after the check above
  const dbUrl = connectionString as string;

  console.log('🔄 Running role refactoring migration...');
  console.log(`📦 Database: ${dbUrl.split('@')[1] || 'unknown'}`);

  // Import postgres
  const postgres = await import('postgres');
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call
  const sql = postgres.default(dbUrl, { max: 1 });

  try {
    // Read the migration file
    const migrationPath = path.join(
      __dirname,
      'migrations',
      '0012_refactor_roles_and_enforce_owner_constraint.sql',
    );

    if (!fs.existsSync(migrationPath)) {
      console.error(`❌ Migration file not found: ${migrationPath}`);
      process.exit(1);
    }

    const migrationSQL = fs.readFileSync(migrationPath, 'utf-8');

    console.log('📄 Executing migration SQL...');
    
    // Execute the migration
    // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    await sql.unsafe(migrationSQL);

    console.log('✅ Role migration completed successfully!');
    console.log('📝 The database enum has been updated to: OWNER, ADMIN, EDITOR, VIEWER');
    console.log('🔒 ONE OWNER constraint has been enforced');
  } catch (error) {
    console.error('❌ Migration failed:', error);
    process.exit(1);
  } finally {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
    await sql.end();
  }
}

void runRoleMigration();
