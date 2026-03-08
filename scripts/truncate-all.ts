/**
 * Truncate All Data Script
 *
 * Clears all data from the application database tables.
 * Use for development reset or testing.
 *
 * Usage:
 *   bun run scripts/truncate-all.ts           # Truncate app data (keeps users & orgs)
 *   bun run scripts/truncate-all.ts --full    # Truncate everything including users & orgs
 *   bun run scripts/truncate-all.ts --dry-run # Show SQL without executing
 *
 * Requires DATABASE_URL (or DATABASE_DIRECT_URL for pgmq) in .env
 */

import * as dotenv from "dotenv";
import postgres from "postgres";

dotenv.config();

const connectionString =
  process.env.DATABASE_DIRECT_URL ||
  (process.env.DATABASE_URL?.includes(":6543")
    ? process.env.DATABASE_URL.replace(":6543", ":5432")
    : process.env.DATABASE_URL);

const isFull = process.argv.includes("--full");
const isDryRun = process.argv.includes("--dry-run");

if (!connectionString) {
  console.error("❌ DATABASE_URL or DATABASE_DIRECT_URL is required");
  process.exit(1);
}

// Root tables: TRUNCATE CASCADE will also truncate tables that reference these
const APP_DATA_TABLES = [
  "data_sources",
  "pipelines",
  "activity_logs",
] as const;

async function truncateAll() {
  const sql = postgres(connectionString!, { max: 1 });

  if (isDryRun) {
    console.log("\n[DRY RUN] Would execute:");
    if (isFull) {
      console.log("  1. TRUNCATE TABLE organizations RESTART IDENTITY CASCADE;");
      console.log("  2. TRUNCATE TABLE users RESTART IDENTITY CASCADE;");
    } else {
      console.log(
        `  TRUNCATE TABLE ${APP_DATA_TABLES.join(", ")} RESTART IDENTITY CASCADE;`,
      );
    }
    await sql.end();
    return;
  }

  try {
    if (isFull) {
      console.log("⚠️  FULL RESET: Truncating all tables including users & organizations");
      // Organizations first (cascades to org_members, data_sources, pipelines, activity_logs, etc.)
      await sql.unsafe("TRUNCATE TABLE organizations RESTART IDENTITY CASCADE");
      // Users last (organizations references users)
      await sql.unsafe("TRUNCATE TABLE users RESTART IDENTITY CASCADE");
    } else {
      console.log("📋 Truncating app data (keeping users & organizations)");
      await sql.unsafe(
        `TRUNCATE TABLE ${APP_DATA_TABLES.join(", ")} RESTART IDENTITY CASCADE`,
      );
    }
    console.log("✅ All data truncated successfully");
  } catch (err) {
    console.error("❌ Truncate failed:", err);
    process.exit(1);
  } finally {
    await sql.end();
  }
}

// Optionally purge pgmq queues (requires pgmq extension)
async function purgePgmqQueues() {
  if (isDryRun) {
    console.log("\n[DRY RUN] Would purge pgmq queues: pipeline_jobs, incremental_sync, polling_checks");
    return;
  }
  const sql = postgres(connectionString!, { max: 1 });
  const queues = ["pipeline_jobs", "incremental_sync", "polling_checks"];
  try {
    for (const q of queues) {
      await sql.unsafe(`SELECT pgmq.purge_queue($1)`, [q]);
      console.log(`   Purged pgmq queue: ${q}`);
    }
  } catch (err) {
    console.warn("⚠️  pgmq purge skipped (extension may not be installed):", (err as Error).message);
  } finally {
    await sql.end();
  }
}

void truncateAll().then(() => purgePgmqQueues());
