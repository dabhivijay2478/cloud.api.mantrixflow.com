/**
 * Delete Orphaned Data Sources
 *
 * Removes data sources that have no connection record.
 * Use after migration fixes when connection creation failed mid-flow.
 *
 * Usage:
 *   bun run scripts/delete-orphaned-data-sources.ts              # Delete all orphaned
 *   bun run scripts/delete-orphaned-data-sources.ts --name Neon  # Delete orphaned named "Neon"
 *   bun run scripts/delete-orphaned-data-sources.ts --dry-run    # Show what would be deleted
 *
 * Requires DATABASE_URL in .env
 */

import * as dotenv from "dotenv";
import postgres from "postgres";

dotenv.config();

const connectionString =
  process.env.DATABASE_DIRECT_URL ||
  (process.env.DATABASE_URL?.includes(":6543")
    ? process.env.DATABASE_URL.replace(":6543", ":5432")
    : process.env.DATABASE_URL);

const nameFilter = (() => {
  const idx = process.argv.indexOf("--name");
  return idx >= 0 && process.argv[idx + 1]
    ? process.argv[idx + 1]
    : null;
})();
const isDryRun = process.argv.includes("--dry-run");

if (!connectionString) {
  console.error("❌ DATABASE_URL or DATABASE_DIRECT_URL is required");
  process.exit(1);
}

async function deleteOrphaned() {
  const sql = postgres(connectionString!, { max: 1 });

  try {
    // Find data sources with no connection
    const orphaned = await sql`
      SELECT ds.id, ds.name, ds.organization_id, ds.created_at
      FROM data_sources ds
      LEFT JOIN data_source_connections dsc ON dsc.data_source_id = ds.id
      WHERE dsc.id IS NULL
        AND ds.deleted_at IS NULL
        ${nameFilter ? sql`AND ds.name = ${nameFilter}` : sql``}
      ORDER BY ds.created_at DESC
    `;

    if (orphaned.length === 0) {
      console.log(
        nameFilter
          ? `No orphaned data sources found with name "${nameFilter}".`
          : "No orphaned data sources found.",
      );
      await sql.end();
      return;
    }

    console.log(
      `Found ${orphaned.length} orphaned data source(s)${nameFilter ? ` named "${nameFilter}"` : ""}:`,
    );
    for (const row of orphaned) {
      console.log(`  - ${row.name} (id: ${row.id}, org: ${row.organization_id})`);
    }

    if (isDryRun) {
      console.log("\n[DRY RUN] Would delete the above. Run without --dry-run to execute.");
      await sql.end();
      return;
    }

    const ids = orphaned.map((r) => r.id);
    await sql`DELETE FROM data_sources WHERE id IN ${sql(ids)}`;
    console.log(`\n✅ Deleted ${orphaned.length} orphaned data source(s).`);
  } catch (err) {
    console.error("❌ Delete failed:", err);
    process.exit(1);
  } finally {
    await sql.end();
  }
}

void deleteOrphaned();
