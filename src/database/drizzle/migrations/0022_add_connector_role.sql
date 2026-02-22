-- Add connector_role to data_sources for identifying source vs destination
-- source = data origin (e.g. PostgreSQL, Stripe, GitHub)
-- destination = data target (e.g. BigQuery, Snowflake, S3)

ALTER TABLE "data_sources"
  ADD COLUMN IF NOT EXISTS "connector_role" varchar(20) NOT NULL DEFAULT 'source';

-- Backfill existing rows: infer from source_type
-- source_type starting with "source-" -> source
-- source_type starting with "destination-" or "dest-" -> destination
-- Known destination-only types (no source equivalent in our registry) -> destination
-- Everything else (postgres, mysql, etc. - can be both) -> keep as source
UPDATE "data_sources"
SET "connector_role" = CASE
  WHEN "source_type" LIKE 'source-%' THEN 'source'
  WHEN "source_type" LIKE 'destination-%' OR "source_type" LIKE 'dest-%' THEN 'destination'
  WHEN LOWER("source_type") IN (
    'duckdb', 'motherduck', 'redshift', 'databricks', 'clickhouse',
    'kafka', 'elasticsearch', 'pinecone', 'weaviate', 'qdrant',
    'chroma', 'meilisearch'
  ) THEN 'destination'
  ELSE 'source'
END;

-- Add index for filtering by connector role
CREATE INDEX IF NOT EXISTS "data_sources_connector_role_idx"
  ON "data_sources"("connector_role");
