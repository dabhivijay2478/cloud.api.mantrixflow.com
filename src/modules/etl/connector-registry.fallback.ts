/**
 * Fallback connector registry — mirrors apps/new-etl/connectors/registry.py
 * Used when ETL service is unreachable so the UI always receives the full connector list.
 */

export interface ConnectorSource {
  type: string;
  id: string;
  label: string;
  category: string;
  cdc: boolean;
}

export interface ConnectorDestination {
  type: string;
  id: string;
  label: string;
  cache_type?: string | null;
  docker?: boolean;
}

const TOP_20_SOURCES: Array<{ id: string; label: string; category: string; cdc: boolean }> = [
  { id: 'source-postgres', label: 'PostgreSQL', category: 'Database', cdc: true },
  { id: 'source-mysql', label: 'MySQL', category: 'Database', cdc: true },
  { id: 'source-mongodb-v2', label: 'MongoDB', category: 'Database', cdc: false },
  { id: 'source-mssql', label: 'SQL Server', category: 'Database', cdc: true },
  { id: 'source-snowflake', label: 'Snowflake', category: 'Warehouse', cdc: false },
  { id: 'source-bigquery', label: 'BigQuery', category: 'Warehouse', cdc: false },
  { id: 'source-s3', label: 'Amazon S3', category: 'Storage', cdc: false },
  { id: 'source-shopify', label: 'Shopify', category: 'E-Commerce', cdc: false },
  { id: 'source-stripe', label: 'Stripe', category: 'Finance', cdc: false },
  { id: 'source-hubspot', label: 'HubSpot', category: 'CRM', cdc: false },
  { id: 'source-salesforce', label: 'Salesforce', category: 'CRM', cdc: false },
  { id: 'source-github', label: 'GitHub', category: 'DevTools', cdc: false },
  { id: 'source-google-sheets', label: 'Google Sheets', category: 'Google', cdc: false },
  { id: 'source-google-analytics', label: 'Google Analytics', category: 'Analytics', cdc: false },
  { id: 'source-facebook-marketing', label: 'Facebook Marketing', category: 'Marketing', cdc: false },
  { id: 'source-airtable', label: 'Airtable', category: 'Product', cdc: false },
  { id: 'source-notion', label: 'Notion', category: 'Product', cdc: false },
  { id: 'source-slack', label: 'Slack', category: 'Collaboration', cdc: false },
  { id: 'source-faker', label: 'Faker', category: 'Testing', cdc: false },
  { id: 'source-file', label: 'File', category: 'Files', cdc: false },
];

const TOP_20_DESTINATIONS: Array<{
  id: string;
  label: string;
  cache_type?: string | null;
  docker?: boolean;
}> = [
  { id: 'postgres', label: 'PostgreSQL', cache_type: 'PostgresCache', docker: false },
  { id: 'bigquery', label: 'BigQuery', cache_type: 'BigQueryCache', docker: false },
  { id: 'snowflake', label: 'Snowflake', cache_type: 'SnowflakeCache', docker: false },
  { id: 'duckdb', label: 'DuckDB', cache_type: 'DuckDBCache', docker: false },
  { id: 'motherduck', label: 'MotherDuck', cache_type: 'MotherDuckCache', docker: false },
  { id: 'destination-mysql', label: 'MySQL', cache_type: null, docker: true },
  { id: 'destination-mongodb', label: 'MongoDB', cache_type: null, docker: true },
  { id: 'destination-s3', label: 'S3', cache_type: null, docker: true },
  { id: 'destination-redshift', label: 'Redshift', cache_type: null, docker: true },
  { id: 'destination-databricks', label: 'Databricks', cache_type: null, docker: true },
  { id: 'destination-kafka', label: 'Kafka', cache_type: null, docker: true },
  { id: 'destination-elasticsearch', label: 'Elasticsearch', cache_type: null, docker: true },
  { id: 'destination-pinecone', label: 'Pinecone', cache_type: null, docker: true },
  { id: 'destination-weaviate', label: 'Weaviate', cache_type: null, docker: true },
  { id: 'destination-qdrant', label: 'Qdrant', cache_type: null, docker: true },
  { id: 'destination-chroma', label: 'Chroma', cache_type: null, docker: true },
  { id: 'destination-meilisearch', label: 'Meilisearch', cache_type: null, docker: true },
  { id: 'destination-clickhouse', label: 'ClickHouse', cache_type: null, docker: true },
  { id: 'destination-mssql', label: 'MSSQL', cache_type: null, docker: true },
];

function getFallbackSources(): ConnectorSource[] {
  return TOP_20_SOURCES.map((s) => ({ type: s.id, ...s }));
}

function getFallbackDestinations(): ConnectorDestination[] {
  return TOP_20_DESTINATIONS.map((d) => ({ type: d.id, ...d }));
}

export function getFallbackConnectors(): {
  sources: ConnectorSource[];
  destinations: ConnectorDestination[];
} {
  return {
    sources: getFallbackSources(),
    destinations: getFallbackDestinations(),
  };
}
