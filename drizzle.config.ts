import type { Config } from 'drizzle-kit';
import * as dotenv from 'dotenv';

// Load environment variables
dotenv.config();

if (!process.env.DATABASE_URL) {
  throw new Error('DATABASE_URL environment variable is required');
}

export default {
  schema: [
    './src/database/schemas/users/users.schema.ts',
    './src/database/schemas/organizations/organizations.schema.ts',
    './src/database/schemas/data-sources/connections/postgres-connections.schema.ts',
    './src/database/schemas/data-sources/query-logs/postgres-query-logs.schema.ts',
    './src/database/schemas/data-sources/sync-jobs/postgres-sync-jobs.schema.ts',
    './src/database/schemas/data-pipelines/source-schemas/pipeline-source-schemas.schema.ts',
    './src/database/schemas/data-pipelines/destination-schemas/pipeline-destination-schemas.schema.ts',
    './src/database/schemas/data-pipelines/pipelines/postgres-pipelines.schema.ts',
    './src/database/schemas/data-pipelines/pipeline-runs/postgres-pipeline-runs.schema.ts',
  ],
  out: './src/database/drizzle/migrations',
  dialect: 'postgresql',
  dbCredentials: {
    url: process.env.DATABASE_URL,
  },
  verbose: true,
  strict: true,
} satisfies Config;
