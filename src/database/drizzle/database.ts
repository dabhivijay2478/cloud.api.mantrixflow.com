/**
 * Drizzle Database Configuration
 * TODO: Configure with actual Supabase/PostgreSQL connection
 */

import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import * as schema from './schema/postgres-connectors.schema';

// TODO: Replace with actual database connection string from environment
const connectionString =
  process.env.DATABASE_URL ||
  'postgresql://user:password@localhost:5432/dbname';

// Create postgres client
const client = postgres(connectionString, {
  max: 10,
});

// Create Drizzle instance
export const db = drizzle(client, { schema });

// Export schema for use in repositories
export { schema };
