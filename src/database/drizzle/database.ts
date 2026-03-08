/**
 * Drizzle Database Configuration
 * Factory function to create Drizzle database instance with ConfigService
 *
 * Uses transaction mode (port 6543) when DATABASE_URL uses the pooler, so Drizzle
 * does not consume direct connections. Reserves direct (5432) for pgmq/LISTEN/pg_cron.
 * Migrations use DATABASE_DIRECT_URL separately (see migrate.ts).
 */

import type { ConfigService } from '@nestjs/config';
import { drizzle } from 'drizzle-orm/postgres-js';
import * as schema from '../schemas';

export const createDrizzleDatabase = (configService: ConfigService) => {
  const databaseUrl = configService.get<string>('DATABASE_URL');
  const directUrl = configService.get<string>('DATABASE_DIRECT_URL');

  // Prefer pooler (6543) for Drizzle — transaction mode, no direct connections.
  // Fall back to direct when no pooler is configured.
  const connectionString =
    (databaseUrl?.includes(':6543') ? databaseUrl : null) ||
    directUrl ||
    databaseUrl ||
    null;

  if (!connectionString) {
    throw new Error('DATABASE_URL or DATABASE_DIRECT_URL environment variable is required');
  }

  // Use require for postgres to handle CommonJS properly
  // eslint-disable-next-line @typescript-eslint/no-require-imports, @typescript-eslint/no-unsafe-assignment
  const postgres = require('postgres');

  // Create postgres client
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call
  const client = postgres(connectionString, {
    max: 10,
  });

  // Create Drizzle instance
  // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
  return drizzle(client, { schema });
};

// Export schema for use in repositories
export { schema };
export type DrizzleDatabase = ReturnType<typeof createDrizzleDatabase>;
