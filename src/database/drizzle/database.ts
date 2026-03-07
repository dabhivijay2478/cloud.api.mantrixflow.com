/**
 * Drizzle Database Configuration
 * Factory function to create Drizzle database instance with ConfigService
 *
 * Uses same connection logic as migrate.ts: prefer DATABASE_DIRECT_URL or port 5432
 * when DATABASE_URL uses pooler (6543), so that transactions work and schema matches
 * migrations.
 */

import type { ConfigService } from '@nestjs/config';
import { drizzle } from 'drizzle-orm/postgres-js';
import * as schema from '../schemas';

export const createDrizzleDatabase = (configService: ConfigService) => {
  const databaseUrl = configService.get<string>('DATABASE_URL');
  const directUrl = configService.get<string>('DATABASE_DIRECT_URL');

  const connectionString =
    directUrl ||
    (databaseUrl?.includes(':6543')
      ? databaseUrl.replace(':6543', ':5432')
      : databaseUrl);

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
