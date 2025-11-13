/**
 * Drizzle Database Configuration
 * Factory function to create Drizzle database instance with ConfigService
 */

import { drizzle } from 'drizzle-orm/postgres-js';
import { ConfigService } from '@nestjs/config';
import * as schema from './schema/postgres-connectors.schema';

export const createDrizzleDatabase = (configService: ConfigService) => {
  const connectionString = configService.get<string>('DATABASE_URL');

  if (!connectionString) {
    throw new Error('DATABASE_URL environment variable is not set');
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
