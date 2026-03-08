/**
 * Database Module
 * Provides a single shared Drizzle database instance for the entire application.
 * Uses transaction mode (port 6543) to avoid consuming direct connections.
 */

import { Global, Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { createDrizzleDatabase } from './drizzle/database';

@Global()
@Module({
  imports: [ConfigModule],
  providers: [
    {
      provide: 'DRIZZLE_DB',
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => createDrizzleDatabase(configService),
    },
  ],
  exports: ['DRIZZLE_DB'],
})
export class DatabaseModule {}
