/**
 * PgBoss Module
 * PostgreSQL-based job queue module for NestJS
 * Replaces BullMQ (Redis-based) with PgBoss (PostgreSQL-based)
 *
 * Benefits:
 * - No Redis dependency
 * - Uses existing PostgreSQL database
 * - ACID-compliant job processing
 * - Exactly-once delivery
 * - Distributed cron jobs
 */

import { DynamicModule, Global, Module, type Provider } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PgBossService } from './pgboss.service';
import { PGBOSS_OPTIONS } from './pgboss.constants';
import type { PgBossModuleOptions, PgBossModuleAsyncOptions } from './pgboss.interfaces';

@Global()
@Module({})
// biome-ignore lint/complexity/noStaticOnlyClass: NestJS module pattern
export class PgBossModule {
  static forRoot(options: PgBossModuleOptions): DynamicModule {
    return {
      module: PgBossModule,
      providers: [
        {
          provide: PGBOSS_OPTIONS,
          useValue: options,
        },
        PgBossService,
      ],
      exports: [PgBossService],
    };
  }

  /**
   * Register PgBoss module with async configuration
   * Uses factory pattern for dependency injection
   */
  static forRootAsync(options: PgBossModuleAsyncOptions): DynamicModule {
    const asyncProviders = PgBossModule.createAsyncProviders(options);

    return {
      module: PgBossModule,
      imports: options.imports || [],
      providers: [...asyncProviders, PgBossService],
      exports: [PgBossService],
    };
  }

  /**
   * Create async providers for factory pattern
   */
  private static createAsyncProviders(options: PgBossModuleAsyncOptions): Provider[] {
    if (options.useFactory) {
      return [
        {
          provide: PGBOSS_OPTIONS,
          useFactory: options.useFactory,
          inject: options.inject || [],
        },
      ];
    }

    if (options.useClass) {
      return [
        {
          provide: PGBOSS_OPTIONS,
          useClass: options.useClass,
        },
      ];
    }

    if (options.useExisting) {
      return [
        {
          provide: PGBOSS_OPTIONS,
          useExisting: options.useExisting,
        },
      ];
    }

    return [];
  }

  /**
   * Convenience method for typical NestJS setup with ConfigService
   */
  static forRootWithConfig(): DynamicModule {
    return PgBossModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => {
        const databaseUrl = configService.get<string>('DATABASE_URL');

        if (databaseUrl) {
          return {
            connectionString: databaseUrl,
            // PgBoss configuration options
            application_name: 'ai-bi-queue',
            schema: 'pgboss', // Separate schema for queue tables
            retryLimit: 3,
            retryDelay: 60, // 60 seconds
            retryBackoff: true, // Exponential backoff
            expireInHours: 24, // Jobs expire after 24 hours
            archiveCompletedAfterSeconds: 3600, // Archive completed jobs after 1 hour
            deleteAfterDays: 7, // Delete archived jobs after 7 days
            monitorStateIntervalSeconds: 30,
            maintenanceIntervalSeconds: 120,
          };
        }

        // Fallback to individual configuration
        return {
          host: configService.get('DB_HOST', 'localhost'),
          port: configService.get<number>('DB_PORT', 5432),
          database: configService.get('DB_NAME', 'postgres'),
          user: configService.get('DB_USER', 'postgres'),
          password: configService.get<string>('DB_PASSWORD'),
          schema: 'pgboss',
          application_name: 'ai-bi-queue',
          retryLimit: 3,
          retryDelay: 60,
          retryBackoff: true,
        };
      },
    });
  }
}
