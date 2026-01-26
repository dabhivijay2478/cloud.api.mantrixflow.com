/**
 * Schema Validation Service
 * Validates that database schema matches code expectations
 *
 * ROOT FIX: Detects missing columns/tables and provides actionable error messages
 */

import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Inject } from '@nestjs/common';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import { sql } from 'drizzle-orm';

@Injectable()
export class SchemaValidationService implements OnModuleInit {
  private readonly logger = new Logger(SchemaValidationService.name);

  constructor(
    @Inject('DRIZZLE_DB')
    private readonly db: NodePgDatabase<any>,
  ) {}

  async onModuleInit() {
    // Run validation in background (don't block startup)
    // Log warnings if schema is out of sync, but don't throw
    this.validateSchema().catch((error) => {
      // Don't throw - just log warning so app can start
      // The actual queries will fail with better error messages
      this.logger.warn(
        `Schema validation warning: ${error instanceof Error ? error.message : String(error)}`,
      );
    });
  }

  /**
   * Validate that required columns exist in the pipelines table
   */
  async validateSchema(): Promise<void> {
    try {
      // Check if pipelines table exists
      const tableExists = await this.db.execute(sql`
        SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE table_schema = 'public' 
          AND table_name = 'pipelines'
        )
      `);

      if (!tableExists.rows[0]?.exists) {
        throw new Error(
          'Table "pipelines" does not exist. Please run migrations: bun run db:migrate',
        );
      }

      // Check for required columns
      const requiredColumns = [
        'pause_timestamp',
        'schedule_type',
        'next_scheduled_run_at',
        'status',
        'checkpoint',
      ];

      const missingColumns: string[] = [];

      for (const column of requiredColumns) {
        const columnExists = await this.db.execute(sql`
          SELECT EXISTS (
            SELECT FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = 'pipelines' 
            AND column_name = ${column}
          )
        `);

        if (!columnExists.rows[0]?.exists) {
          missingColumns.push(column);
        }
      }

      if (missingColumns.length > 0) {
        const errorMessage = `Missing required columns in pipelines table: ${missingColumns.join(', ')}. Please run migrations: bun run db:migrate`;
        this.logger.warn(
          `\n${'='.repeat(80)}\n` +
            `⚠️  SCHEMA VALIDATION WARNING\n` +
            `${'='.repeat(80)}\n` +
            `${errorMessage}\n` +
            `\nTo fix this, run:\n` +
            `  cd apps/api\n` +
            `  bun run db:migrate\n` +
            `\nThis will apply all pending migrations.\n` +
            `${'='.repeat(80)}\n`,
        );
        throw new Error(errorMessage);
      }

      // Check if trigger_type enum includes 'polling'
      const enumCheck = await this.db.execute(sql`
        SELECT EXISTS (
          SELECT 1 
          FROM pg_enum 
          WHERE enumlabel = 'polling' 
          AND enumtypid = (SELECT oid FROM pg_type WHERE typname = 'trigger_type')
        )
      `);

      if (!enumCheck.rows[0]?.exists) {
        this.logger.warn(
          "Enum value 'polling' not found in trigger_type enum. Please run migrations: bun run db:migrate",
        );
      }

      this.logger.log('✅ Schema validation passed - all required columns exist');
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);

      // If it's a schema validation error, provide actionable message
      if (errorMessage.includes('does not exist') || errorMessage.includes('Missing required')) {
        this.logger.error(
          `\n${'='.repeat(80)}\n` +
            `❌ SCHEMA VALIDATION FAILED\n` +
            `${'='.repeat(80)}\n` +
            `${errorMessage}\n` +
            `\nTo fix this, run:\n` +
            `  cd apps/api\n` +
            `  bun run db:migrate\n` +
            `\nThis will apply all pending migrations including:\n` +
            `  - 0016_pipeline_incremental_sync_fixes.sql (adds pause_timestamp)\n` +
            `  - 0017_add_polling_trigger_type.sql (adds polling to trigger_type enum)\n` +
            `${'='.repeat(80)}\n`,
        );
      }

      throw error;
    }
  }
}
