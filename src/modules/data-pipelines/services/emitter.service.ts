/**
 * Emitter Service
 * Generic service for emitting/writing data to any destination type
 * Supports: PostgreSQL, MySQL, MongoDB ONLY
 *
 * Architecture: Collector → Emitter (with transformation) → Transformer (post-processing)
 * Transformation happens during emission for efficiency
 *
 * ROOT FIX: Uses UPSERT mode by default to prevent data loss on re-sync
 *
 * Guide: To add a new destination type:
 * 1. Add emitTo{Type} method
 * 2. Add validate{Type}Schema method (optional)
 * 3. Add create{Type}Table method (optional)
 * 4. Add {type}TableExists method
 * 5. Update switch cases in emit(), validateSchema(), createTable(), tableExists()
 */

import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { Pool } from 'pg';
import * as mysql from 'mysql2/promise';
import { MongoClient } from 'mongodb';
import { ConnectionService } from '../../data-sources/connection.service';
import { DataSourceRepository } from '../../data-sources/repositories/data-source.repository';
import { TransformerService } from './transformer.service';

import type {
  ColumnMapping,
  SchemaValidationResult,
  Transformation,
  WriteResult,
  PipelineError,
} from '../types/common.types';
import type { PipelineDestinationSchema } from '../../../database/schemas';

/**
 * Batch size for bulk operations
 * Default is 500 for balanced performance/memory
 */
const DEFAULT_BATCH_SIZE = 500;

@Injectable()
export class EmitterService {
  private readonly logger = new Logger(EmitterService.name);

  constructor(
    private readonly connectionService: ConnectionService,
    private readonly dataSourceRepository: DataSourceRepository,
    private readonly transformerService: TransformerService,
  ) {}

  /**
   * Write data to destination
   * Architecture: Collector → Emitter (with transformation) → Transformer (post-processing)
   * Transformation happens during emission
   *
   * ROOT FIX: Defaults to 'upsert' mode when upsertKey is available to prevent data loss
   */
  async emit(options: {
    destinationSchema: PipelineDestinationSchema;
    organizationId: string;
    userId: string;
    rows: any[];
    writeMode: 'append' | 'upsert' | 'replace';
    upsertKey?: string[];
    columnMappings?: ColumnMapping[];
    transformations?: Transformation[];
  }): Promise<WriteResult> {
    const {
      destinationSchema,
      organizationId,
      userId,
      rows,
      writeMode,
      upsertKey,
      columnMappings = [],
      transformations = [],
    } = options;

    if (!destinationSchema.dataSourceId) {
      throw new BadRequestException('Destination schema must have a data source ID');
    }

    if (rows.length === 0) {
      return { rowsWritten: 0, rowsSkipped: 0, rowsFailed: 0 };
    }

    const dataSource = await this.dataSourceRepository.findById(destinationSchema.dataSourceId);
    if (!dataSource) {
      throw new BadRequestException(`Data source ${destinationSchema.dataSourceId} not found`);
    }

    // Validate data source type (only PostgreSQL, MySQL, MongoDB supported)
    if (!['postgres', 'mysql', 'mongodb'].includes(dataSource.sourceType)) {
      throw new BadRequestException(
        `Unsupported destination type: ${dataSource.sourceType}. Only postgres, mysql, mongodb are supported.`,
      );
    }

    const connectionConfig = await this.connectionService.getDecryptedConnection(
      organizationId,
      destinationSchema.dataSourceId,
      userId,
    );

    // STEP 1: Transform data during emission (Collector → Emitter with transformation)
    let effectiveMappings =
      columnMappings.length > 0
        ? columnMappings
        : (destinationSchema.columnMappings as ColumnMapping[]) || [];

    // Auto-enhance mappings for MongoDB ObjectId -> UUID conversion if not already enhanced
    effectiveMappings = this.transformerService.enhanceColumnMappings(effectiveMappings, rows);

    const transformedRows = await this.transformerService.transform(
      rows,
      effectiveMappings,
      transformations.length > 0 ? transformations : undefined,
    );

    // ROOT FIX: Prefer upsert mode to prevent data loss
    // If upsertKey is provided, always use upsert mode unless explicitly replace
    let effectiveWriteMode = writeMode;
    const effectiveUpsertKey = upsertKey || (destinationSchema.upsertKey as string[]);

    if (effectiveUpsertKey?.length > 0 && writeMode === 'append') {
      effectiveWriteMode = 'upsert';
      this.logger.log(`Using UPSERT mode (upsertKey provided: ${effectiveUpsertKey.join(', ')})`);
    }

    // STEP 2: Emit transformed data to destination in batches
    let writeResult: WriteResult;

    try {
      switch (dataSource.sourceType) {
        case 'postgres':
          writeResult = await this.emitToPostgres(
            destinationSchema,
            connectionConfig,
            transformedRows,
            effectiveWriteMode,
            effectiveUpsertKey,
            effectiveMappings,
          );
          break;
        case 'mysql':
          writeResult = await this.emitToMySQL(
            destinationSchema,
            connectionConfig,
            transformedRows,
            effectiveWriteMode,
            effectiveUpsertKey,
          );
          break;
        case 'mongodb':
          writeResult = await this.emitToMongoDB(
            destinationSchema,
            connectionConfig,
            transformedRows,
            effectiveWriteMode,
            effectiveUpsertKey,
          );
          break;
        default:
          throw new BadRequestException(`Unsupported destination type: ${dataSource.sourceType}`);
      }

      this.logger.log(
        `Emitted ${writeResult.rowsWritten} rows to ${dataSource.sourceType} destination (mode: ${effectiveWriteMode})`,
      );

      return writeResult;
    } catch (error) {
      this.logger.error(
        `Emission failed: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error;
    }
  }

  /**
   * Validate destination schema
   */
  async validateSchema(options: {
    destinationSchema: PipelineDestinationSchema;
    organizationId: string;
    userId: string;
    columnMappings: ColumnMapping[];
  }): Promise<SchemaValidationResult> {
    const { destinationSchema, organizationId, userId, columnMappings } = options;

    if (!destinationSchema.dataSourceId) {
      return {
        valid: false,
        errors: ['Destination schema must have a data source ID'],
      };
    }

    const dataSource = await this.dataSourceRepository.findById(destinationSchema.dataSourceId);
    if (!dataSource) {
      return {
        valid: false,
        errors: [`Data source ${destinationSchema.dataSourceId} not found`],
      };
    }

    const connectionConfig = await this.connectionService.getDecryptedConnection(
      organizationId,
      destinationSchema.dataSourceId,
      userId,
    );

    switch (dataSource.sourceType) {
      case 'postgres':
        return this.validatePostgresSchema(destinationSchema, connectionConfig, columnMappings);
      case 'mysql':
        return this.validateMySQLSchema(destinationSchema, connectionConfig, columnMappings);
      case 'mongodb':
        return { valid: true, errors: [], warnings: ['MongoDB schema validation not required'] };
      default:
        return {
          valid: false,
          errors: [`Unsupported destination type: ${dataSource.sourceType}`],
        };
    }
  }

  /**
   * Create destination table if needed
   */
  async createTable(options: {
    destinationSchema: PipelineDestinationSchema;
    organizationId: string;
    userId: string;
    columnMappings: ColumnMapping[];
  }): Promise<{ created: boolean; tableName: string }> {
    const { destinationSchema, organizationId, userId, columnMappings } = options;

    if (!destinationSchema.dataSourceId) {
      throw new BadRequestException('Destination schema must have a data source ID');
    }

    const dataSource = await this.dataSourceRepository.findById(destinationSchema.dataSourceId);
    if (!dataSource) {
      throw new BadRequestException(`Data source ${destinationSchema.dataSourceId} not found`);
    }

    const connectionConfig = await this.connectionService.getDecryptedConnection(
      organizationId,
      destinationSchema.dataSourceId,
      userId,
    );

    switch (dataSource.sourceType) {
      case 'postgres':
        return this.createPostgresTable(destinationSchema, connectionConfig, columnMappings);
      case 'mysql':
        return this.createMySQLTable(destinationSchema, connectionConfig, columnMappings);
      case 'mongodb':
        // MongoDB doesn't need table creation
        return { created: false, tableName: destinationSchema.destinationTable };
      default:
        throw new BadRequestException(`Unsupported destination type: ${dataSource.sourceType}`);
    }
  }

  /**
   * Check if table exists
   */
  async tableExists(options: {
    destinationSchema: PipelineDestinationSchema;
    organizationId: string;
    userId: string;
  }): Promise<boolean> {
    const { destinationSchema, organizationId, userId } = options;

    if (!destinationSchema.dataSourceId) {
      return false;
    }

    const dataSource = await this.dataSourceRepository.findById(destinationSchema.dataSourceId);
    if (!dataSource) {
      return false;
    }

    const connectionConfig = await this.connectionService.getDecryptedConnection(
      organizationId,
      destinationSchema.dataSourceId,
      userId,
    );

    switch (dataSource.sourceType) {
      case 'postgres':
        return this.postgresTableExists(destinationSchema, connectionConfig);
      case 'mysql':
        return this.mysqlTableExists(destinationSchema, connectionConfig);
      case 'mongodb':
        return true; // MongoDB doesn't require table existence checks
      default:
        return false;
    }
  }

  // ============================================================================
  // POSTGRESQL IMPLEMENTATION
  // ============================================================================

  private async emitToPostgres(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    rows: any[],
    writeMode: 'append' | 'upsert' | 'replace',
    upsertKey?: string[],
    columnMappings?: ColumnMapping[],
  ): Promise<WriteResult> {
    const sslConfig =
      typeof connectionConfig.ssl === 'object'
        ? connectionConfig.ssl.enabled
          ? { rejectUnauthorized: false }
          : undefined
        : connectionConfig.ssl
          ? { rejectUnauthorized: false }
          : undefined;

    const pool = new Pool({
      host: connectionConfig.host,
      port: connectionConfig.port,
      database: connectionConfig.database,
      user: connectionConfig.username,
      password: connectionConfig.password,
      ssl: sslConfig,
      max: 5,
    });

    let rowsWritten = 0;
    const rowsSkipped = 0;
    let rowsFailed = 0;
    const errors: PipelineError[] = [];

    try {
      const client = await pool.connect();
      try {
        const schemaName = destinationSchema.destinationSchema || 'public';
        const tableName = destinationSchema.destinationTable;
        const fullTableName = `"${schemaName}"."${tableName}"`;

        // Check if table exists and create if needed
        const tableExists = await this.postgresTableExists(destinationSchema, connectionConfig);
        if (!tableExists && columnMappings && columnMappings.length > 0) {
          this.logger.log(`Table ${fullTableName} does not exist, creating...`);
          await this.createPostgresTable(destinationSchema, connectionConfig, columnMappings);
        }

        // Process in batches
        for (let i = 0; i < rows.length; i += DEFAULT_BATCH_SIZE) {
          const batch = rows.slice(i, i + DEFAULT_BATCH_SIZE);

          try {
            await client.query('BEGIN');

            // Handle replace mode - truncate on first batch only
            if (writeMode === 'replace' && i === 0) {
              await client.query(`TRUNCATE ${fullTableName}`);
              this.logger.log(`Truncated table ${fullTableName} for replace mode`);
            }

            for (const row of batch) {
              const validEntries = Object.entries(row).filter(
                ([_, v]) => v !== undefined && v !== null,
              );
              const columns = validEntries.map(([k]) => k);
              const values = validEntries.map(([_, v]) => v);

              if (columns.length === 0) {
                await client.query(`INSERT INTO ${fullTableName} DEFAULT VALUES`);
                rowsWritten++;
                continue;
              }

              const placeholders = columns.map((_, idx) => `$${idx + 1}`).join(', ');
              const columnList = columns.map((c) => `"${c}"`).join(', ');

              if (writeMode === 'upsert' && upsertKey && upsertKey.length > 0) {
                // UPSERT using ON CONFLICT - ROOT FIX for data preservation
                const conflictTarget = upsertKey.map((k) => `"${k}"`).join(', ');
                const updateSet = columns
                  .filter((c) => !upsertKey.includes(c))
                  .map((c) => `"${c}" = EXCLUDED."${c}"`)
                  .join(', ');

                const query =
                  updateSet.length > 0
                    ? `
                    INSERT INTO ${fullTableName} (${columnList})
                    VALUES (${placeholders})
                    ON CONFLICT (${conflictTarget}) DO UPDATE SET ${updateSet}
                  `
                    : `
                    INSERT INTO ${fullTableName} (${columnList})
                    VALUES (${placeholders})
                    ON CONFLICT (${conflictTarget}) DO NOTHING
                  `;

                await client.query(query, values);
              } else {
                // APPEND mode
                const query = `INSERT INTO ${fullTableName} (${columnList}) VALUES (${placeholders})`;
                await client.query(query, values);
              }

              rowsWritten++;
            }

            await client.query('COMMIT');
          } catch (error) {
            await client.query('ROLLBACK');
            rowsFailed += batch.length;
            errors.push({
              message: error instanceof Error ? error.message : String(error),
              row: i,
            });
            this.logger.error(`Batch failed at row ${i}: ${error}`);
          }
        }

        return {
          rowsWritten,
          rowsSkipped,
          rowsFailed,
          errors: errors.length > 0 ? errors : undefined,
        };
      } finally {
        client.release();
      }
    } finally {
      await pool.end();
    }
  }

  private async validatePostgresSchema(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    columnMappings: ColumnMapping[],
  ): Promise<SchemaValidationResult> {
    const sslConfig =
      typeof connectionConfig.ssl === 'object'
        ? connectionConfig.ssl.enabled
          ? { rejectUnauthorized: false }
          : undefined
        : connectionConfig.ssl
          ? { rejectUnauthorized: false }
          : undefined;

    const pool = new Pool({
      host: connectionConfig.host,
      port: connectionConfig.port,
      database: connectionConfig.database,
      user: connectionConfig.username,
      password: connectionConfig.password,
      ssl: sslConfig,
    });

    try {
      const client = await pool.connect();

      try {
        const schemaName = destinationSchema.destinationSchema || 'public';
        const tableName = destinationSchema.destinationTable;

        const query = `
          SELECT column_name, data_type, is_nullable
          FROM information_schema.columns
          WHERE table_schema = $1 AND table_name = $2
        `;
        const result = await client.query(query, [schemaName, tableName]);
        const existingColumns = new Map(
          result.rows.map((r) => [
            r.column_name,
            { type: r.data_type, nullable: r.is_nullable === 'YES' },
          ]),
        );

        const errors: string[] = [];
        const warnings: string[] = [];
        const missingColumns: string[] = [];
        const typeMismatches: any[] = [];

        for (const mapping of columnMappings) {
          const existing = existingColumns.get(mapping.destinationColumn);
          if (!existing) {
            missingColumns.push(mapping.destinationColumn);
          } else {
            const compatible = this.areTypesCompatible(mapping.dataType, existing.type);
            if (!compatible) {
              typeMismatches.push({
                column: mapping.destinationColumn,
                expectedType: mapping.dataType,
                actualType: existing.type,
                severity: 'warning',
              });
            }
          }
        }

        if (missingColumns.length > 0) {
          errors.push(`Missing columns: ${missingColumns.join(', ')}`);
        }

        return {
          valid: errors.length === 0,
          errors,
          warnings,
          missingColumns,
          typeMismatches,
        };
      } finally {
        client.release();
      }
    } finally {
      await pool.end();
    }
  }

  private async createPostgresTable(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    columnMappings: ColumnMapping[],
  ): Promise<{ created: boolean; tableName: string }> {
    const sslConfig =
      typeof connectionConfig.ssl === 'object'
        ? connectionConfig.ssl.enabled
          ? { rejectUnauthorized: false }
          : undefined
        : connectionConfig.ssl
          ? { rejectUnauthorized: false }
          : undefined;

    const pool = new Pool({
      host: connectionConfig.host,
      port: connectionConfig.port,
      database: connectionConfig.database,
      user: connectionConfig.username,
      password: connectionConfig.password,
      ssl: sslConfig,
    });

    try {
      const client = await pool.connect();

      try {
        const schemaName = destinationSchema.destinationSchema || 'public';
        const tableName = destinationSchema.destinationTable;
        const fullTableName = `"${schemaName}"."${tableName}"`;

        // Check if table exists
        const existsQuery = `
          SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = $1 AND table_name = $2
          )
        `;
        const existsResult = await client.query(existsQuery, [schemaName, tableName]);

        if (existsResult.rows[0].exists) {
          this.logger.log(`Table ${fullTableName} already exists`);
          return { created: false, tableName };
        }

        // Build column definitions
        const columnDefs = columnMappings.map((col) => {
          let type = this.mapToPostgresType(col.dataType);

          if (col.transformation === 'objectIdToUuid') {
            type = 'UUID';
          }

          if (col.isPrimaryKey && type !== 'UUID') {
            if (type === 'INTEGER') type = 'SERIAL';
            if (type === 'BIGINT') type = 'BIGSERIAL';
          }

          const nullable = col.nullable ? '' : ' NOT NULL';
          const pk = col.isPrimaryKey ? ' PRIMARY KEY' : '';
          const defaultVal = col.defaultValue ? ` DEFAULT ${col.defaultValue}` : '';
          return `"${col.destinationColumn}" ${type}${nullable}${pk}${defaultVal}`;
        });

        const createQuery = `CREATE TABLE ${fullTableName} (${columnDefs.join(', ')})`;
        await client.query(createQuery);

        this.logger.log(`Created PostgreSQL table: ${fullTableName}`);
        return { created: true, tableName };
      } finally {
        client.release();
      }
    } finally {
      await pool.end();
    }
  }

  private async postgresTableExists(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
  ): Promise<boolean> {
    const sslConfig =
      typeof connectionConfig.ssl === 'object'
        ? connectionConfig.ssl.enabled
          ? { rejectUnauthorized: false }
          : undefined
        : connectionConfig.ssl
          ? { rejectUnauthorized: false }
          : undefined;

    const pool = new Pool({
      host: connectionConfig.host,
      port: connectionConfig.port,
      database: connectionConfig.database,
      user: connectionConfig.username,
      password: connectionConfig.password,
      ssl: sslConfig,
    });

    try {
      const client = await pool.connect();
      try {
        const query = `
          SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = $1 AND table_name = $2
          )
        `;
        const result = await client.query(query, [
          destinationSchema.destinationSchema || 'public',
          destinationSchema.destinationTable,
        ]);
        return result.rows[0].exists;
      } finally {
        client.release();
      }
    } finally {
      await pool.end();
    }
  }

  private mapToPostgresType(type: string): string {
    const typeMap: Record<string, string> = {
      string: 'TEXT',
      text: 'TEXT',
      varchar: 'VARCHAR(255)',
      number: 'NUMERIC',
      integer: 'INTEGER',
      bigint: 'BIGINT',
      float: 'DOUBLE PRECISION',
      double: 'DOUBLE PRECISION',
      boolean: 'BOOLEAN',
      date: 'DATE',
      timestamp: 'TIMESTAMP WITH TIME ZONE',
      datetime: 'TIMESTAMP WITH TIME ZONE',
      json: 'JSONB',
      object: 'JSONB',
      array: 'JSONB',
      uuid: 'UUID',
    };
    return typeMap[type.toLowerCase()] || 'TEXT';
  }

  // ============================================================================
  // MYSQL IMPLEMENTATION
  // ============================================================================

  private async emitToMySQL(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    rows: any[],
    writeMode: 'append' | 'upsert' | 'replace',
    upsertKey?: string[],
  ): Promise<WriteResult> {
    const connection = await mysql.createConnection({
      host: connectionConfig.host,
      port: connectionConfig.port,
      database: connectionConfig.database,
      user: connectionConfig.username,
      password: connectionConfig.password,
    });

    let rowsWritten = 0;
    const rowsSkipped = 0;
    let rowsFailed = 0;
    const errors: PipelineError[] = [];

    try {
      const schemaName = destinationSchema.destinationSchema || connectionConfig.database;
      const tableName = destinationSchema.destinationTable;
      const fullTableName = `\`${schemaName}\`.\`${tableName}\``;

      if (writeMode === 'replace') {
        await connection.execute(`TRUNCATE TABLE ${fullTableName}`);
      }

      for (let i = 0; i < rows.length; i += DEFAULT_BATCH_SIZE) {
        const batch = rows.slice(i, i + DEFAULT_BATCH_SIZE);

        try {
          await connection.beginTransaction();

          for (const row of batch) {
            const columns = Object.keys(row);
            const values = Object.values(row);
            const placeholders = columns.map(() => '?').join(', ');
            const columnList = columns.map((c) => `\`${c}\``).join(', ');

            if (writeMode === 'upsert' && upsertKey && upsertKey.length > 0) {
              // ROOT FIX: Use ON DUPLICATE KEY UPDATE for upsert
              const updateSet = columns
                .filter((c) => !upsertKey.includes(c))
                .map((c) => `\`${c}\` = VALUES(\`${c}\`)`)
                .join(', ');

              const query =
                updateSet.length > 0
                  ? `
                  INSERT INTO ${fullTableName} (${columnList})
                  VALUES (${placeholders})
                  ON DUPLICATE KEY UPDATE ${updateSet}
                `
                  : `
                  INSERT INTO ${fullTableName} (${columnList})
                  VALUES (${placeholders})
                  ON DUPLICATE KEY UPDATE ${columns[0]} = VALUES(${columns[0]})
                `;

              await connection.execute(query, values);
            } else {
              const query = `INSERT INTO ${fullTableName} (${columnList}) VALUES (${placeholders})`;
              await connection.execute(query, values);
            }

            rowsWritten++;
          }

          await connection.commit();
        } catch (error) {
          await connection.rollback();
          rowsFailed += batch.length;
          errors.push({ message: error instanceof Error ? error.message : String(error), row: i });
        }
      }

      return {
        rowsWritten,
        rowsSkipped,
        rowsFailed,
        errors: errors.length > 0 ? errors : undefined,
      };
    } finally {
      await connection.end();
    }
  }

  private async validateMySQLSchema(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    columnMappings: ColumnMapping[],
  ): Promise<SchemaValidationResult> {
    const connection = await mysql.createConnection({
      host: connectionConfig.host,
      port: connectionConfig.port,
      database: connectionConfig.database,
      user: connectionConfig.username,
      password: connectionConfig.password,
    });

    try {
      const [rows] = await connection.execute(
        `
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
      `,
        [
          destinationSchema.destinationSchema || connectionConfig.database,
          destinationSchema.destinationTable,
        ],
      );

      const existingColumns = new Map(
        (rows as any[]).map((r) => [
          r.COLUMN_NAME,
          { type: r.DATA_TYPE, nullable: r.IS_NULLABLE === 'YES' },
        ]),
      );

      const errors: string[] = [];
      const missingColumns: string[] = [];

      for (const mapping of columnMappings) {
        if (!existingColumns.has(mapping.destinationColumn)) {
          missingColumns.push(mapping.destinationColumn);
        }
      }

      if (missingColumns.length > 0) {
        errors.push(`Missing columns: ${missingColumns.join(', ')}`);
      }

      return { valid: errors.length === 0, errors, missingColumns };
    } finally {
      await connection.end();
    }
  }

  private async createMySQLTable(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    columnMappings: ColumnMapping[],
  ): Promise<{ created: boolean; tableName: string }> {
    const connection = await mysql.createConnection({
      host: connectionConfig.host,
      port: connectionConfig.port,
      database: connectionConfig.database,
      user: connectionConfig.username,
      password: connectionConfig.password,
    });

    try {
      const schemaName = destinationSchema.destinationSchema || connectionConfig.database;
      const tableName = destinationSchema.destinationTable;

      const columnDefs = columnMappings.map((col) => {
        const nullable = col.nullable ? '' : ' NOT NULL';
        const pk = col.isPrimaryKey ? ' PRIMARY KEY' : '';
        return `\`${col.destinationColumn}\` ${this.mapToMySQLType(col.dataType)}${nullable}${pk}`;
      });

      await connection.execute(
        `CREATE TABLE IF NOT EXISTS \`${schemaName}\`.\`${tableName}\` (${columnDefs.join(', ')})`,
      );

      return { created: true, tableName };
    } finally {
      await connection.end();
    }
  }

  private async mysqlTableExists(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
  ): Promise<boolean> {
    const connection = await mysql.createConnection({
      host: connectionConfig.host,
      port: connectionConfig.port,
      database: connectionConfig.database,
      user: connectionConfig.username,
      password: connectionConfig.password,
    });

    try {
      const [rows] = await connection.execute(
        `SELECT COUNT(*) as cnt FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
        [
          destinationSchema.destinationSchema || connectionConfig.database,
          destinationSchema.destinationTable,
        ],
      );
      return (rows as any[])[0].cnt > 0;
    } finally {
      await connection.end();
    }
  }

  private mapToMySQLType(type: string): string {
    const typeMap: Record<string, string> = {
      string: 'TEXT',
      text: 'TEXT',
      varchar: 'VARCHAR(255)',
      number: 'DECIMAL(18,2)',
      integer: 'INT',
      bigint: 'BIGINT',
      float: 'DOUBLE',
      double: 'DOUBLE',
      boolean: 'TINYINT(1)',
      date: 'DATE',
      timestamp: 'TIMESTAMP',
      datetime: 'DATETIME',
      json: 'JSON',
      object: 'JSON',
      array: 'JSON',
    };
    return typeMap[type.toLowerCase()] || 'TEXT';
  }

  // ============================================================================
  // MONGODB IMPLEMENTATION
  // ============================================================================

  private async emitToMongoDB(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    rows: any[],
    writeMode: 'append' | 'upsert' | 'replace',
    upsertKey?: string[],
  ): Promise<WriteResult> {
    const connectionString =
      connectionConfig.connection_string ||
      `mongodb://${connectionConfig.username}:${connectionConfig.password}@${connectionConfig.host}:${connectionConfig.port}/${connectionConfig.database}`;

    const client = new MongoClient(connectionString);

    try {
      await client.connect();
      const db = client.db(connectionConfig.database);
      const collection = db.collection(destinationSchema.destinationTable);

      let rowsWritten = 0;
      const rowsSkipped = 0;
      let rowsFailed = 0;

      if (writeMode === 'replace') {
        await collection.deleteMany({});
        this.logger.log(`Cleared collection ${destinationSchema.destinationTable} for replace mode`);
      }

      if (writeMode === 'upsert' && upsertKey && upsertKey.length > 0) {
        // ROOT FIX: Use bulkWrite with upsert for efficient data preservation
        const bulkOps = rows.map((row) => {
          const filter: any = {};
          upsertKey.forEach((key) => {
            filter[key] = row[key];
          });

          return {
            updateOne: {
              filter,
              update: { $set: row },
              upsert: true,
            },
          };
        });

        try {
          const result = await collection.bulkWrite(bulkOps, { ordered: false });
          rowsWritten = (result.upsertedCount || 0) + (result.modifiedCount || 0);
        } catch (bulkError: any) {
          // Partial success
          rowsWritten = bulkError.result?.nUpserted || 0;
          rowsFailed = rows.length - rowsWritten;
          this.logger.warn(`MongoDB bulk upsert partial failure: ${bulkError.message}`);
        }
      } else {
        // Bulk insert
        try {
          const result = await collection.insertMany(rows, { ordered: false });
          rowsWritten = result.insertedCount;
        } catch (insertError: any) {
          rowsWritten = insertError.result?.insertedCount || 0;
          rowsFailed = rows.length - rowsWritten;
          this.logger.warn(`MongoDB bulk insert partial failure: ${insertError.message}`);
        }
      }

      return { rowsWritten, rowsSkipped, rowsFailed };
    } finally {
      await client.close();
    }
  }

  // ============================================================================
  // HELPER METHODS
  // ============================================================================

  private areTypesCompatible(sourceType: string, destType: string): boolean {
    const normalizedSource = sourceType.toLowerCase();
    const normalizedDest = destType.toLowerCase();

    const stringTypes = ['string', 'text', 'varchar', 'char', 'character varying'];
    const numericTypes = [
      'number',
      'integer',
      'int',
      'bigint',
      'float',
      'double',
      'numeric',
      'decimal',
    ];
    const boolTypes = ['boolean', 'bool', 'tinyint'];
    const dateTypes = ['date', 'timestamp', 'datetime', 'timestamptz', 'timestamp with time zone'];

    const getTypeGroup = (type: string) => {
      if (stringTypes.some((t) => type.includes(t))) return 'string';
      if (numericTypes.some((t) => type.includes(t))) return 'numeric';
      if (boolTypes.some((t) => type.includes(t))) return 'boolean';
      if (dateTypes.some((t) => type.includes(t))) return 'date';
      return 'other';
    };

    return getTypeGroup(normalizedSource) === getTypeGroup(normalizedDest);
  }

  /**
   * Emit data to multiple entities (for bidirectional transformations)
   * This method handles writing transformed data to multiple destination tables/collections
   */
  async emitMultiEntity(options: {
    destinationSchema: PipelineDestinationSchema;
    organizationId: string;
    userId: string;
    entityData: Record<string, any[]>;
    writeMode?: 'append' | 'upsert' | 'replace';
    upsertKeys?: Record<string, string[]>;
  }): Promise<Record<string, WriteResult>> {
    const { destinationSchema, organizationId, userId, entityData, writeMode, upsertKeys } = options;

    const results: Record<string, WriteResult> = {};

    // Process each entity's data
    for (const [entityName, rows] of Object.entries(entityData)) {
      if (rows.length === 0) {
        results[entityName] = { rowsWritten: 0, rowsSkipped: 0, rowsFailed: 0 };
        continue;
      }

      // Create a copy of the destination schema with the entity name as the table
      const entitySchema = {
        ...destinationSchema,
        destTable: entityName,
        destCollection: entityName,
      };

      // Get upsert key for this entity if specified
      const entityUpsertKey = upsertKeys?.[entityName] || (destinationSchema.upsertKey as string[]);

      try {
        const result = await this.emit({
          destinationSchema: entitySchema,
          organizationId,
          userId,
          rows,
          writeMode: writeMode || 'append',
          upsertKey: entityUpsertKey,
        });

        results[entityName] = result;
      } catch (error) {
        this.logger.error(`Failed to emit to entity ${entityName}: ${error}`);
        results[entityName] = {
          rowsWritten: 0,
          rowsSkipped: 0,
          rowsFailed: rows.length,
          errors: [
            {
              message: error instanceof Error ? error.message : String(error),
            },
          ],
        };
      }
    }

    return results;
  }
}
