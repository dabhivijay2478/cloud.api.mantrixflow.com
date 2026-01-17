/**
 * Emitter Service
 * Generic service for emitting/writing data to any destination type
 * Supports: PostgreSQL, MySQL, MongoDB, S3, REST API, BigQuery, Snowflake
 *
 * Architecture: Collector → Emitter (with transformation) → Transformer (post-processing)
 * Transformation happens during emission for efficiency
 */

import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { Pool } from 'pg';
import * as mysql from 'mysql2/promise';
import { MongoClient } from 'mongodb';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { BigQuery } from '@google-cloud/bigquery';
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
import { firstValueFrom } from 'rxjs';

/**
 * Batch size for bulk operations
 */
const DEFAULT_BATCH_SIZE = 1000;
const RETRY_ATTEMPTS = 3;
const RETRY_DELAY_MS = 1000;

@Injectable()
export class EmitterService {
  private readonly logger = new Logger(EmitterService.name);

  constructor(
    private readonly connectionService: ConnectionService,
    private readonly dataSourceRepository: DataSourceRepository,
    private readonly transformerService: TransformerService,

    private readonly httpService: HttpService,
  ) {}

  /**
   * Write data to destination
   * Architecture: Collector → Emitter (with transformation) → Transformer (post-processing)
   * Transformation happens during emission
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

    const connectionConfig = await this.connectionService.getDecryptedConnection(
      organizationId,
      destinationSchema.dataSourceId,
      userId,
    );

    // STEP 1: Transform data during emission (Collector → Emitter with transformation)
    const effectiveMappings =
      columnMappings.length > 0
        ? columnMappings
        : (destinationSchema.columnMappings as ColumnMapping[]) || [];

    const transformedRows = await this.transformerService.transform(
      rows,
      effectiveMappings,
      transformations.length > 0 ? transformations : undefined,
    );

    // STEP 2: Emit transformed data to destination in batches
    let writeResult: WriteResult;

    try {
      switch (dataSource.sourceType) {
        case 'postgres':
          writeResult = await this.emitToPostgres(
            destinationSchema,
            connectionConfig,
            transformedRows,
            writeMode,
            upsertKey || (destinationSchema.upsertKey as string[]),
          );
          break;
        case 'mysql':
          writeResult = await this.emitToMySQL(
            destinationSchema,
            connectionConfig,
            transformedRows,
            writeMode,
            upsertKey || (destinationSchema.upsertKey as string[]),
          );
          break;
        case 'mongodb':
          writeResult = await this.emitToMongoDB(
            destinationSchema,
            connectionConfig,
            transformedRows,
            writeMode,
            upsertKey || (destinationSchema.upsertKey as string[]),
          );
          break;
        case 's3':
          writeResult = await this.emitToS3(
            destinationSchema,
            connectionConfig,
            transformedRows,
            writeMode,
          );
          break;
        case 'api':
          writeResult = await this.emitToAPI(destinationSchema, connectionConfig, transformedRows);
          break;
        case 'bigquery':
          writeResult = await this.emitToBigQuery(
            destinationSchema,
            connectionConfig,
            transformedRows,
            writeMode,
          );
          break;
        case 'snowflake':
          writeResult = await this.emitToSnowflake(
            destinationSchema,
            connectionConfig,
            transformedRows,
            writeMode,
          );
          break;
        default:
          throw new BadRequestException(`Unsupported destination type: ${dataSource.sourceType}`);
      }

      // Log successful emission
      this.logger.log(
        `Emitted ${writeResult.rowsWritten} rows to ${dataSource.sourceType} destination`,
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
      case 's3':
        return { valid: true, errors: [], warnings: ['S3 schema validation not required'] };
      case 'api':
        return { valid: true, errors: [], warnings: ['API schema validation not required'] };
      case 'bigquery':
        return this.validateBigQuerySchema(destinationSchema, connectionConfig, columnMappings);
      case 'snowflake':
        return this.validateSnowflakeSchema(destinationSchema, connectionConfig, columnMappings);
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
      case 'bigquery':
        return this.createBigQueryTable(destinationSchema, connectionConfig, columnMappings);
      case 'snowflake':
        return this.createSnowflakeTable(destinationSchema, connectionConfig, columnMappings);
      case 'mongodb':
        // MongoDB doesn't need table creation
        return { created: false, tableName: destinationSchema.destinationTable };
      case 's3':
      case 'api':
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
      case 'bigquery':
        return this.bigQueryTableExists(destinationSchema, connectionConfig);
      case 'snowflake':
        return this.snowflakeTableExists(destinationSchema, connectionConfig);
      case 'mongodb':
      case 's3':
      case 'api':
        return true; // These don't require table existence checks
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
      // ... rest of implementation (unchanged logic) ...
      try {
        const schemaName = destinationSchema.destinationSchema || 'public';
        const tableName = destinationSchema.destinationTable;
        const fullTableName = `"${schemaName}"."${tableName}"`;

        // Process in batches
        for (let i = 0; i < rows.length; i += DEFAULT_BATCH_SIZE) {
          const batch = rows.slice(i, i + DEFAULT_BATCH_SIZE);

          try {
            await client.query('BEGIN');

            if (writeMode === 'replace' && i === 0) {
              await client.query(`TRUNCATE ${fullTableName}`);
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
                // UPSERT using ON CONFLICT
                const conflictTarget = upsertKey.map((k) => `"${k}"`).join(', ');
                const updateSet = columns
                  .filter((c) => !upsertKey.includes(c))
                  .map((c) => `"${c}" = EXCLUDED."${c}"`)
                  .join(', ');

                const query = `
                  INSERT INTO ${fullTableName} (${columnList})
                  VALUES (${placeholders})
                  ON CONFLICT (${conflictTarget}) DO UPDATE SET ${updateSet}
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

        // Get existing columns
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
            // Check type compatibility
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

        // Build column definitions
        const columnDefs = columnMappings.map((col) => {
          let type = this.mapToPostgresType(col.dataType);

          // Use SERIAL/BIGSERIAL for auto-incrementing primary keys
          if (col.isPrimaryKey) {
            if (type === 'INTEGER') type = 'SERIAL';
            if (type === 'BIGINT') type = 'BIGSERIAL';
          }

          const nullable = col.nullable ? '' : ' NOT NULL';
          const pk = col.isPrimaryKey ? ' PRIMARY KEY' : '';
          const defaultVal = col.defaultValue ? ` DEFAULT ${col.defaultValue}` : '';
          return `"${col.destinationColumn}" ${type}${nullable}${pk}${defaultVal}`;
        });

        const createQuery = `CREATE TABLE IF NOT EXISTS ${fullTableName} (${columnDefs.join(', ')})`;
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
              const updateSet = columns
                .filter((c) => !upsertKey.includes(c))
                .map((c) => `\`${c}\` = VALUES(\`${c}\`)`)
                .join(', ');

              const query = `
                INSERT INTO ${fullTableName} (${columnList})
                VALUES (${placeholders})
                ON DUPLICATE KEY UPDATE ${updateSet}
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
      }

      if (writeMode === 'upsert' && upsertKey && upsertKey.length > 0) {
        // Upsert each document
        for (const row of rows) {
          try {
            const filter: any = {};
            upsertKey.forEach((key) => {
              filter[key] = row[key];
            });

            await collection.updateOne(filter, { $set: row }, { upsert: true });
            rowsWritten++;
          } catch (_error) {
            rowsFailed++;
          }
        }
      } else {
        // Bulk insert
        const result = await collection.insertMany(rows, { ordered: false });
        rowsWritten = result.insertedCount;
      }

      return { rowsWritten, rowsSkipped, rowsFailed };
    } finally {
      await client.close();
    }
  }

  // ============================================================================
  // S3 IMPLEMENTATION
  // ============================================================================

  private async emitToS3(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    rows: any[],
    _writeMode: 'append' | 'upsert' | 'replace',
  ): Promise<WriteResult> {
    const s3Client = new S3Client({
      region: connectionConfig.region,
      credentials: {
        accessKeyId: connectionConfig.access_key_id,
        secretAccessKey: connectionConfig.secret_access_key,
      },
    });

    const bucket = connectionConfig.bucket;
    const prefix = destinationSchema.destinationTable;
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const key = `${prefix}/${timestamp}.json`;

    try {
      const content = JSON.stringify(rows, null, 2);

      const command = new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: content,
        ContentType: 'application/json',
      });

      await s3Client.send(command);

      this.logger.log(`Uploaded ${rows.length} rows to s3://${bucket}/${key}`);

      return { rowsWritten: rows.length, rowsSkipped: 0, rowsFailed: 0 };
    } catch (error) {
      this.logger.error(`S3 upload failed: ${error}`);
      throw error;
    }
  }

  // ============================================================================
  // REST API IMPLEMENTATION
  // ============================================================================

  private async emitToAPI(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    rows: any[],
  ): Promise<WriteResult> {
    const baseUrl = connectionConfig.base_url;
    const endpoint = destinationSchema.destinationTable; // Use table name as endpoint
    const url = new URL(endpoint, baseUrl);

    let rowsWritten = 0;
    let rowsFailed = 0;
    const errors: PipelineError[] = [];

    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };

    // Add authentication
    switch (connectionConfig.auth_type) {
      case 'bearer':
        headers.Authorization = `Bearer ${connectionConfig.auth_token}`;
        break;
      case 'api_key':
        headers['X-API-Key'] = connectionConfig.api_key;
        break;
    }

    // Send data in batches
    for (let i = 0; i < rows.length; i += 100) {
      const batch = rows.slice(i, i + 100);

      for (let attempt = 0; attempt < RETRY_ATTEMPTS; attempt++) {
        try {
          await firstValueFrom(this.httpService.post(url.toString(), batch, { headers }));
          rowsWritten += batch.length;
          break;
        } catch (error) {
          if (attempt === RETRY_ATTEMPTS - 1) {
            rowsFailed += batch.length;
            errors.push({
              message: error instanceof Error ? error.message : String(error),
              row: i,
            });
          } else {
            await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS * (attempt + 1)));
          }
        }
      }
    }

    return {
      rowsWritten,
      rowsSkipped: 0,
      rowsFailed,
      errors: errors.length > 0 ? errors : undefined,
    };
  }

  // ============================================================================
  // BIGQUERY IMPLEMENTATION
  // ============================================================================

  private async emitToBigQuery(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    rows: any[],
    writeMode: 'append' | 'upsert' | 'replace',
  ): Promise<WriteResult> {
    const bigquery = new BigQuery({
      projectId: connectionConfig.project_id,
      credentials: connectionConfig.credentials,
    });

    const dataset = connectionConfig.dataset;
    const tableName = destinationSchema.destinationTable;

    try {
      const table = bigquery.dataset(dataset).table(tableName);

      // Configure write disposition
      const _writeDisposition = writeMode === 'replace' ? 'WRITE_TRUNCATE' : 'WRITE_APPEND';

      await table.insert(rows, {
        skipInvalidRows: true,
        ignoreUnknownValues: true,
      });

      return { rowsWritten: rows.length, rowsSkipped: 0, rowsFailed: 0 };
    } catch (error) {
      this.logger.error(`BigQuery insert failed: ${error}`);
      throw error;
    }
  }

  private async validateBigQuerySchema(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    columnMappings: ColumnMapping[],
  ): Promise<SchemaValidationResult> {
    const bigquery = new BigQuery({
      projectId: connectionConfig.project_id,
      credentials: connectionConfig.credentials,
    });

    try {
      const [metadata] = await bigquery
        .dataset(connectionConfig.dataset)
        .table(destinationSchema.destinationTable)
        .getMetadata();

      const existingFields = new Set(metadata.schema.fields.map((f: any) => f.name));
      const missingColumns = columnMappings
        .filter((m) => !existingFields.has(m.destinationColumn))
        .map((m) => m.destinationColumn);

      return {
        valid: missingColumns.length === 0,
        errors: missingColumns.length > 0 ? [`Missing columns: ${missingColumns.join(', ')}`] : [],
        missingColumns,
      };
    } catch (error) {
      return { valid: false, errors: [String(error)] };
    }
  }

  private async createBigQueryTable(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    columnMappings: ColumnMapping[],
  ): Promise<{ created: boolean; tableName: string }> {
    const bigquery = new BigQuery({
      projectId: connectionConfig.project_id,
      credentials: connectionConfig.credentials,
    });

    const schema = columnMappings.map((col) => ({
      name: col.destinationColumn,
      type: this.mapToBigQueryType(col.dataType),
      mode: col.nullable ? 'NULLABLE' : 'REQUIRED',
    }));

    await bigquery
      .dataset(connectionConfig.dataset)
      .createTable(destinationSchema.destinationTable, {
        schema,
      });

    return { created: true, tableName: destinationSchema.destinationTable };
  }

  private async bigQueryTableExists(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
  ): Promise<boolean> {
    const bigquery = new BigQuery({
      projectId: connectionConfig.project_id,
      credentials: connectionConfig.credentials,
    });

    try {
      const [exists] = await bigquery
        .dataset(connectionConfig.dataset)
        .table(destinationSchema.destinationTable)
        .exists();
      return exists;
    } catch {
      return false;
    }
  }

  private mapToBigQueryType(type: string): string {
    const typeMap: Record<string, string> = {
      string: 'STRING',
      text: 'STRING',
      number: 'FLOAT64',
      integer: 'INT64',
      bigint: 'INT64',
      float: 'FLOAT64',
      double: 'FLOAT64',
      boolean: 'BOOL',
      date: 'DATE',
      timestamp: 'TIMESTAMP',
      datetime: 'DATETIME',
      json: 'JSON',
      object: 'JSON',
      array: 'ARRAY',
    };
    return typeMap[type.toLowerCase()] || 'STRING';
  }

  // ============================================================================
  // SNOWFLAKE IMPLEMENTATION
  // ============================================================================

  private async emitToSnowflake(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    rows: any[],
    writeMode: 'append' | 'upsert' | 'replace',
  ): Promise<WriteResult> {
    const snowflake = await import('snowflake-sdk');

    return new Promise((resolve, reject) => {
      const connection = snowflake.createConnection({
        account: connectionConfig.account,
        username: connectionConfig.username,
        password: connectionConfig.password,
        warehouse: connectionConfig.warehouse,
        database: connectionConfig.database,
        schema: connectionConfig.schema,
      });

      connection.connect((err, conn) => {
        if (err) {
          reject(err);
          return;
        }

        const schema = destinationSchema.destinationSchema || connectionConfig.schema;
        const tableName = destinationSchema.destinationTable;

        const insertRows = async () => {
          let rowsWritten = 0;

          if (writeMode === 'replace') {
            await new Promise<void>((res, rej) => {
              conn.execute({
                sqlText: `TRUNCATE TABLE "${schema}"."${tableName}"`,
                complete: (err) => (err ? rej(err) : res()),
              });
            });
          }

          for (const row of rows) {
            const columns = Object.keys(row);
            const values = Object.values(row).map((v) =>
              typeof v === 'string' ? `'${v.replace(/'/g, "''")}'` : v,
            );

            await new Promise<void>((res, rej) => {
              conn.execute({
                sqlText: `INSERT INTO "${schema}"."${tableName}" (${columns.join(', ')}) VALUES (${values.join(', ')})`,
                complete: (err) => {
                  if (err) {
                    rej(err);
                  } else {
                    rowsWritten++;
                    res();
                  }
                },
              });
            });
          }

          return rowsWritten;
        };

        insertRows()
          .then((rowsWritten) => {
            connection.destroy(() => {});
            resolve({ rowsWritten, rowsSkipped: 0, rowsFailed: 0 });
          })
          .catch((error) => {
            connection.destroy(() => {});
            reject(error);
          });
      });
    });
  }

  private async validateSnowflakeSchema(
    _destinationSchema: PipelineDestinationSchema,
    _connectionConfig: any,
    _columnMappings: ColumnMapping[],
  ): Promise<SchemaValidationResult> {
    // Simplified validation - return success for now
    return {
      valid: true,
      errors: [],
      warnings: ['Full Snowflake schema validation not implemented'],
    };
  }

  private async createSnowflakeTable(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    columnMappings: ColumnMapping[],
  ): Promise<{ created: boolean; tableName: string }> {
    const snowflake = await import('snowflake-sdk');

    return new Promise((resolve, reject) => {
      const connection = snowflake.createConnection({
        account: connectionConfig.account,
        username: connectionConfig.username,
        password: connectionConfig.password,
        warehouse: connectionConfig.warehouse,
        database: connectionConfig.database,
        schema: connectionConfig.schema,
      });

      connection.connect((err, conn) => {
        if (err) {
          reject(err);
          return;
        }

        const schema = destinationSchema.destinationSchema || connectionConfig.schema;
        const tableName = destinationSchema.destinationTable;

        const columnDefs = columnMappings
          .map((col) => `"${col.destinationColumn}" ${this.mapToSnowflakeType(col.dataType)}`)
          .join(', ');

        conn.execute({
          sqlText: `CREATE TABLE IF NOT EXISTS "${schema}"."${tableName}" (${columnDefs})`,
          complete: (err) => {
            connection.destroy(() => {});
            if (err) {
              reject(err);
            } else {
              resolve({ created: true, tableName });
            }
          },
        });
      });
    });
  }

  private async snowflakeTableExists(
    _destinationSchema: PipelineDestinationSchema,
    _connectionConfig: any,
  ): Promise<boolean> {
    // Simplified check
    return false;
  }

  private mapToSnowflakeType(type: string): string {
    const typeMap: Record<string, string> = {
      string: 'VARCHAR',
      text: 'VARCHAR',
      number: 'NUMBER',
      integer: 'INTEGER',
      bigint: 'BIGINT',
      float: 'FLOAT',
      double: 'DOUBLE',
      boolean: 'BOOLEAN',
      date: 'DATE',
      timestamp: 'TIMESTAMP_NTZ',
      datetime: 'TIMESTAMP_NTZ',
      json: 'VARIANT',
      object: 'VARIANT',
      array: 'ARRAY',
    };
    return typeMap[type.toLowerCase()] || 'VARCHAR';
  }

  // ============================================================================
  // HELPER METHODS
  // ============================================================================

  private areTypesCompatible(sourceType: string, destType: string): boolean {
    const normalizedSource = sourceType.toLowerCase();
    const normalizedDest = destType.toLowerCase();

    // Basic type compatibility check
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
}
