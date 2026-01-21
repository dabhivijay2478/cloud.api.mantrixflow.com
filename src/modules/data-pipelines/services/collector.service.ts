/**
 * Collector Service
 * Generic service for collecting data from any data source type
 * Supports: PostgreSQL, MySQL, MongoDB only
 *
 * Architecture: Collector → Emitter (with transformation) → Transformer (post-processing)
 * 
 * Uses handler registry pattern for extensibility
 */

import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { Pool } from 'pg';
import * as mysql from 'mysql2/promise';
import { MongoClient } from 'mongodb';
import { ConnectionService } from '../../data-sources/connection.service';
import { DataSourceRepository } from '../../data-sources/repositories/data-source.repository';
import { ActivityLogService } from '../../activity-logs/activity-log.service';
import { DATASOURCE_ACTIONS } from '../../activity-logs/constants/activity-log-types';
import type { ColumnInfo } from '../types/common.types';
import type { PipelineSourceSchema } from '../../../database/schemas';
import { createHandlerRegistry, getHandler } from './handlers/handler-registry';
import type { PipelineSourceSchemaWithConfig } from '../types/source-handler.types';

/**
 * Batch size constants
 * Default is 500 for balanced performance/memory
 */
const DEFAULT_BATCH_SIZE = 500;
const MAX_BATCH_SIZE = 10000;

/**
 * Retry configuration
 */
const RETRY_ATTEMPTS = 3;
const RETRY_DELAY_MS = 1000;

@Injectable()
export class CollectorService {
  private readonly logger = new Logger(CollectorService.name);
  private readonly handlerRegistry = createHandlerRegistry();

  constructor(
    private readonly connectionService: ConnectionService,
    private readonly dataSourceRepository: DataSourceRepository,
    private readonly activityLogService: ActivityLogService,
  ) {}

  /**
   * Collect data from source
   * Generic method that routes to type-specific handlers
   */
  async collect(options: {
    sourceSchema: PipelineSourceSchema;
    organizationId: string;
    userId: string;
    limit?: number;
    offset?: number;
    cursor?: string;
    incrementalColumn?: string;
    lastSyncValue?: any;
  }): Promise<{
    rows: any[];
    totalRows?: number;
    nextCursor?: string;
    hasMore?: boolean;
  }> {
    const {
      sourceSchema,
      organizationId,
      userId,
      limit = DEFAULT_BATCH_SIZE,
      offset = 0,
      cursor,
      incrementalColumn,
      lastSyncValue,
    } = options;

    // Validate batch size
    const effectiveLimit = Math.min(limit, MAX_BATCH_SIZE);

    // Get data source
    if (!sourceSchema.dataSourceId) {
      throw new BadRequestException('Source schema must have a data source ID');
    }

    const dataSource = await this.dataSourceRepository.findById(sourceSchema.dataSourceId);
    if (!dataSource) {
      throw new BadRequestException(`Data source ${sourceSchema.dataSourceId} not found`);
    }

    // Get decrypted connection config
    const connectionConfig = await this.connectionService.getDecryptedConnection(
      organizationId,
      sourceSchema.dataSourceId,
      userId,
    );

    // Use handler registry for collection
    const handler = getHandler(this.handlerRegistry, sourceSchema.sourceType);
    if (!handler) {
      throw new BadRequestException(`Unsupported source type: ${sourceSchema.sourceType}`);
    }

    // Convert PipelineSourceSchema to PipelineSourceSchemaWithConfig
    const sourceSchemaWithConfig: PipelineSourceSchemaWithConfig = {
      id: sourceSchema.id,
      organizationId: sourceSchema.organizationId,
      sourceType: sourceSchema.sourceType,
      dataSourceId: sourceSchema.dataSourceId || undefined,
      sourceSchema: sourceSchema.sourceSchema || undefined,
      sourceTable: sourceSchema.sourceTable || undefined,
      sourceQuery: sourceSchema.sourceQuery || undefined,
      sourceConfig: (sourceSchema.sourceConfig as any) || null,
      name: sourceSchema.name || undefined,
      config: {
        schema: sourceSchema.sourceSchema || undefined,
        table: sourceSchema.sourceTable || undefined,
        tableName: sourceSchema.sourceTable || undefined,
        database: connectionConfig.database || undefined,
        collection: sourceSchema.sourceTable || undefined,
        ...((sourceSchema.sourceConfig as any) || {}),
      },
    };

    // Route to handler
    let result: { rows: any[]; totalRows?: number; nextCursor?: string; hasMore?: boolean };

    try {
      result = await handler.collect(sourceSchemaWithConfig, connectionConfig, {
        limit: effectiveLimit,
        offset,
        cursor,
        incrementalColumn,
        lastSyncValue,
      });

      // Log successful collection
      this.logger.log(
        `Collected ${result.rows.length} rows from ${sourceSchema.sourceType} source`,
      );

      return result;
    } catch (error) {
      this.logger.error(
        `Collection failed for ${sourceSchema.sourceType}: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error;
    }
  }

  /**
   * Discover schema from source
   */
  async discoverSchema(options: {
    sourceSchema: PipelineSourceSchema;
    organizationId: string;
    userId: string;
  }): Promise<{
    columns: ColumnInfo[];
    primaryKeys: string[];
    estimatedRowCount?: number;
  }> {
    const { sourceSchema, organizationId, userId } = options;

    if (!sourceSchema.dataSourceId) {
      throw new BadRequestException('Source schema must have a data source ID for discovery');
    }

    const dataSource = await this.dataSourceRepository.findById(sourceSchema.dataSourceId);
    if (!dataSource) {
      throw new BadRequestException(`Data source ${sourceSchema.dataSourceId} not found`);
    }

    const connectionConfig = await this.connectionService.getDecryptedConnection(
      organizationId,
      sourceSchema.dataSourceId,
      userId,
    );

    // Use handler registry for schema discovery
    const handler = getHandler(this.handlerRegistry, sourceSchema.sourceType);
    if (!handler) {
      throw new BadRequestException(`Unsupported source type: ${sourceSchema.sourceType}`);
    }

    // Convert PipelineSourceSchema to PipelineSourceSchemaWithConfig
    const sourceSchemaWithConfig: PipelineSourceSchemaWithConfig = {
      id: sourceSchema.id,
      organizationId: sourceSchema.organizationId,
      sourceType: sourceSchema.sourceType,
      dataSourceId: sourceSchema.dataSourceId || undefined,
      sourceSchema: sourceSchema.sourceSchema || undefined,
      sourceTable: sourceSchema.sourceTable || undefined,
      sourceQuery: sourceSchema.sourceQuery || undefined,
      sourceConfig: (sourceSchema.sourceConfig as any) || null,
      name: sourceSchema.name || undefined,
      config: {
        schema: sourceSchema.sourceSchema || undefined,
        table: sourceSchema.sourceTable || undefined,
        tableName: sourceSchema.sourceTable || undefined,
        database: connectionConfig.database || undefined,
        collection: sourceSchema.sourceTable || undefined,
        ...((sourceSchema.sourceConfig as any) || {}),
      },
    };

    let result: { columns: ColumnInfo[]; primaryKeys: string[]; estimatedRowCount?: number };

    try {
      const schemaInfo = await handler.discoverSchema(sourceSchemaWithConfig, connectionConfig);
      result = {
        columns: schemaInfo.columns,
        primaryKeys: schemaInfo.primaryKeys,
        estimatedRowCount: schemaInfo.estimatedRowCount,
      };

      // Log schema discovery
      await this.activityLogService.logActivity({
        organizationId,
        userId,
        actionType: DATASOURCE_ACTIONS.SCHEMA_DISCOVERED,
        entityType: 'data_source',
        entityId: sourceSchema.dataSourceId,
        message: `Schema discovered: ${result.columns.length} columns, ${result.primaryKeys.length} primary keys`,
        metadata: {
          columnsCount: result.columns.length,
          primaryKeysCount: result.primaryKeys.length,
          estimatedRowCount: result.estimatedRowCount,
        },
      });

      return result;
    } catch (error) {
      this.logger.error(
        `Schema discovery failed: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error;
    }
  }

  // ============================================================================
  // POSTGRESQL IMPLEMENTATION
  // ============================================================================

  /**
   * Collect data from PostgreSQL using cursors for large datasets
   */
  private async collectFromPostgres(
    sourceSchema: PipelineSourceSchema,
    connectionConfig: any,
    limit: number,
    offset: number,
    _cursor?: string,
    incrementalColumn?: string,
    lastSyncValue?: any,
  ): Promise<{ rows: any[]; totalRows?: number; nextCursor?: string; hasMore?: boolean }> {
    const pool = new Pool({
      host: connectionConfig.host,
      port: connectionConfig.port,
      database: connectionConfig.database,
      user: connectionConfig.username,
      password: connectionConfig.password,
      ssl: connectionConfig.ssl?.enabled
        ? {
            rejectUnauthorized: connectionConfig.ssl?.reject_unauthorized !== false,
            ca: connectionConfig.ssl?.ca_cert,
          }
        : false,
      max: 5, // Connection pool size
    });

    try {
      const client = await pool.connect();

      try {
        // Build query
        let query: string;
        const params: any[] = [];

        if (sourceSchema.sourceQuery) {
          // Custom query with pagination
          query = `${sourceSchema.sourceQuery} LIMIT $1 OFFSET $2`;
          params.push(limit, offset);
        } else {
          // Table-based query
          const tableName = sourceSchema.sourceTable;
          const schemaName = sourceSchema.sourceSchema || 'public';
          const fullTableName = `"${schemaName}"."${tableName}"`;

          let whereClause = '';
          if (incrementalColumn && lastSyncValue) {
            whereClause = `WHERE "${incrementalColumn}" > $3`;
            params.push(limit, offset, lastSyncValue);
          } else {
            params.push(limit, offset);
          }

          query = `SELECT * FROM ${fullTableName} ${whereClause} LIMIT $1 OFFSET $2`;
        }

        // Execute query
        const result = await client.query(query, params);
        const rows = result.rows;

        // Get total count
        let totalRows: number | undefined;
        if (sourceSchema.sourceTable) {
          const countQuery = `SELECT count(*) FROM "${sourceSchema.sourceSchema || 'public'}"."${sourceSchema.sourceTable}"`;
          const countResult = await client.query(countQuery);
          totalRows = parseInt(countResult.rows[0].count, 10);
        }

        const hasMore = rows.length === limit;
        const nextCursor = hasMore ? String(offset + limit) : undefined;

        return { rows, totalRows, nextCursor, hasMore };
      } finally {
        client.release();
      }
    } finally {
      await pool.end();
    }
  }

  /**
   * Discover PostgreSQL schema
   */
  private async discoverPostgresSchema(
    sourceSchema: PipelineSourceSchema,
    connectionConfig: any,
  ): Promise<{ columns: ColumnInfo[]; primaryKeys: string[]; estimatedRowCount?: number }> {
    const pool = new Pool({
      host: connectionConfig.host,
      port: connectionConfig.port,
      database: connectionConfig.database,
      user: connectionConfig.username,
      password: connectionConfig.password,
      ssl: connectionConfig.ssl?.enabled ? { rejectUnauthorized: false } : false,
    });

    try {
      const client = await pool.connect();

      try {
        const tableName = sourceSchema.sourceTable;
        const schemaName = sourceSchema.sourceSchema || 'public';

        // Get column information
        const columnsQuery = `
          SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length,
            numeric_precision,
            numeric_scale
          FROM information_schema.columns
          WHERE table_schema = $1 AND table_name = $2
          ORDER BY ordinal_position
        `;
        const columnsResult = await client.query(columnsQuery, [schemaName, tableName]);

        // Get primary keys
        const pkQuery = `
          SELECT a.attname
          FROM pg_index i
          JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
          WHERE i.indrelid = $1::regclass AND i.indisprimary
        `;
        const pkResult = await client.query(pkQuery, [`"${schemaName}"."${tableName}"`]);
        const primaryKeys = pkResult.rows.map((row) => row.attname);

        // Get estimated row count
        const countQuery = `
          SELECT reltuples::bigint AS estimate
          FROM pg_class
          WHERE oid = $1::regclass
        `;
        const countResult = await client.query(countQuery, [`"${schemaName}"."${tableName}"`]);
        const estimatedRowCount = countResult.rows[0]?.estimate || 0;

        const columns: ColumnInfo[] = columnsResult.rows.map((row) => ({
          name: row.column_name,
          dataType: row.data_type,
          nullable: row.is_nullable === 'YES',
          defaultValue: row.column_default,
          maxLength: row.character_maximum_length,
          precision: row.numeric_precision,
          scale: row.numeric_scale,
          isPrimaryKey: primaryKeys.includes(row.column_name),
        }));

        return { columns, primaryKeys, estimatedRowCount };
      } finally {
        client.release();
      }
    } finally {
      await pool.end();
    }
  }

  // ============================================================================
  // MYSQL IMPLEMENTATION
  // ============================================================================

  /**
   * Collect data from MySQL with streaming support
   */
  private async collectFromMySQL(
    sourceSchema: PipelineSourceSchema,
    connectionConfig: any,
    limit: number,
    offset: number,
    _cursor?: string,
    incrementalColumn?: string,
    lastSyncValue?: any,
  ): Promise<{ rows: any[]; totalRows?: number; nextCursor?: string; hasMore?: boolean }> {
    const connection = await mysql.createConnection({
      host: connectionConfig.host,
      port: connectionConfig.port,
      database: connectionConfig.database,
      user: connectionConfig.username,
      password: connectionConfig.password,
      ssl: connectionConfig.ssl?.enabled
        ? {
            rejectUnauthorized: connectionConfig.ssl?.reject_unauthorized !== false,
            ca: connectionConfig.ssl?.ca_cert,
          }
        : undefined,
    });

    try {
      // Build query
      let query: string;
      const params: any[] = [];

      if (sourceSchema.sourceQuery) {
        query = `${sourceSchema.sourceQuery} LIMIT ? OFFSET ?`;
        params.push(limit, offset);
      } else {
        const tableName = sourceSchema.sourceTable;
        const schemaName = sourceSchema.sourceSchema || connectionConfig.database;

        let whereClause = '';
        if (incrementalColumn && lastSyncValue) {
          whereClause = `WHERE \`${incrementalColumn}\` > ?`;
          params.push(lastSyncValue);
        }

        query = `SELECT * FROM \`${schemaName}\`.\`${tableName}\` ${whereClause} LIMIT ? OFFSET ?`;
        params.push(limit, offset);
      }

      // Execute query
      const [rows] = await connection.execute(query, params);
      const resultRows = rows as any[];

      // Get total count
      let totalRows: number | undefined;
      if (sourceSchema.sourceTable) {
        const [countResult] = await connection.execute(
          `SELECT COUNT(*) as count FROM \`${sourceSchema.sourceSchema || connectionConfig.database}\`.\`${sourceSchema.sourceTable}\``,
        );
        totalRows = (countResult as any[])[0]?.count;
      }

      const hasMore = resultRows.length === limit;
      const nextCursor = hasMore ? String(offset + limit) : undefined;

      return { rows: resultRows, totalRows, nextCursor, hasMore };
    } finally {
      await connection.end();
    }
  }

  /**
   * Discover MySQL schema
   */
  private async discoverMySQLSchema(
    sourceSchema: PipelineSourceSchema,
    connectionConfig: any,
  ): Promise<{ columns: ColumnInfo[]; primaryKeys: string[]; estimatedRowCount?: number }> {
    const connection = await mysql.createConnection({
      host: connectionConfig.host,
      port: connectionConfig.port,
      database: connectionConfig.database,
      user: connectionConfig.username,
      password: connectionConfig.password,
    });

    try {
      const tableName = sourceSchema.sourceTable;
      const schemaName = sourceSchema.sourceSchema || connectionConfig.database;

      // Get column information
      const [columnsResult] = await connection.execute(
        `
        SELECT 
          COLUMN_NAME,
          DATA_TYPE,
          IS_NULLABLE,
          COLUMN_DEFAULT,
          CHARACTER_MAXIMUM_LENGTH,
          NUMERIC_PRECISION,
          NUMERIC_SCALE,
          COLUMN_KEY
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
      `,
        [schemaName, tableName],
      );

      const columns: ColumnInfo[] = (columnsResult as any[]).map((row) => ({
        name: row.COLUMN_NAME,
        dataType: row.DATA_TYPE,
        nullable: row.IS_NULLABLE === 'YES',
        defaultValue: row.COLUMN_DEFAULT,
        maxLength: row.CHARACTER_MAXIMUM_LENGTH,
        precision: row.NUMERIC_PRECISION,
        scale: row.NUMERIC_SCALE,
        isPrimaryKey: row.COLUMN_KEY === 'PRI',
      }));

      const primaryKeys = columns.filter((col) => col.isPrimaryKey).map((col) => col.name);

      // Get estimated row count
      const [countResult] = await connection.execute(
        `SELECT TABLE_ROWS as estimate FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
        [schemaName, tableName],
      );
      const estimatedRowCount = (countResult as any[])[0]?.estimate || 0;

      return { columns, primaryKeys, estimatedRowCount };
    } finally {
      await connection.end();
    }
  }

  // ============================================================================
  // MONGODB IMPLEMENTATION
  // ============================================================================

  /**
   * Collect data from MongoDB using find/aggregate
   */
  private async collectFromMongoDB(
    sourceSchema: PipelineSourceSchema,
    connectionConfig: any,
    limit: number,
    offset: number,
    cursor?: string,
    incrementalColumn?: string,
    lastSyncValue?: any,
  ): Promise<{ rows: any[]; totalRows?: number; nextCursor?: string; hasMore?: boolean }> {
    const connectionString =
      connectionConfig.connection_string ||
      `mongodb://${connectionConfig.username}:${connectionConfig.password}@${connectionConfig.host}:${connectionConfig.port}/${connectionConfig.database}`;

    const client = new MongoClient(connectionString);

    try {
      await client.connect();

      // For MongoDB: database can be in sourceSchema.sourceSchema (when created from UI)
      // or in connectionConfig.database (from connection config)
      // Collection is in sourceSchema.sourceTable
      const databaseName = sourceSchema.sourceSchema || connectionConfig.database;
      const collectionName = sourceSchema.sourceTable;

      if (!databaseName) {
        throw new Error(
          'MongoDB database name is required. Please specify database in connection config or source schema.',
        );
      }

      if (!collectionName) {
        throw new Error(
          'MongoDB collection name is required. Please specify collection in source schema.',
        );
      }

      this.logger.log(`MongoDB collect: database=${databaseName}, collection=${collectionName}`);

      const db = client.db(databaseName);
      const collection = db.collection(collectionName);

      // Build query options
      const findOptions: any = {
        limit,
        skip: offset,
      };

      // Use cursor-based pagination if available
      let query: any = {};
      if (cursor) {
        try {
          // Import ObjectId for cursor-based pagination
          const { ObjectId } = await import('mongodb');
          query = { _id: { $gt: new ObjectId(cursor) } };
        } catch {
          // Invalid ObjectId, ignore cursor
          this.logger.warn(`Invalid cursor format: ${cursor}, ignoring cursor-based pagination`);
        }
      }

      // Add incremental filter if provided
      if (incrementalColumn && lastSyncValue) {
        this.logger.log(`MongoDB incremental sync: ${incrementalColumn} > ${lastSyncValue}`);
        // Handle different types of lastSyncValue
        let filterValue = lastSyncValue;
        
        // Try to parse as ObjectId if it looks like one
        if (incrementalColumn === '_id' && typeof lastSyncValue === 'string' && lastSyncValue.length === 24) {
          try {
            const { ObjectId } = await import('mongodb');
            filterValue = new ObjectId(lastSyncValue);
          } catch {
            // Keep as string
          }
        }
        // Try to parse as Date if it's an ISO string
        else if (typeof lastSyncValue === 'string' && lastSyncValue.match(/^\d{4}-\d{2}-\d{2}/)) {
          filterValue = new Date(lastSyncValue);
        }
        
        query[incrementalColumn] = { $gt: filterValue };
      }

      this.logger.log(`MongoDB query: ${JSON.stringify(query)}, limit=${limit}, skip=${offset}`);

      // Execute query
      const rows = await collection.find(query, findOptions).toArray();

      this.logger.log(`MongoDB found ${rows.length} documents`);

      // Get total count (with filter for incremental)
      const totalRows = await collection.countDocuments(query);

      const hasMore = rows.length === limit;
      const nextCursor = hasMore && rows.length > 0 ? String(rows[rows.length - 1]._id) : undefined;

      // Convert ObjectId to string for serialization
      const serializedRows = rows.map((row) => ({
        ...row,
        _id: row._id?.toString(),
      }));

      return { rows: serializedRows, totalRows, nextCursor, hasMore };
    } finally {
      await client.close();
    }
  }

  /**
   * Discover MongoDB schema by sampling documents
   */
  private async discoverMongoDBSchema(
    sourceSchema: PipelineSourceSchema,
    connectionConfig: any,
  ): Promise<{ columns: ColumnInfo[]; primaryKeys: string[]; estimatedRowCount?: number }> {
    const connectionString =
      connectionConfig.connection_string ||
      `mongodb://${connectionConfig.username}:${connectionConfig.password}@${connectionConfig.host}:${connectionConfig.port}/${connectionConfig.database}`;

    const client = new MongoClient(connectionString);

    try {
      await client.connect();
      
      // For MongoDB: database can be in sourceSchema.sourceSchema (when created from UI)
      // or in connectionConfig.database (from connection config)
      // Collection is in sourceSchema.sourceTable
      const databaseName = sourceSchema.sourceSchema || connectionConfig.database;
      const collectionName = sourceSchema.sourceTable;
      
      if (!databaseName) {
        throw new Error('MongoDB database name is required. Please specify database in connection config or source schema.');
      }
      
      if (!collectionName) {
        throw new Error('MongoDB collection name is required. Please specify collection in source schema.');
      }
      
      this.logger.log(`MongoDB discoverSchema: database=${databaseName}, collection=${collectionName}`);
      
      const db = client.db(databaseName);
      const collection = db.collection(collectionName);

      // Sample documents to infer schema
      const sampleDocs = await collection.find().limit(100).toArray();
      const estimatedRowCount = await collection.estimatedDocumentCount();

      // Infer schema from sample
      const fieldMap = new Map<string, { types: Set<string>; nullable: boolean }>();

      for (const doc of sampleDocs) {
        this.extractMongoFields(doc, '', fieldMap);
      }

      const columns: ColumnInfo[] = Array.from(fieldMap.entries()).map(([name, info]) => ({
        name,
        dataType: Array.from(info.types).join(' | '),
        nullable: info.nullable,
        isPrimaryKey: name === '_id',
      }));

      return {
        columns,
        primaryKeys: ['_id'],
        estimatedRowCount,
      };
    } finally {
      await client.close();
    }
  }

  /**
   * Extract fields from MongoDB document for schema inference
   */
  private extractMongoFields(
    obj: any,
    prefix: string,
    fieldMap: Map<string, { types: Set<string>; nullable: boolean }>,
  ): void {
    for (const [key, value] of Object.entries(obj)) {
      const fieldName = prefix ? `${prefix}.${key}` : key;

      if (!fieldMap.has(fieldName)) {
        fieldMap.set(fieldName, { types: new Set(), nullable: false });
      }

      const fieldInfo = fieldMap.get(fieldName)!;

      if (value === null || value === undefined) {
        fieldInfo.nullable = true;
      } else if (Array.isArray(value)) {
        fieldInfo.types.add('array');
      } else if (typeof value === 'object' && value.constructor === Object) {
        fieldInfo.types.add('object');
        this.extractMongoFields(value, fieldName, fieldMap);
      } else {
        fieldInfo.types.add(typeof value);
      }
    }
  }

  /**
   * Collect incremental data using handler's collectIncremental method
   * ROOT FIX: Uses strict incremental filtering to prevent re-writing all data
   */
  async collectIncremental(options: {
    sourceSchema: PipelineSourceSchema;
    organizationId: string;
    userId: string;
    checkpoint: { watermarkField: string; lastValue: string | number; pauseTimestamp?: string };
    limit?: number;
    offset?: number;
    cursor?: string;
  }): Promise<{
    rows: any[];
    totalRows?: number;
    nextCursor?: string;
    hasMore?: boolean;
  }> {
    const {
      sourceSchema,
      organizationId,
      userId,
      checkpoint,
      limit = DEFAULT_BATCH_SIZE,
      offset = 0,
      cursor,
    } = options;

    // Validate batch size
    const effectiveLimit = Math.min(limit, MAX_BATCH_SIZE);

    // Get data source
    if (!sourceSchema.dataSourceId) {
      throw new BadRequestException('Source schema must have a data source ID');
    }

    const dataSource = await this.dataSourceRepository.findById(sourceSchema.dataSourceId);
    if (!dataSource) {
      throw new BadRequestException(`Data source ${sourceSchema.dataSourceId} not found`);
    }

    // Get decrypted connection config
    const connectionConfig = await this.connectionService.getDecryptedConnection(
      organizationId,
      sourceSchema.dataSourceId,
      userId,
    );

    // Use handler registry for incremental collection
    const handler = getHandler(this.handlerRegistry, sourceSchema.sourceType);
    if (!handler) {
      throw new BadRequestException(`Unsupported source type: ${sourceSchema.sourceType}`);
    }

    // Convert PipelineSourceSchema to PipelineSourceSchemaWithConfig
    const sourceSchemaWithConfig: PipelineSourceSchemaWithConfig = {
      id: sourceSchema.id,
      organizationId: sourceSchema.organizationId,
      sourceType: sourceSchema.sourceType,
      dataSourceId: sourceSchema.dataSourceId || undefined,
      sourceSchema: sourceSchema.sourceSchema || undefined,
      sourceTable: sourceSchema.sourceTable || undefined,
      sourceQuery: sourceSchema.sourceQuery || undefined,
      sourceConfig: (sourceSchema.sourceConfig as any) || null,
      name: sourceSchema.name || undefined,
      config: {
        schema: sourceSchema.sourceSchema || undefined,
        table: sourceSchema.sourceTable || undefined,
        tableName: sourceSchema.sourceTable || undefined,
        database: connectionConfig.database || undefined,
        collection: sourceSchema.sourceTable || undefined,
        ...((sourceSchema.sourceConfig as any) || {}),
      },
    };

    // Use handler's collectIncremental method for strict incremental filtering
    try {
      const result = await handler.collectIncremental(
        sourceSchemaWithConfig,
        connectionConfig,
        checkpoint,
        {
          limit: effectiveLimit,
          offset,
          cursor,
        },
      );

      this.logger.log(
        `Incremental collection: ${result.rows.length} rows from ${sourceSchema.sourceType} source`,
      );

      return result;
    } catch (error) {
      this.logger.error(
        `Incremental collection failed for ${sourceSchema.sourceType}: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error;
    }
  }

  // ============================================================================
  // REMOVED: S3, API, BigQuery, Snowflake implementations
  // Only PostgreSQL, MySQL, MongoDB are supported
  // All collection now uses handler registry pattern
  // ============================================================================
}
