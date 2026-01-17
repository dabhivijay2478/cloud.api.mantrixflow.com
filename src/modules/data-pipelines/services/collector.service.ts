/**
 * Collector Service
 * Generic service for collecting data from any data source type
 * Supports: PostgreSQL, MySQL, MongoDB, S3, REST API, BigQuery, Snowflake
 *
 * Architecture: Collector → Emitter (with transformation) → Transformer (post-processing)
 */

import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { Pool } from 'pg';
import * as mysql from 'mysql2/promise';
import { MongoClient } from 'mongodb';
import { S3Client, GetObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { BigQuery } from '@google-cloud/bigquery';
import { ConnectionService } from '../../data-sources/connection.service';
import { DataSourceRepository } from '../../data-sources/repositories/data-source.repository';
import { ActivityLogService } from '../../activity-logs/activity-log.service';
import { DATASOURCE_ACTIONS } from '../../activity-logs/constants/activity-log-types';
import type { ColumnInfo } from '../types/common.types';
import type { PipelineSourceSchema } from '../../../database/schemas';
import { firstValueFrom } from 'rxjs';

/**
 * Batch size constants
 */
const DEFAULT_BATCH_SIZE = 1000;
const MAX_BATCH_SIZE = 10000;

/**
 * Retry configuration
 */
const RETRY_ATTEMPTS = 3;
const RETRY_DELAY_MS = 1000;

@Injectable()
export class CollectorService {
  private readonly logger = new Logger(CollectorService.name);

  constructor(
    private readonly connectionService: ConnectionService,
    private readonly dataSourceRepository: DataSourceRepository,
    private readonly activityLogService: ActivityLogService,
    private readonly httpService: HttpService,
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

    // Route to appropriate collector based on source type
    let result: { rows: any[]; totalRows?: number; nextCursor?: string; hasMore?: boolean };

    try {
      switch (sourceSchema.sourceType) {
        case 'postgres':
          result = await this.collectFromPostgres(
            sourceSchema,
            connectionConfig,
            effectiveLimit,
            offset,
            cursor,
            incrementalColumn,
            lastSyncValue,
          );
          break;
        case 'mysql':
          result = await this.collectFromMySQL(
            sourceSchema,
            connectionConfig,
            effectiveLimit,
            offset,
            cursor,
            incrementalColumn,
            lastSyncValue,
          );
          break;
        case 'mongodb':
          result = await this.collectFromMongoDB(
            sourceSchema,
            connectionConfig,
            effectiveLimit,
            offset,
            cursor,
          );
          break;
        case 's3':
          result = await this.collectFromS3(
            sourceSchema,
            connectionConfig,
            effectiveLimit,
            offset,
            cursor,
          );
          break;
        case 'api':
          result = await this.collectFromAPI(
            sourceSchema,
            connectionConfig,
            effectiveLimit,
            offset,
            cursor,
          );
          break;
        case 'bigquery':
          result = await this.collectFromBigQuery(
            sourceSchema,
            connectionConfig,
            effectiveLimit,
            offset,
          );
          break;
        case 'snowflake':
          result = await this.collectFromSnowflake(
            sourceSchema,
            connectionConfig,
            effectiveLimit,
            offset,
          );
          break;
        default:
          throw new BadRequestException(`Unsupported source type: ${sourceSchema.sourceType}`);
      }

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

    let result: { columns: ColumnInfo[]; primaryKeys: string[]; estimatedRowCount?: number };

    try {
      switch (sourceSchema.sourceType) {
        case 'postgres':
          result = await this.discoverPostgresSchema(sourceSchema, connectionConfig);
          break;
        case 'mysql':
          result = await this.discoverMySQLSchema(sourceSchema, connectionConfig);
          break;
        case 'mongodb':
          result = await this.discoverMongoDBSchema(sourceSchema, connectionConfig);
          break;
        case 's3':
          result = await this.discoverS3Schema(sourceSchema, connectionConfig);
          break;
        case 'api':
          result = await this.discoverAPISchema(sourceSchema, connectionConfig);
          break;
        case 'bigquery':
          result = await this.discoverBigQuerySchema(sourceSchema, connectionConfig);
          break;
        case 'snowflake':
          result = await this.discoverSnowflakeSchema(sourceSchema, connectionConfig);
          break;
        default:
          throw new BadRequestException(`Unsupported source type: ${sourceSchema.sourceType}`);
      }

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
  ): Promise<{ rows: any[]; totalRows?: number; nextCursor?: string; hasMore?: boolean }> {
    const connectionString =
      connectionConfig.connection_string ||
      `mongodb://${connectionConfig.username}:${connectionConfig.password}@${connectionConfig.host}:${connectionConfig.port}/${connectionConfig.database}`;

    const client = new MongoClient(connectionString);

    try {
      await client.connect();
      const db = client.db(connectionConfig.database);
      const collection = db.collection(sourceSchema.sourceTable || 'documents');

      // Build query options
      const findOptions: any = {
        limit,
        skip: offset,
      };

      // Use cursor-based pagination if available
      let query: any = {};
      if (cursor) {
        query = { _id: { $gt: cursor } };
      }

      // Execute query
      const rows = await collection.find(query, findOptions).toArray();

      // Get total count
      const totalRows = await collection.countDocuments();

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
      const db = client.db(connectionConfig.database);
      const collection = db.collection(sourceSchema.sourceTable || 'documents');

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

  // ============================================================================
  // S3 IMPLEMENTATION
  // ============================================================================

  /**
   * Collect data from S3 (CSV/JSON files)
   */
  private async collectFromS3(
    sourceSchema: PipelineSourceSchema,
    connectionConfig: any,
    limit: number,
    offset: number,
    cursor?: string,
  ): Promise<{ rows: any[]; totalRows?: number; nextCursor?: string; hasMore?: boolean }> {
    const s3Client = new S3Client({
      region: connectionConfig.region,
      credentials: {
        accessKeyId: connectionConfig.access_key_id,
        secretAccessKey: connectionConfig.secret_access_key,
      },
    });

    const bucket = connectionConfig.bucket;
    const prefix = sourceSchema.sourceConfig?.prefix || sourceSchema.sourceTable || '';
    const _filePattern = sourceSchema.sourceConfig?.filePattern || '*.json';

    try {
      // List objects in bucket
      const listCommand = new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
        ContinuationToken: cursor,
        MaxKeys: 1, // Process one file at a time
      });

      const listResponse = await s3Client.send(listCommand);
      const objects = listResponse.Contents || [];

      if (objects.length === 0) {
        return { rows: [], totalRows: 0, hasMore: false };
      }

      // Get the first file
      const fileKey = objects[0].Key!;
      const getCommand = new GetObjectCommand({
        Bucket: bucket,
        Key: fileKey,
      });

      const getResponse = await s3Client.send(getCommand);
      const body = await getResponse.Body?.transformToString();

      if (!body) {
        return { rows: [], totalRows: 0, hasMore: false };
      }

      // Parse based on file type
      let rows: any[];
      if (fileKey.endsWith('.json')) {
        const parsed = JSON.parse(body);
        rows = Array.isArray(parsed) ? parsed : [parsed];
      } else if (fileKey.endsWith('.csv')) {
        rows = this.parseCSV(body);
      } else {
        rows = [{ content: body }];
      }

      // Apply pagination
      const paginatedRows = rows.slice(offset, offset + limit);
      const hasMore =
        paginatedRows.length === limit || listResponse.IsTruncated || offset + limit < rows.length;
      const nextCursor = listResponse.NextContinuationToken;

      return {
        rows: paginatedRows,
        totalRows: rows.length,
        nextCursor,
        hasMore,
      };
    } catch (error) {
      this.logger.error(`S3 collection failed: ${error}`);
      throw error;
    }
  }

  /**
   * Simple CSV parser
   */
  private parseCSV(content: string): any[] {
    const lines = content.split('\n').filter((line) => line.trim());
    if (lines.length === 0) return [];

    const headers = lines[0].split(',').map((h) => h.trim().replace(/^"|"$/g, ''));
    const rows: any[] = [];

    for (let i = 1; i < lines.length; i++) {
      const values = lines[i].split(',').map((v) => v.trim().replace(/^"|"$/g, ''));
      const row: any = {};
      headers.forEach((header, index) => {
        row[header] = values[index];
      });
      rows.push(row);
    }

    return rows;
  }

  /**
   * Discover S3 schema by sampling files
   */
  private async discoverS3Schema(
    sourceSchema: PipelineSourceSchema,
    connectionConfig: any,
  ): Promise<{ columns: ColumnInfo[]; primaryKeys: string[]; estimatedRowCount?: number }> {
    // Get sample data first
    const sample = await this.collectFromS3(sourceSchema, connectionConfig, 10, 0);

    if (sample.rows.length === 0) {
      return { columns: [], primaryKeys: [], estimatedRowCount: 0 };
    }

    // Infer schema from sample
    const fieldSet = new Set<string>();
    for (const row of sample.rows) {
      Object.keys(row).forEach((key) => {
        fieldSet.add(key);
      });
    }

    const columns: ColumnInfo[] = Array.from(fieldSet).map((name) => {
      const sampleValue = sample.rows.find((r) => r[name] !== undefined)?.[name];
      return {
        name,
        dataType: typeof sampleValue || 'string',
        nullable: true,
      };
    });

    return {
      columns,
      primaryKeys: [],
      estimatedRowCount: sample.totalRows,
    };
  }

  // ============================================================================
  // REST API IMPLEMENTATION
  // ============================================================================

  /**
   * Collect data from REST API with rate limiting
   */
  private async collectFromAPI(
    sourceSchema: PipelineSourceSchema,
    connectionConfig: any,
    limit: number,
    offset: number,
    cursor?: string,
  ): Promise<{ rows: any[]; totalRows?: number; nextCursor?: string; hasMore?: boolean }> {
    const baseUrl = connectionConfig.base_url;
    const endpoint = sourceSchema.sourceConfig?.endpoint || '';
    const authType = connectionConfig.auth_type;
    const _rateLimit = sourceSchema.sourceConfig?.rateLimit || 10; // requests per second

    // Build URL with pagination
    const url = new URL(endpoint, baseUrl);
    url.searchParams.set('limit', String(limit));
    url.searchParams.set('offset', String(offset));
    if (cursor) {
      url.searchParams.set('cursor', cursor);
    }

    // Build headers
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      ...(sourceSchema.sourceConfig?.headers || {}),
    };

    // Add authentication
    switch (authType) {
      case 'bearer':
        headers.Authorization = `Bearer ${connectionConfig.auth_token}`;
        break;
      case 'api_key':
        headers['X-API-Key'] = connectionConfig.api_key;
        break;
      case 'basic': {
        const credentials = Buffer.from(
          `${connectionConfig.username}:${connectionConfig.password}`,
        ).toString('base64');
        headers.Authorization = `Basic ${credentials}`;
        break;
      }
    }

    // Execute request with retry
    let lastError: Error | undefined;
    for (let attempt = 0; attempt < RETRY_ATTEMPTS; attempt++) {
      try {
        // Simple rate limiting delay
        if (attempt > 0) {
          await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS * attempt));
        }

        const response = await firstValueFrom(this.httpService.get(url.toString(), { headers }));

        const data = response.data;

        // Handle different response formats
        let rows: any[];
        let totalRows: number | undefined;
        let nextCursor: string | undefined;

        if (Array.isArray(data)) {
          rows = data;
        } else if (data.data && Array.isArray(data.data)) {
          rows = data.data;
          totalRows = data.total || data.count;
          nextCursor = data.next_cursor || data.nextCursor;
        } else if (data.results && Array.isArray(data.results)) {
          rows = data.results;
          totalRows = data.total;
          nextCursor = data.next;
        } else {
          rows = [data];
        }

        const hasMore = rows.length === limit || !!nextCursor;

        return { rows, totalRows, nextCursor, hasMore };
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        this.logger.warn(`API request attempt ${attempt + 1} failed: ${lastError.message}`);
      }
    }

    throw lastError || new Error('API request failed after all retry attempts');
  }

  /**
   * Discover API schema by making a sample request
   */
  private async discoverAPISchema(
    sourceSchema: PipelineSourceSchema,
    connectionConfig: any,
  ): Promise<{ columns: ColumnInfo[]; primaryKeys: string[]; estimatedRowCount?: number }> {
    const sample = await this.collectFromAPI(sourceSchema, connectionConfig, 10, 0);

    if (sample.rows.length === 0) {
      return { columns: [], primaryKeys: [] };
    }

    const fieldSet = new Set<string>();
    for (const row of sample.rows) {
      Object.keys(row).forEach((key) => {
        fieldSet.add(key);
      });
    }

    const columns: ColumnInfo[] = Array.from(fieldSet).map((name) => {
      const sampleValue = sample.rows.find((r) => r[name] !== undefined)?.[name];
      return {
        name,
        dataType: typeof sampleValue || 'string',
        nullable: true,
        isPrimaryKey: name === 'id',
      };
    });

    return {
      columns,
      primaryKeys: columns.filter((c) => c.isPrimaryKey).map((c) => c.name),
      estimatedRowCount: sample.totalRows,
    };
  }

  // ============================================================================
  // BIGQUERY IMPLEMENTATION
  // ============================================================================

  /**
   * Collect data from BigQuery
   */
  private async collectFromBigQuery(
    sourceSchema: PipelineSourceSchema,
    connectionConfig: any,
    limit: number,
    offset: number,
  ): Promise<{ rows: any[]; totalRows?: number; nextCursor?: string; hasMore?: boolean }> {
    const bigquery = new BigQuery({
      projectId: connectionConfig.project_id,
      credentials: connectionConfig.credentials,
    });

    const dataset = connectionConfig.dataset;
    const table = sourceSchema.sourceTable;

    let query: string;
    if (sourceSchema.sourceQuery) {
      query = `${sourceSchema.sourceQuery} LIMIT ${limit} OFFSET ${offset}`;
    } else {
      query = `SELECT * FROM \`${dataset}.${table}\` LIMIT ${limit} OFFSET ${offset}`;
    }

    const [rows] = await bigquery.query({ query });

    // Get total count
    const [countResult] = await bigquery.query({
      query: `SELECT COUNT(*) as count FROM \`${dataset}.${table}\``,
    });
    const totalRows = countResult[0]?.count;

    const hasMore = rows.length === limit;
    const nextCursor = hasMore ? String(offset + limit) : undefined;

    return { rows, totalRows, nextCursor, hasMore };
  }

  /**
   * Discover BigQuery schema
   */
  private async discoverBigQuerySchema(
    sourceSchema: PipelineSourceSchema,
    connectionConfig: any,
  ): Promise<{ columns: ColumnInfo[]; primaryKeys: string[]; estimatedRowCount?: number }> {
    const bigquery = new BigQuery({
      projectId: connectionConfig.project_id,
      credentials: connectionConfig.credentials,
    });

    const dataset = connectionConfig.dataset;
    const table = sourceSchema.sourceTable;

    const [metadata] = await bigquery.dataset(dataset).table(table!).getMetadata();

    const columns: ColumnInfo[] = metadata.schema.fields.map((field: any) => ({
      name: field.name,
      dataType: field.type,
      nullable: field.mode !== 'REQUIRED',
    }));

    const estimatedRowCount = parseInt(metadata.numRows || '0', 10);

    return {
      columns,
      primaryKeys: [],
      estimatedRowCount,
    };
  }

  // ============================================================================
  // SNOWFLAKE IMPLEMENTATION
  // ============================================================================

  /**
   * Collect data from Snowflake
   * Note: Uses snowflake-sdk which requires callback-based connection
   */
  private async collectFromSnowflake(
    sourceSchema: PipelineSourceSchema,
    connectionConfig: any,
    limit: number,
    offset: number,
  ): Promise<{ rows: any[]; totalRows?: number; nextCursor?: string; hasMore?: boolean }> {
    // Dynamic import for snowflake-sdk
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

        const table = sourceSchema.sourceTable;
        const schema = sourceSchema.sourceSchema || connectionConfig.schema;

        let query: string;
        if (sourceSchema.sourceQuery) {
          query = `${sourceSchema.sourceQuery} LIMIT ${limit} OFFSET ${offset}`;
        } else {
          query = `SELECT * FROM "${schema}"."${table}" LIMIT ${limit} OFFSET ${offset}`;
        }

        conn.execute({
          sqlText: query,
          complete: (err, _stmt, rows) => {
            if (err) {
              connection.destroy(() => {});
              reject(err);
              return;
            }

            const resultRows = rows || [];
            const hasMore = resultRows.length === limit;
            const nextCursor = hasMore ? String(offset + limit) : undefined;

            connection.destroy(() => {});
            resolve({ rows: resultRows, totalRows: undefined, nextCursor, hasMore });
          },
        });
      });
    });
  }

  /**
   * Discover Snowflake schema
   */
  private async discoverSnowflakeSchema(
    sourceSchema: PipelineSourceSchema,
    connectionConfig: any,
  ): Promise<{ columns: ColumnInfo[]; primaryKeys: string[]; estimatedRowCount?: number }> {
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

        const table = sourceSchema.sourceTable;
        const schema = sourceSchema.sourceSchema || connectionConfig.schema;

        const query = `
          SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
          FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = '${schema}' AND TABLE_NAME = '${table}'
          ORDER BY ORDINAL_POSITION
        `;

        conn.execute({
          sqlText: query,
          complete: (err, _stmt, rows) => {
            if (err) {
              connection.destroy(() => {});
              reject(err);
              return;
            }

            const columns: ColumnInfo[] = (rows || []).map((row: any) => ({
              name: row.COLUMN_NAME,
              dataType: row.DATA_TYPE,
              nullable: row.IS_NULLABLE === 'YES',
            }));

            connection.destroy(() => {});
            resolve({ columns, primaryKeys: [], estimatedRowCount: undefined });
          },
        });
      });
    });
  }
}
