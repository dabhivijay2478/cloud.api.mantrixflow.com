/**
 * MongoDB Source Handler
 * Handles data collection and schema discovery for MongoDB databases
 */

import { Logger } from '@nestjs/common';
import { ColumnInfo, DataSourceType } from '../../types/common.types';
import {
  BaseSourceHandler,
  CollectParams,
  CollectResult,
  ConnectionTestResult,
  PipelineSourceSchemaWithConfig,
  SchemaInfo,
} from '../../types/source-handler.types';

export class MongoDBHandler extends BaseSourceHandler {
  readonly type = DataSourceType.MONGODB;
  private readonly logger = new Logger(MongoDBHandler.name);

  /**
   * Test MongoDB connection
   */
  async testConnection(connectionConfig: any): Promise<ConnectionTestResult> {
    let MongoClient: any;
    let client: any = null;

    try {
      // Dynamic import mongodb driver
      const mongodb = await import('mongodb');
      MongoClient = mongodb.MongoClient;

      const connectionString = this.buildConnectionString(connectionConfig);
      const options = this.getConnectionOptions(connectionConfig);

      client = new MongoClient(connectionString, options);
      await client.connect();

      // Ping the server
      const adminDb = client.db('admin');
      await adminDb.command({ ping: 1 });

      // Get server info
      const serverInfo = await adminDb.command({ serverStatus: 1 }).catch(() => ({}));

      return {
        success: true,
        message: 'Connection successful',
        details: {
          version: serverInfo.version || 'Unknown',
          serverInfo: {
            version: serverInfo.version,
            host: serverInfo.host,
          },
        },
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return {
        success: false,
        message: `Connection failed: ${message}`,
      };
    } finally {
      if (client) {
        await client.close();
      }
    }
  }

  /**
   * Discover MongoDB schema by sampling documents
   */
  async discoverSchema(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
  ): Promise<SchemaInfo> {
    let MongoClient: any;
    let client: any = null;

    try {
      const mongodb = await import('mongodb');
      MongoClient = mongodb.MongoClient;

      const connectionString = this.buildConnectionString(connectionConfig);
      const options = this.getConnectionOptions(connectionConfig);

      client = new MongoClient(connectionString, options);
      await client.connect();

      // For MongoDB: database can be in sourceSchema.sourceSchema (when created from UI)
      // or in sourceSchema.config.database (when configured manually)
      // Collection can be in sourceSchema.sourceTable (when created from UI)
      // or in sourceSchema.config.collection (when configured manually)
      const databaseName = 
        connectionConfig.database || 
        sourceSchema.config?.database || 
        sourceSchema.sourceSchema; // MongoDB: sourceSchema field stores database name
      
      const collectionName = 
        sourceSchema.config?.collection || 
        sourceSchema.config?.table || 
        sourceSchema.sourceTable; // MongoDB: sourceTable field stores collection name
      
      if (!databaseName) {
        throw new Error('MongoDB database name is required. Please specify database in connection config or source schema.');
      }
      
      if (!collectionName) {
        throw new Error('MongoDB collection name is required. Please specify collection in source schema.');
      }
      
      this.logger.log(`MongoDB discoverSchema: database=${databaseName}, collection=${collectionName}`);

      this.logger.log(`Discovering schema for ${databaseName}.${collectionName}`);

      const db = client.db(databaseName);
      const collection = db.collection(collectionName);

      // Sample documents to infer schema
      const sampleDocs = await collection.find({}).limit(100).toArray();
      const estimatedRowCount = await collection.estimatedDocumentCount();

      // Infer fields from sample documents
      const fieldsMap = new Map<string, { types: Set<string>; nullable: boolean }>();

      for (const doc of sampleDocs) {
        this.extractFields(doc, '', fieldsMap);
      }

      // Convert to columns
      const columns: ColumnInfo[] = Array.from(fieldsMap.entries()).map(([name, info]) => ({
        name,
        dataType: Array.from(info.types).join(' | '),
        nullable: info.nullable,
        isPrimaryKey: name === '_id',
      }));

      const primaryKeys = ['_id'];

      this.logger.log(`Inferred ${columns.length} fields from ${sampleDocs.length} sample documents`);

      return {
        columns,
        primaryKeys,
        estimatedRowCount,
        sampleDocuments: sampleDocs.slice(0, 5),
        isRelational: false,
        sourceType: 'mongodb',
        entityName: collectionName || undefined,
      };
    } catch (error) {
      this.logger.error(`Failed to discover schema: ${error}`);
      throw error;
    } finally {
      if (client) {
        await client.close();
      }
    }
  }

  /**
   * Collect data from MongoDB
   */
  async collect(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    params: CollectParams,
  ): Promise<CollectResult> {
    let MongoClient: any;
    let client: any = null;

    try {
      const mongodb = await import('mongodb');
      MongoClient = mongodb.MongoClient;
      const ObjectId = mongodb.ObjectId;

      const connectionString = this.buildConnectionString(connectionConfig);
      const options = this.getConnectionOptions(connectionConfig);

      client = new MongoClient(connectionString, options);
      await client.connect();

      // For MongoDB: database can be in sourceSchema.sourceSchema (when created from UI)
      // or in sourceSchema.config.database (when configured manually)
      // Collection can be in sourceSchema.sourceTable (when created from UI)
      // or in sourceSchema.config.collection (when configured manually)
      const databaseName = 
        connectionConfig.database || 
        sourceSchema.config?.database || 
        sourceSchema.sourceSchema; // MongoDB: sourceSchema field stores database name
      
      const collectionName = 
        sourceSchema.config?.collection || 
        sourceSchema.config?.table || 
        sourceSchema.sourceTable; // MongoDB: sourceTable field stores collection name
      
      if (!databaseName) {
        throw new Error('MongoDB database name is required. Please specify database in connection config or source schema.');
      }
      
      if (!collectionName) {
        throw new Error('MongoDB collection name is required. Please specify collection in source schema.');
      }
      
      this.logger.log(`MongoDB collect: database=${databaseName}, collection=${collectionName}`);

      const db = client.db(databaseName);
      const collection = db.collection(collectionName);

      // Build query
      let query: any = {};

      // Handle cursor-based pagination
      if (params.cursor) {
        try {
          query._id = { $gt: new ObjectId(params.cursor) };
        } catch {
          // Invalid ObjectId, ignore
        }
      }

      // Handle incremental sync
      if (params.incrementalColumn && params.lastSyncValue) {
        query[params.incrementalColumn] = { $gt: params.lastSyncValue };
      }

      // Apply custom filters
      if (params.filters) {
        query = { ...query, ...params.filters };
      }

      this.logger.log(`Querying collection ${collectionName} with: ${JSON.stringify(query)}`);

      // Execute query with skip/limit
      const rows = await collection
        .find(query)
        .skip(params.offset)
        .limit(params.limit)
        .toArray();

      // Get total count
      const totalRows = await collection.countDocuments(query);

      // Determine next cursor
      const lastRow = rows[rows.length - 1];
      const nextCursor = lastRow?._id?.toString();
      const hasMore = rows.length === params.limit;

      return {
        rows,
        totalRows,
        nextCursor,
        hasMore,
      };
    } catch (error) {
      this.logger.error(`Failed to collect data: ${error}`);
      throw error;
    } finally {
      if (client) {
        await client.close();
      }
    }
  }

  /**
   * Collect incremental data (new/changed records since checkpoint)
   * Implements strict incremental filtering: { watermarkField: { $gt: lastValue } }
   * 
   * Root Fix: This ensures only new/changed records are collected, preventing re-writing all data
   */
  async collectIncremental(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    checkpoint: { watermarkField: string; lastValue: string | number; pauseTimestamp?: string },
    params: Omit<CollectParams, 'incrementalColumn' | 'lastSyncValue'>,
  ): Promise<CollectResult> {
    let MongoClient: any;
    let client: any = null;

    try {
      const mongodb = await import('mongodb');
      MongoClient = mongodb.MongoClient;

      const connectionString = this.buildConnectionString(connectionConfig);
      const options = this.getConnectionOptions(connectionConfig);

      client = new MongoClient(connectionString, options);
      await client.connect();

      const databaseName = 
        connectionConfig.database || 
        sourceSchema.config?.database || 
        sourceSchema.sourceSchema;
      
      const collectionName = 
        sourceSchema.config?.collection || 
        sourceSchema.config?.table || 
        sourceSchema.sourceTable;
      
      if (!databaseName) {
        throw new Error('MongoDB database name is required.');
      }
      
      if (!collectionName) {
        throw new Error('MongoDB collection name is required.');
      }

      this.logger.log(`MongoDB incremental collect: database=${databaseName}, collection=${collectionName}`);

      const db = client.db(databaseName);
      const collection = db.collection(collectionName);

      // Determine the effective last value (consider pause timestamp)
      let effectiveLastValue: any = checkpoint.lastValue;
      if (checkpoint.pauseTimestamp) {
        const pauseDate = new Date(checkpoint.pauseTimestamp);
        const lastValueDate = typeof checkpoint.lastValue === 'string' || typeof checkpoint.lastValue === 'number'
          ? new Date(checkpoint.lastValue)
          : new Date();
        
        effectiveLastValue = pauseDate < lastValueDate ? pauseDate : lastValueDate;
      }

      // Build strict incremental query: { watermarkField: { $gt: lastValue } }
      const query: any = {
        [checkpoint.watermarkField]: { $gt: effectiveLastValue },
      };

      this.logger.log(`Incremental query: ${JSON.stringify(query)}`);

      // Execute query with skip/limit
      const rows = await collection
        .find(query)
        .sort({ [checkpoint.watermarkField]: 1 }) // Sort by watermark field
        .skip(params.offset)
        .limit(params.limit)
        .toArray();

      // Get total count for incremental records
      const totalRows = await collection.countDocuments(query);

      // Determine next cursor (use last document's _id)
      const lastRow = rows[rows.length - 1];
      const nextCursor = lastRow?._id?.toString();
      const hasMore = rows.length === params.limit;

      this.logger.log(
        `Incremental sync: Found ${rows.length} new records (total available: ${totalRows})`,
      );

      return {
        rows,
        totalRows,
        nextCursor,
        hasMore,
      };
    } catch (error) {
      this.logger.error(`Failed to collect incremental data: ${error}`);
      throw error;
    } finally {
      if (client) {
        await client.close();
      }
    }
  }

  /**
   * Stream data using async generator for large datasets
   */
  async *collectStream(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    params: CollectParams,
  ): AsyncIterable<any[]> {
    let MongoClient: any;
    let client: any = null;

    try {
      const mongodb = await import('mongodb');
      MongoClient = mongodb.MongoClient;

      const connectionString = this.buildConnectionString(connectionConfig);
      const options = this.getConnectionOptions(connectionConfig);

      client = new MongoClient(connectionString, options);
      await client.connect();

      // For MongoDB: database can be in sourceSchema.sourceSchema (when created from UI)
      // or in sourceSchema.config.database (when configured manually)
      // Collection can be in sourceSchema.sourceTable (when created from UI)
      // or in sourceSchema.config.collection (when configured manually)
      const databaseName = 
        connectionConfig.database || 
        sourceSchema.config?.database || 
        sourceSchema.sourceSchema; // MongoDB: sourceSchema field stores database name
      
      const collectionName = 
        sourceSchema.config?.collection || 
        sourceSchema.config?.table || 
        sourceSchema.sourceTable; // MongoDB: sourceTable field stores collection name
      
      if (!databaseName) {
        throw new Error('MongoDB database name is required. Please specify database in connection config or source schema.');
      }
      
      if (!collectionName) {
        throw new Error('MongoDB collection name is required. Please specify collection in source schema.');
      }
      
      this.logger.log(`MongoDB collectStream: database=${databaseName}, collection=${collectionName}`);
      const batchSize = params.batchSize || 1000;

      const db = client.db(databaseName);
      const collection = db.collection(collectionName);

      // Use cursor for efficient streaming
      const cursor = collection.find({}).batchSize(batchSize);

      let batch: any[] = [];
      for await (const doc of cursor) {
        batch.push(doc);
        if (batch.length >= batchSize) {
          yield batch;
          batch = [];
        }
      }

      // Yield remaining documents
      if (batch.length > 0) {
        yield batch;
      }
    } finally {
      if (client) {
        await client.close();
      }
    }
  }

  /**
   * Build MongoDB connection string
   */
  private buildConnectionString(connectionConfig: any): string {
    if (connectionConfig.connection_string) {
      return connectionConfig.connection_string;
    }

    const auth = connectionConfig.username && connectionConfig.password
      ? `${encodeURIComponent(connectionConfig.username)}:${encodeURIComponent(connectionConfig.password)}@`
      : '';

    const port = connectionConfig.port || 27017;
    const database = connectionConfig.database || '';
    const authSource = connectionConfig.auth_source ? `?authSource=${connectionConfig.auth_source}` : '';

    return `mongodb://${auth}${connectionConfig.host}:${port}/${database}${authSource}`;
  }

  /**
   * Get MongoDB connection options
   */
  private getConnectionOptions(connectionConfig: any): any {
    return {
      serverSelectionTimeoutMS: 10000,
      connectTimeoutMS: 10000,
      tls: connectionConfig.tls || false,
    };
  }

  /**
   * Extract fields from MongoDB document for schema inference
   */
  private extractFields(
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
        fieldInfo.types.add('null');
      } else if (Array.isArray(value)) {
        fieldInfo.types.add('array');
      } else if (value instanceof Date) {
        fieldInfo.types.add('date');
      } else if (typeof value === 'object') {
        // Check if it's an ObjectId
        if (value.constructor?.name === 'ObjectId') {
          fieldInfo.types.add('objectId');
        } else {
          fieldInfo.types.add('object');
          // Recurse into nested objects (limit depth)
          if (prefix.split('.').length < 3) {
            this.extractFields(value, fieldName, fieldMap);
          }
        }
      } else {
        fieldInfo.types.add(typeof value);
      }
    }
  }
}
