/**
 * BigQuery Source Handler
 * Handles data collection and schema discovery for Google BigQuery
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

export class BigQueryHandler extends BaseSourceHandler {
  readonly type = DataSourceType.BIGQUERY;
  private readonly logger = new Logger(BigQueryHandler.name);

  /**
   * Test BigQuery connection
   */
  async testConnection(connectionConfig: any): Promise<ConnectionTestResult> {
    try {
      const { BigQuery } = await import('@google-cloud/bigquery');
      
      const bigquery = new BigQuery({
        projectId: connectionConfig.project_id || connectionConfig.projectId,
        credentials: connectionConfig.credentials || (connectionConfig.credentials_json 
          ? JSON.parse(connectionConfig.credentials_json) 
          : undefined),
      });

      // Test by listing datasets
      const [datasets] = await bigquery.getDatasets({ maxResults: 1 });
      
      return {
        success: true,
        message: 'Connection successful',
        details: {
          serverInfo: {
            projectId: connectionConfig.project_id || connectionConfig.projectId,
            datasetCount: datasets.length,
          },
        },
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return {
        success: false,
        message: `Connection failed: ${message}`,
      };
    }
  }

  /**
   * Discover BigQuery schema
   */
  async discoverSchema(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
  ): Promise<SchemaInfo> {
    try {
      const { BigQuery } = await import('@google-cloud/bigquery');
      
      const bigquery = new BigQuery({
        projectId: connectionConfig.project_id || connectionConfig.projectId,
        credentials: connectionConfig.credentials || (connectionConfig.credentials_json 
          ? JSON.parse(connectionConfig.credentials_json) 
          : undefined),
      });

      const datasetId = sourceSchema.config.dataset || connectionConfig.dataset || sourceSchema.sourceSchema;
      const tableId = sourceSchema.config.table || sourceSchema.config.tableName || sourceSchema.sourceTable;

      if (!tableId) {
        throw new Error('Table ID is required for BigQuery schema discovery');
      }

      this.logger.log(`Discovering schema for ${datasetId}.${tableId}`);

      const dataset = bigquery.dataset(datasetId);
      const table = dataset.table(tableId);

      // Get table metadata
      const [metadata] = await table.getMetadata();
      const schema = metadata.schema?.fields || [];

      const columns: ColumnInfo[] = schema.map((field: any) => ({
        name: field.name,
        dataType: this.normalizeBigQueryType(field.type),
        nullable: field.mode === 'NULLABLE' || field.mode === 'REPEATED',
        isPrimaryKey: false, // BigQuery doesn't have primary keys
        maxLength: field.maxLength,
        precision: field.precision,
        scale: field.scale,
      }));

      // Get row count estimate
      const [job] = await bigquery.createQueryJob({
        query: `SELECT COUNT(*) as count FROM \`${datasetId}.${tableId}\``,
      });
      const [rows] = await job.getQueryResults();
      const estimatedRowCount = rows[0]?.count ? Number(rows[0].count) : undefined;

      this.logger.log(`Found ${columns.length} columns, estimated ${estimatedRowCount} rows`);

      return {
        columns,
        primaryKeys: [],
        estimatedRowCount,
        isRelational: true,
        sourceType: 'bigquery',
        entityName: tableId || undefined,
      };
    } catch (error) {
      this.logger.error(`Failed to discover schema: ${error}`);
      throw error;
    }
  }

  /**
   * Collect data from BigQuery
   */
  async collect(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    params: CollectParams,
  ): Promise<CollectResult> {
    try {
      const { BigQuery } = await import('@google-cloud/bigquery');
      
      const bigquery = new BigQuery({
        projectId: connectionConfig.project_id || connectionConfig.projectId,
        credentials: connectionConfig.credentials || (connectionConfig.credentials_json 
          ? JSON.parse(connectionConfig.credentials_json) 
          : undefined),
      });

      const datasetId = sourceSchema.config.dataset || connectionConfig.dataset || sourceSchema.sourceSchema;
      const tableId = sourceSchema.config.table || sourceSchema.config.tableName || sourceSchema.sourceTable;

      // Build query
      let query = sourceSchema.sourceQuery;
      if (!query) {
        query = `SELECT * FROM \`${datasetId}.${tableId}\``;
        
        // Add incremental filter if provided
        if (params.incrementalColumn && params.lastSyncValue) {
          query += ` WHERE \`${params.incrementalColumn}\` > @lastSyncValue`;
        }
      }

      // Add pagination
      query += ` LIMIT ${params.limit} OFFSET ${params.offset}`;

      this.logger.log(`Executing BigQuery query with limit ${params.limit}, offset ${params.offset}`);

      const options: any = {
        query,
        maxResults: params.limit,
      };

      if (params.incrementalColumn && params.lastSyncValue) {
        options.params = {
          lastSyncValue: params.lastSyncValue,
        };
      }

      const [job] = await bigquery.createQueryJob(options);
      const [rows] = await job.getQueryResults();

      // Convert BigQuery rows to plain objects
      const plainRows = rows.map((row: any) => {
        const obj: any = {};
        for (const [key, value] of Object.entries(row)) {
          obj[key] = value;
        }
        return obj;
      });

      const hasMore = plainRows.length === params.limit;
      const nextCursor = hasMore ? String(params.offset + params.limit) : undefined;

      // Get total count if needed (for future use)
      let totalRows: number | undefined;
      // Note: includeTotal is not in CollectParams, but we can add it if needed

      return {
        rows: plainRows,
        totalRows,
        nextCursor,
        hasMore,
      };
    } catch (error) {
      this.logger.error(`Failed to collect data: ${error}`);
      throw error;
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
    const { BigQuery } = await import('@google-cloud/bigquery');
    
    const bigquery = new BigQuery({
      projectId: connectionConfig.project_id || connectionConfig.projectId,
      credentials: connectionConfig.credentials || (connectionConfig.credentials_json 
        ? JSON.parse(connectionConfig.credentials_json) 
        : undefined),
    });

    const datasetId = sourceSchema.config.dataset || connectionConfig.dataset || sourceSchema.sourceSchema;
    const tableId = sourceSchema.config.table || sourceSchema.config.tableName || sourceSchema.sourceTable;
    
    if (!tableId) {
      throw new Error('Table ID is required for BigQuery streaming');
    }
    
    const batchSize = params.batchSize || 1000;

    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      let query = sourceSchema.sourceQuery;
      if (!query) {
        query = `SELECT * FROM \`${datasetId}.${tableId}\``;
      }
      query += ` LIMIT ${batchSize} OFFSET ${offset}`;

      const [job] = await bigquery.createQueryJob({ query });
      const [rows] = await job.getQueryResults();

      if (rows.length > 0) {
        const plainRows = rows.map((row: any) => {
          const obj: any = {};
          for (const [key, value] of Object.entries(row)) {
            obj[key] = value;
          }
          return obj;
        });
        yield plainRows;
        offset += plainRows.length;
      }

      hasMore = rows.length === batchSize;
    }
  }

  /**
   * Get sample data for preview
   */
  async getSampleData(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    sampleSize: number = 10,
  ): Promise<any[]> {
    const result = await this.collect(sourceSchema, connectionConfig, {
      limit: sampleSize,
      offset: 0,
    });
    return result.rows;
  }

  /**
   * Normalize BigQuery type to standard type
   */
  private normalizeBigQueryType(bqType: string): string {
    const typeMap: Record<string, string> = {
      'STRING': 'string',
      'BYTES': 'binary',
      'INTEGER': 'integer',
      'INT64': 'bigint',
      'FLOAT': 'float',
      'FLOAT64': 'double',
      'NUMERIC': 'decimal',
      'BIGNUMERIC': 'decimal',
      'BOOLEAN': 'boolean',
      'BOOL': 'boolean',
      'TIMESTAMP': 'timestamp',
      'DATE': 'date',
      'TIME': 'time',
      'DATETIME': 'datetime',
      'GEOGRAPHY': 'string',
      'JSON': 'json',
      'RECORD': 'object',
      'ARRAY': 'array',
    };

    return typeMap[bqType.toUpperCase()] || 'string';
  }
}
