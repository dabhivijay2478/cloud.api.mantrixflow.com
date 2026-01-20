/**
 * S3 Source Handler
 * Handles data collection and schema discovery for AWS S3 (CSV/JSON files)
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

export class S3Handler extends BaseSourceHandler {
  readonly type = DataSourceType.S3;
  private readonly logger = new Logger(S3Handler.name);

  /**
   * Test S3 connection
   */
  async testConnection(connectionConfig: any): Promise<ConnectionTestResult> {
    try {
      const { S3Client, ListBucketsCommand, HeadBucketCommand } = await import('@aws-sdk/client-s3');
      
      const client = new S3Client({
        region: connectionConfig.region || 'us-east-1',
        credentials: {
          accessKeyId: connectionConfig.access_key_id || connectionConfig.accessKeyId,
          secretAccessKey: connectionConfig.secret_access_key || connectionConfig.secretAccessKey,
        },
      });

      // Test by checking if bucket exists
      if (connectionConfig.bucket) {
        await client.send(new HeadBucketCommand({ Bucket: connectionConfig.bucket }));
        return {
          success: true,
          message: `Successfully connected to bucket: ${connectionConfig.bucket}`,
          details: {
            serverInfo: { bucket: connectionConfig.bucket, region: connectionConfig.region },
          },
        };
      } else {
        // List buckets to verify credentials
        const result = await client.send(new ListBucketsCommand({}));
        return {
          success: true,
          message: `Connection successful. Found ${result.Buckets?.length || 0} buckets`,
          details: {
            serverInfo: { bucketCount: result.Buckets?.length || 0 },
          },
        };
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return {
        success: false,
        message: `Connection failed: ${message}`,
      };
    }
  }

  /**
   * Discover S3 schema by sampling files
   */
  async discoverSchema(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
  ): Promise<SchemaInfo> {
    try {
      const { S3Client, GetObjectCommand, ListObjectsV2Command } = await import('@aws-sdk/client-s3');

      const client = new S3Client({
        region: connectionConfig.region || 'us-east-1',
        credentials: {
          accessKeyId: connectionConfig.access_key_id || connectionConfig.accessKeyId,
          secretAccessKey: connectionConfig.secret_access_key || connectionConfig.secretAccessKey,
        },
      });

      const bucket = connectionConfig.bucket;
      const prefix = sourceSchema.config.prefix || connectionConfig.path_prefix || '';

      this.logger.log(`Discovering schema from s3://${bucket}/${prefix}`);

      // List files
      const listResult = await client.send(
        new ListObjectsV2Command({
          Bucket: bucket,
          Prefix: prefix,
          MaxKeys: 10,
        }),
      );

      if (!listResult.Contents || listResult.Contents.length === 0) {
        throw new Error(`No files found in s3://${bucket}/${prefix}`);
      }

      // Get first file to infer schema
      const firstFile = listResult.Contents[0];
      const getResult = await client.send(
        new GetObjectCommand({
          Bucket: bucket,
          Key: firstFile.Key,
        }),
      );

      const bodyString = await getResult.Body?.transformToString();
      if (!bodyString) {
        throw new Error('Empty file');
      }

      // Detect file format and parse
      const fileKey = firstFile.Key?.toLowerCase() || '';
      let sampleData: any[];

      if (fileKey.endsWith('.json') || fileKey.endsWith('.jsonl')) {
        sampleData = this.parseJSON(bodyString);
      } else if (fileKey.endsWith('.csv')) {
        sampleData = this.parseCSV(bodyString);
      } else {
        // Try to detect format
        if (bodyString.trim().startsWith('[') || bodyString.trim().startsWith('{')) {
          sampleData = this.parseJSON(bodyString);
        } else {
          sampleData = this.parseCSV(bodyString);
        }
      }

      // Infer columns from sample data
      const columns = this.inferColumnsFromData(sampleData);
      const estimatedRowCount = listResult.KeyCount || listResult.Contents.length;

      this.logger.log(`Inferred ${columns.length} columns from ${sampleData.length} sample rows`);

      const fileName = firstFile.Key?.split('/').pop() || 'file';
      return {
        columns,
        primaryKeys: [],
        estimatedRowCount,
        sampleDocuments: sampleData.slice(0, 5),
        isRelational: false,
        sourceType: 's3',
        entityName: fileName,
      };
    } catch (error) {
      this.logger.error(`Failed to discover S3 schema: ${error}`);
      throw error;
    }
  }

  /**
   * Collect data from S3
   */
  async collect(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    params: CollectParams,
  ): Promise<CollectResult> {
    try {
      const { S3Client, GetObjectCommand, ListObjectsV2Command } = await import('@aws-sdk/client-s3');

      const client = new S3Client({
        region: connectionConfig.region || 'us-east-1',
        credentials: {
          accessKeyId: connectionConfig.access_key_id || connectionConfig.accessKeyId,
          secretAccessKey: connectionConfig.secret_access_key || connectionConfig.secretAccessKey,
        },
      });

      const bucket = connectionConfig.bucket;
      const prefix = sourceSchema.config.prefix || connectionConfig.path_prefix || '';

      this.logger.log(`Collecting data from s3://${bucket}/${prefix}`);

      // List files
      const listResult = await client.send(
        new ListObjectsV2Command({
          Bucket: bucket,
          Prefix: prefix,
          ContinuationToken: params.cursor || undefined,
        }),
      );

      if (!listResult.Contents || listResult.Contents.length === 0) {
        return { rows: [], hasMore: false };
      }

      // Collect data from files
      const allRows: any[] = [];
      let processedFiles = 0;

      for (const file of listResult.Contents) {
        if (allRows.length >= params.limit) break;

        try {
          const getResult = await client.send(
            new GetObjectCommand({
              Bucket: bucket,
              Key: file.Key,
            }),
          );

          const bodyString = await getResult.Body?.transformToString();
          if (!bodyString) continue;

          // Parse file
          const fileKey = file.Key?.toLowerCase() || '';
          let rows: any[];

          if (fileKey.endsWith('.json') || fileKey.endsWith('.jsonl')) {
            rows = this.parseJSON(bodyString);
          } else if (fileKey.endsWith('.csv')) {
            rows = this.parseCSV(bodyString);
          } else {
            continue; // Skip unsupported files
          }

          // Apply offset within file (simplified)
          const startIndex = processedFiles === 0 ? params.offset : 0;
          const endIndex = Math.min(startIndex + (params.limit - allRows.length), rows.length);
          allRows.push(...rows.slice(startIndex, endIndex));

          processedFiles++;
        } catch (fileError) {
          this.logger.warn(`Failed to process file ${file.Key}: ${fileError}`);
        }
      }

      return {
        rows: allRows,
        totalRows: listResult.KeyCount,
        nextCursor: listResult.NextContinuationToken,
        hasMore: listResult.IsTruncated || false,
      };
    } catch (error) {
      this.logger.error(`Failed to collect S3 data: ${error}`);
      throw error;
    }
  }

  /**
   * Parse JSON/JSONL content
   */
  private parseJSON(content: string): any[] {
    const trimmed = content.trim();

    // JSON array
    if (trimmed.startsWith('[')) {
      return JSON.parse(trimmed);
    }

    // JSONL (newline-delimited JSON)
    return trimmed
      .split('\n')
      .filter(line => line.trim())
      .map(line => JSON.parse(line));
  }

  /**
   * Parse CSV content
   */
  private parseCSV(content: string): any[] {
    const lines = content.split('\n').filter(line => line.trim());
    if (lines.length < 2) return [];

    // Parse header
    const headers = this.parseCSVLine(lines[0]);

    // Parse data rows
    const rows: any[] = [];
    for (let i = 1; i < lines.length; i++) {
      const values = this.parseCSVLine(lines[i]);
      const row: any = {};
      headers.forEach((header, index) => {
        row[header] = values[index] || null;
      });
      rows.push(row);
    }

    return rows;
  }

  /**
   * Parse a single CSV line
   */
  private parseCSVLine(line: string): string[] {
    const result: string[] = [];
    let current = '';
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const char = line[i];

      if (char === '"') {
        inQuotes = !inQuotes;
      } else if (char === ',' && !inQuotes) {
        result.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }

    result.push(current.trim());
    return result;
  }

  /**
   * Infer columns from sample data
   */
  private inferColumnsFromData(data: any[]): ColumnInfo[] {
    if (data.length === 0) return [];

    const fieldTypes = new Map<string, Set<string>>();

    for (const row of data) {
      for (const [key, value] of Object.entries(row)) {
        if (!fieldTypes.has(key)) {
          fieldTypes.set(key, new Set());
        }

        const type = this.inferTypeFromValue(value);
        fieldTypes.get(key)!.add(type);
      }
    }

    return Array.from(fieldTypes.entries()).map(([name, types]) => ({
      name,
      dataType: Array.from(types).filter(t => t !== 'null').join(' | ') || 'string',
      nullable: types.has('null'),
    }));
  }
}
