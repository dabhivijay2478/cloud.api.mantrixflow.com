/**
 * REST API Source Handler
 * Handles data collection and schema discovery for REST APIs
 */

import { Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { firstValueFrom } from 'rxjs';
import { ColumnInfo, DataSourceType } from '../../types/common.types';
import {
  BaseSourceHandler,
  CollectParams,
  CollectResult,
  ConnectionTestResult,
  PipelineSourceSchemaWithConfig,
  SchemaInfo,
} from '../../types/source-handler.types';

export class APIHandler extends BaseSourceHandler {
  readonly type = DataSourceType.API;
  private readonly logger = new Logger(APIHandler.name);

  constructor(private readonly httpService: HttpService) {
    super();
  }

  /**
   * Test API connection
   */
  async testConnection(connectionConfig: any): Promise<ConnectionTestResult> {
    try {
      const url = connectionConfig.base_url || connectionConfig.endpoint;
      const headers = this.buildHeaders(connectionConfig);

      const startTime = Date.now();
      const response = await firstValueFrom(
        this.httpService.get(url, { headers, timeout: 10000 }),
      );
      const latencyMs = Date.now() - startTime;

      return {
        success: true,
        message: `Connection successful (${response.status})`,
        details: {
          serverInfo: {
            statusCode: response.status,
            contentType: response.headers['content-type'],
          },
          latencyMs,
        },
      };
    } catch (error: any) {
      const message = error.response?.data?.message || error.message || String(error);
      return {
        success: false,
        message: `Connection failed: ${message}`,
      };
    }
  }

  /**
   * Discover API schema by making a sample request
   */
  async discoverSchema(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
  ): Promise<SchemaInfo> {
    try {
      const url = this.buildUrl(sourceSchema, connectionConfig);
      const headers = this.buildHeaders(connectionConfig);
      const method = sourceSchema.config.method || 'GET';

      this.logger.log(`Discovering schema from ${method} ${url}`);

      const response = await firstValueFrom(
        this.httpService.request({
          method,
          url,
          headers,
          timeout: 30000,
          params: sourceSchema.config.queryParams,
        }),
      );

      // Extract data from response
      const data = this.extractData(response.data, sourceSchema.config.dataPath);

      if (!Array.isArray(data) || data.length === 0) {
        throw new Error('API response does not contain array data');
      }

      // Infer columns from sample data
      const columns = this.inferColumnsFromData(data);

      this.logger.log(`Inferred ${columns.length} columns from ${data.length} sample records`);

      return {
        columns,
        primaryKeys: this.findPrimaryKeyField(data),
        estimatedRowCount: data.length,
        sampleDocuments: data.slice(0, 5),
        isRelational: false,
        sourceType: 'api',
        entityName: sourceSchema.config.endpoint || sourceSchema.config.path || 'api',
      };
    } catch (error) {
      this.logger.error(`Failed to discover API schema: ${error}`);
      throw error;
    }
  }

  /**
   * Collect data from REST API with pagination support
   */
  async collect(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    params: CollectParams,
  ): Promise<CollectResult> {
    try {
      const url = this.buildUrl(sourceSchema, connectionConfig);
      const headers = this.buildHeaders(connectionConfig);
      const method = sourceSchema.config.method || 'GET';

      // Build pagination params
      const paginationParams = this.buildPaginationParams(sourceSchema, params);

      this.logger.log(`Collecting data from ${method} ${url}`);

      const response = await this.withRetry(async () => {
        const result = await firstValueFrom(
          this.httpService.request({
            method,
            url,
            headers,
            timeout: 60000,
            params: {
              ...sourceSchema.config.queryParams,
              ...paginationParams,
            },
            data: method !== 'GET' ? sourceSchema.config.body : undefined,
          }),
        );
        return result;
      });

      // Extract data from response
      const data = this.extractData(response.data, sourceSchema.config.dataPath);
      const rows = Array.isArray(data) ? data : [data];

      // Extract pagination info
      const totalRows = this.extractTotal(response.data, sourceSchema.config.pagination?.totalPath);
      const nextCursor = this.extractNextCursor(response.data, sourceSchema.config.pagination);
      const hasMore = this.determineHasMore(rows, params.limit, response.data, sourceSchema.config.pagination);

      return {
        rows,
        totalRows,
        nextCursor,
        hasMore,
        metadata: {
          responseHeaders: response.headers,
          statusCode: response.status,
        },
      };
    } catch (error) {
      this.logger.error(`Failed to collect API data: ${error}`);
      throw error;
    }
  }

  /**
   * Stream data using async generator for paginated APIs
   */
  async *collectStream(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    params: CollectParams,
  ): AsyncIterable<any[]> {
    let cursor: string | undefined;
    let offset = 0;
    let hasMore = true;
    const pageSize = params.batchSize || 100;

    while (hasMore) {
      // Rate limiting
      if (connectionConfig.rateLimit?.requestsPerSecond) {
        await new Promise(resolve =>
          setTimeout(resolve, 1000 / connectionConfig.rateLimit.requestsPerSecond),
        );
      }

      const result = await this.collect(sourceSchema, connectionConfig, {
        ...params,
        limit: pageSize,
        offset,
        cursor,
      });

      if (result.rows.length > 0) {
        yield result.rows;
        offset += result.rows.length;
        cursor = result.nextCursor;
      }

      hasMore = result.hasMore || false;
    }
  }

  /**
   * Build full URL
   */
  private buildUrl(sourceSchema: PipelineSourceSchemaWithConfig, connectionConfig: any): string {
    const baseUrl = connectionConfig.base_url || connectionConfig.endpoint || '';
    const path = sourceSchema.config.path || sourceSchema.config.endpoint || '';

    if (path.startsWith('http')) {
      return path;
    }

    return baseUrl.endsWith('/') || path.startsWith('/')
      ? `${baseUrl}${path}`
      : `${baseUrl}/${path}`;
  }

  /**
   * Build headers from connection config
   */
  private buildHeaders(connectionConfig: any): Record<string, string> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      Accept: 'application/json',
      ...connectionConfig.headers,
    };

    // Handle authentication
    const authType = connectionConfig.auth_type || connectionConfig.authType;

    if (authType === 'bearer' && connectionConfig.auth_token) {
      headers['Authorization'] = `Bearer ${connectionConfig.auth_token}`;
    } else if (authType === 'api_key' || authType === 'apiKey') {
      const apiKey = connectionConfig.api_key || connectionConfig.apiKey;
      const keyHeader = connectionConfig.api_key_header || 'X-API-Key';
      headers[keyHeader] = apiKey;
    } else if (authType === 'basic') {
      const credentials = Buffer.from(
        `${connectionConfig.username}:${connectionConfig.password}`,
      ).toString('base64');
      headers['Authorization'] = `Basic ${credentials}`;
    }

    return headers;
  }

  /**
   * Build pagination parameters
   */
  private buildPaginationParams(
    sourceSchema: PipelineSourceSchemaWithConfig,
    params: CollectParams,
  ): Record<string, any> {
    const paginationType = sourceSchema.config.pagination?.type || 'offset';
    const config = sourceSchema.config.pagination || {};

    switch (paginationType) {
      case 'cursor':
        return {
          [config.cursorParam || 'cursor']: params.cursor,
          [config.limitParam || 'limit']: params.limit,
        };

      case 'page':
        const page = Math.floor(params.offset / params.limit) + 1;
        return {
          [config.pageParam || 'page']: page,
          [config.limitParam || 'per_page']: params.limit,
        };

      case 'offset':
      default:
        return {
          [config.offsetParam || 'offset']: params.offset,
          [config.limitParam || 'limit']: params.limit,
        };
    }
  }

  /**
   * Extract data from response using path
   */
  private extractData(response: any, dataPath?: string): any {
    if (!dataPath) return response;

    const parts = dataPath.split('.');
    let result = response;

    for (const part of parts) {
      if (result === null || result === undefined) return [];
      result = result[part];
    }

    return result || [];
  }

  /**
   * Extract total count from response
   */
  private extractTotal(response: any, totalPath?: string): number | undefined {
    if (!totalPath) return undefined;

    const parts = totalPath.split('.');
    let result = response;

    for (const part of parts) {
      if (result === null || result === undefined) return undefined;
      result = result[part];
    }

    return typeof result === 'number' ? result : undefined;
  }

  /**
   * Extract next cursor from response
   */
  private extractNextCursor(response: any, config?: any): string | undefined {
    if (!config?.nextCursorPath) return undefined;

    const parts = config.nextCursorPath.split('.');
    let result = response;

    for (const part of parts) {
      if (result === null || result === undefined) return undefined;
      result = result[part];
    }

    return result ? String(result) : undefined;
  }

  /**
   * Determine if there are more pages
   */
  private determineHasMore(
    rows: any[],
    limit: number,
    response: any,
    config?: any,
  ): boolean {
    // Check explicit hasMore field
    if (config?.hasMorePath) {
      const parts = config.hasMorePath.split('.');
      let result = response;
      for (const part of parts) {
        if (result === null || result === undefined) break;
        result = result[part];
      }
      if (typeof result === 'boolean') return result;
    }

    // Check if full page was returned
    return rows.length >= limit;
  }

  /**
   * Infer columns from sample data
   */
  private inferColumnsFromData(data: any[]): ColumnInfo[] {
    if (data.length === 0) return [];

    const fieldTypes = new Map<string, Set<string>>();

    for (const row of data.slice(0, 100)) {
      this.extractFields(row, '', fieldTypes);
    }

    return Array.from(fieldTypes.entries()).map(([name, types]) => ({
      name,
      dataType: Array.from(types).filter(t => t !== 'null').join(' | ') || 'string',
      nullable: types.has('null'),
    }));
  }

  /**
   * Extract fields from nested object
   */
  private extractFields(
    obj: any,
    prefix: string,
    fieldTypes: Map<string, Set<string>>,
    depth: number = 0,
  ): void {
    if (depth > 3) return; // Limit recursion depth

    for (const [key, value] of Object.entries(obj)) {
      const fieldName = prefix ? `${prefix}.${key}` : key;

      if (!fieldTypes.has(fieldName)) {
        fieldTypes.set(fieldName, new Set());
      }

      const type = this.inferTypeFromValue(value);
      fieldTypes.get(fieldName)!.add(type);

      // Recurse into objects (but not arrays)
      if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
        this.extractFields(value, fieldName, fieldTypes, depth + 1);
      }
    }
  }

  /**
   * Find potential primary key field
   */
  private findPrimaryKeyField(data: any[]): string[] {
    if (data.length === 0) return [];

    const firstRow = data[0];
    const possibleKeys = ['id', '_id', 'ID', 'Id', 'uuid', 'key'];

    for (const key of possibleKeys) {
      if (key in firstRow) {
        return [key];
      }
    }

    return [];
  }
}
