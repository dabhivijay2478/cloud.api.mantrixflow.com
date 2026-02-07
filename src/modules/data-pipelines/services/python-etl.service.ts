/**
 * Python ETL Service Client
 * HTTP client for calling Python FastAPI ETL microservice
 * Replaces CollectorService, TransformerService, and EmitterService
 */

import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { normalizeEtlBaseUrl } from '../../../common/utils/etl-url';
import { DataSourceRepository } from '../../data-sources/repositories/data-source.repository';
import { ConnectionService } from '../../data-sources/connection.service';
import type { WriteResult, ColumnInfo } from '../types/common.types';
import type { PipelineSourceSchema, PipelineDestinationSchema } from '../../../database/schemas';

@Injectable()
export class PythonETLService {
  private readonly logger = new Logger(PythonETLService.name);
  private readonly pythonServiceUrl: string;
  private readonly pythonServiceAuthToken: string;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly dataSourceRepository: DataSourceRepository,
    private readonly connectionService: ConnectionService,
  ) {
    const raw = this.configService.get<string>('ETL_PYTHON_SERVICE_URL') ??
      this.configService.get<string>('PYTHON_SERVICE_URL');
    this.pythonServiceUrl = normalizeEtlBaseUrl(raw);
    this.pythonServiceAuthToken =
      this.configService.get<string>('ETL_PYTHON_SERVICE_TOKEN') ||
      this.configService.get<string>('SUPABASE_SERVICE_ROLE_KEY') ||
      'internal-etl-service';
    if (!this.pythonServiceUrl) {
      const hint = raw != null && String(raw).trim().length > 0
        ? ` Value was normalized to an invalid URL (e.g. missing host). Set a valid base URL like http://localhost:8001 in apps/api/.env (from the api directory so .env is loaded).`
        : ' Set ETL_PYTHON_SERVICE_URL or PYTHON_SERVICE_URL in apps/api/.env and run the API from the api directory so .env is loaded.';
      throw new Error(
        `ETL_PYTHON_SERVICE_URL or PYTHON_SERVICE_URL must be a valid URL with host.${hint}`,
      );
    }
    this.logger.log(`Python ETL Service URL: ${this.pythonServiceUrl}`);
  }

  private buildRequestConfig(timeout: number) {
    return {
      timeout,
      headers: {
        Authorization: `Bearer ${this.pythonServiceAuthToken}`,
      },
    };
  }

  /**
   * Ensures the request URL is valid before passing to axios (avoids "Invalid URL" from axios).
   */
  private assertValidRequestUrl(url: string, label: string): void {
    if (!url || typeof url !== 'string') {
      throw new Error(
        `Python ETL ${label}: request URL is missing. Check ETL_PYTHON_SERVICE_URL (e.g. http://localhost:8001) and run the API from apps/api so .env is loaded.`,
      );
    }
    try {
      const parsed = new URL(url);
      if (!parsed.host) {
        throw new Error('URL has no host');
      }
    } catch (err: any) {
      throw new Error(
        `Python ETL ${label}: invalid URL "${url}". ${err?.message ?? ''} Set ETL_PYTHON_SERVICE_URL in apps/api/.env to a valid base URL (e.g. http://localhost:8001).`,
      );
    }
  }

  /**
   * Discover schema from source
   */
  async discoverSchema(options: {
    sourceSchema: PipelineSourceSchema;
    connectionConfig: any;
    organizationId: string;
    userId: string;
  }): Promise<{
    columns: ColumnInfo[];
    primaryKeys: string[];
    estimatedRowCount?: number;
  }> {
    const { sourceSchema, connectionConfig } = options;

    try {
      const sourceType = this.normalizeSourceType(sourceSchema.sourceType);
      const discoverUrl = `${this.pythonServiceUrl}/discover-schema/${sourceType}`;
      this.assertValidRequestUrl(discoverUrl, 'discover-schema');

      const response = await firstValueFrom(
        this.httpService.post(
          discoverUrl,
          {
            source_type: sourceType,
            connection_config: connectionConfig,
            source_config: sourceSchema.sourceConfig || {},
            table_name: sourceSchema.sourceTable,
            schema_name: sourceSchema.sourceSchema,
            query: sourceSchema.sourceQuery,
          },
          this.buildRequestConfig(30000),
        ),
      );

      return {
        columns: response.data.columns || [],
        primaryKeys: response.data.primary_keys || [],
        estimatedRowCount: response.data.estimated_row_count,
      };
    } catch (error: any) {
      this.logger.error(`Schema discovery failed: ${error.message}`, error.stack);
      throw new Error(`Schema discovery failed: ${error.message}`);
    }
  }

  /**
   * Collect data from source
   */
  async collect(options: {
    sourceSchema: PipelineSourceSchema;
    connectionConfig: any;
    organizationId: string;
    userId: string;
    limit?: number;
    offset?: number;
    cursor?: string;
    syncMode?: 'full' | 'incremental';
    checkpoint?: any;
  }): Promise<{
    rows: any[];
    totalRows?: number;
    nextCursor?: string;
    hasMore?: boolean;
    metadata?: any;
  }> {
    const {
      sourceSchema,
      connectionConfig,
      limit = 500,
      offset = 0,
      cursor,
      syncMode = 'full',
      checkpoint,
    } = options;

    try {
      const sourceType = this.normalizeSourceType(sourceSchema.sourceType);
      const collectUrl = `${this.pythonServiceUrl}/collect/${sourceType}`;
      this.assertValidRequestUrl(collectUrl, 'collect');

      const response = await firstValueFrom(
        this.httpService.post(
          collectUrl,
          {
            source_type: sourceType,
            connection_config: connectionConfig,
            source_config: sourceSchema.sourceConfig || {},
            table_name: sourceSchema.sourceTable,
            schema_name: sourceSchema.sourceSchema,
            query: sourceSchema.sourceQuery,
            sync_mode: syncMode,
            checkpoint: checkpoint || null,
            limit,
            offset,
            cursor: cursor || null,
          },
          this.buildRequestConfig(300000), // 5 minutes for collection (was 60s)
        ),
      );

      return {
        rows: response.data.rows || [],
        totalRows: response.data.total_rows,
        nextCursor: response.data.next_cursor,
        hasMore: response.data.has_more || false,
        metadata: {
          // Include checkpoint in metadata so pipeline service can access it
          checkpoint: response.data.checkpoint,
          ...response.data.metadata,
        },
      };
    } catch (error: any) {
      this.logger.error(`Collection failed: ${error.message}`, error.stack);
      throw new Error(`Collection failed: ${error.message}`);
    }
  }

  /**
   * Delta check for incremental polling.
   */
  async deltaCheck(options: {
    sourceSchema: PipelineSourceSchema;
    connectionConfig: any;
    checkpoint?: any;
  }): Promise<{ hasChanges: boolean; checkpoint?: any }> {
    const { sourceSchema, connectionConfig, checkpoint } = options;
    const sourceType = this.normalizeSourceType(sourceSchema.sourceType);
    const deltaUrl = `${this.pythonServiceUrl}/delta-check/${sourceType}`;
    this.assertValidRequestUrl(deltaUrl, 'delta-check');

    try {
      const response = await firstValueFrom(
        this.httpService.post(
          deltaUrl,
          {
            connection_config: connectionConfig,
            source_config: sourceSchema.sourceConfig || {},
            table_name: sourceSchema.sourceTable,
            schema_name: sourceSchema.sourceSchema,
            query: sourceSchema.sourceQuery,
            checkpoint: checkpoint || null,
          },
          this.buildRequestConfig(60000),
        ),
      );

      return {
        hasChanges: !!response.data?.has_changes,
        checkpoint: response.data?.checkpoint,
      };
    } catch (error: any) {
      this.logger.error(`Delta check failed: ${error.message}`, error.stack);
      throw new Error(`Delta check failed: ${error.message}`);
    }
  }

  /**
   * Transform data using custom Python script
   */
  async transform(options: { rows: any[]; transformScript: string }): Promise<{
    transformedRows: any[];
    errors: any[];
  }> {
    const { rows, transformScript } = options;

    if (!transformScript || !transformScript.trim()) {
      this.logger.warn('Empty transform script provided, returning rows as-is');
      return {
        transformedRows: rows,
        errors: [],
      };
    }

    try {
      const transformUrl = `${this.pythonServiceUrl}/transform`;
      this.assertValidRequestUrl(transformUrl, 'transform');

      const response = await firstValueFrom(
        this.httpService.post(
          transformUrl,
          {
            rows,
            transform_script: transformScript,
          },
          this.buildRequestConfig(300000), // 5 minutes for transformation (was 30s)
        ),
      );

      return {
        transformedRows: response.data.transformed_rows || [],
        errors: response.data.errors || [],
      };
    } catch (error: any) {
      const detail =
        error?.response?.data?.detail ||
        error?.response?.data?.message ||
        error?.response?.data?.error ||
        error?.message ||
        'Unknown transform error';
      this.logger.error(`Transformation failed: ${detail}`, error?.stack);
      throw new Error(`Transformation failed: ${detail}`);
    }
  }

  /**
   * Emit data to destination
   */
  async emit(options: {
    destinationSchema: PipelineDestinationSchema;
    connectionConfig: any;
    organizationId: string;
    userId: string;
    rows: any[];
    writeMode: 'append' | 'upsert' | 'replace';
    upsertKey?: string[];
  }): Promise<WriteResult> {
    const { destinationSchema, connectionConfig, rows, writeMode, upsertKey } = options;

    try {
      // Get destination data source type
      const destDataSource = await this.getDataSourceType(destinationSchema.dataSourceId!);
      const destType = this.normalizeSourceType(destDataSource);
      const emitUrl = `${this.pythonServiceUrl}/emit/${destType}`;
      this.assertValidRequestUrl(emitUrl, 'emit');

      const response = await firstValueFrom(
        this.httpService.post(
          emitUrl,
          {
            destination_type: destType,
            connection_config: connectionConfig,
            destination_config: {}, // Destination-specific config (not stored in schema)
            table_name: destinationSchema.destinationTable,
            schema_name: destinationSchema.destinationSchema || undefined,
            rows: rows,
            write_mode: writeMode,
            upsert_key: upsertKey || [],
          },
          this.buildRequestConfig(300000), // 5 minutes for emission (was 60s)
        ),
      );

      return {
        rowsWritten: response.data.rows_written || 0,
        rowsSkipped: response.data.rows_skipped || 0,
        rowsFailed: response.data.rows_failed || 0,
        errors: response.data.errors || [],
      };
    } catch (error: any) {
      this.logger.error(`Emission failed: ${error.message}`, error.stack);
      throw new Error(`Emission failed: ${error.message}`);
    }
  }

  /**
   * Get connection config for a source schema
   */
  async getConnectionConfig(
    sourceSchema: PipelineSourceSchema,
    organizationId: string,
  ): Promise<any> {
    if (!sourceSchema.dataSourceId) {
      throw new Error('Source schema must have a data source ID');
    }

    const dataSource = await this.dataSourceRepository.findById(sourceSchema.dataSourceId);
    if (!dataSource) {
      throw new Error(`Data source ${sourceSchema.dataSourceId} not found`);
    }

    // Use getDecryptedConnection for internal system calls
    return await this.connectionService.getDecryptedConnection(
      organizationId,
      sourceSchema.dataSourceId,
      'system', // System user for internal calls
    );
  }

  /**
   * Normalize source type to match Python service expectations
   */
  private normalizeSourceType(sourceType: string): string {
    const normalized = sourceType.toLowerCase();
    if (normalized === 'postgres') {
      return 'postgresql';
    }
    return normalized;
  }

  /**
   * Get data source type from repository
   */
  private async getDataSourceType(dataSourceId: string): Promise<string> {
    const dataSource = await this.dataSourceRepository.findById(dataSourceId);
    if (!dataSource) {
      throw new Error(`Data source ${dataSourceId} not found`);
    }
    return dataSource.sourceType;
  }
}
