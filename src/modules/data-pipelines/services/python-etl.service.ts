/**
 * Python ETL Service Client
 * HTTP client for calling Python FastAPI ETL microservice
 * Replaces CollectorService, TransformerService, and EmitterService
 */

import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { DataSourceRepository } from '../../data-sources/repositories/data-source.repository';
import { ConnectionService } from '../../data-sources/connection.service';
import type { WriteResult, ColumnInfo } from '../types/common.types';
import type { PipelineSourceSchema, PipelineDestinationSchema } from '../../../database/schemas';
import { getEtlServiceUrl } from '../../../common/config/etl-url.util';

@Injectable()
export class PythonETLService {
  private readonly logger = new Logger(PythonETLService.name);
  private readonly pythonServiceUrl: string;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
    private readonly dataSourceRepository: DataSourceRepository,
    private readonly connectionService: ConnectionService,
  ) {
    this.pythonServiceUrl = getEtlServiceUrl(this.configService);
    this.logger.log(`Python ETL Service URL: ${this.pythonServiceUrl}`);
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

      const response = await firstValueFrom(
        this.httpService.post(
          `${this.pythonServiceUrl}/discover-schema/${sourceType}`,
          {
            source_type: sourceType,
            connection_config: connectionConfig,
            source_config: sourceSchema.sourceConfig || {},
            table_name: sourceSchema.sourceTable,
            schema_name: sourceSchema.sourceSchema,
            query: sourceSchema.sourceQuery,
          },
          {
            timeout: 30000,
          },
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

      const response = await firstValueFrom(
        this.httpService.post(
          `${this.pythonServiceUrl}/collect/${sourceType}`,
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
          {
            timeout: 300000, // 5 minutes for collection (was 60s)
          },
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
      const response = await firstValueFrom(
        this.httpService.post(
          `${this.pythonServiceUrl}/transform`,
          {
            rows,
            transform_script: transformScript,
          },
          {
            timeout: 300000, // 5 minutes for transformation (was 30s)
          },
        ),
      );

      return {
        transformedRows: response.data.transformed_rows || [],
        errors: response.data.errors || [],
      };
    } catch (error: any) {
      this.logger.error(`Transformation failed: ${error.message}`, error.stack);
      throw new Error(`Transformation failed: ${error.message}`);
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

      const response = await firstValueFrom(
        this.httpService.post(
          `${this.pythonServiceUrl}/emit/${destType}`,
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
          {
            timeout: 300000, // 5 minutes for emission (was 60s)
          },
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
