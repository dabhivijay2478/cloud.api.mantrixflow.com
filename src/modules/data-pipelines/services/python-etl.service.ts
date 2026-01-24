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
import type {
  ColumnMapping,
  Transformation,
  WriteResult,
  ColumnInfo,
} from '../types/common.types';
import type { PipelineSourceSchema, PipelineDestinationSchema } from '../../../database/schemas';

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
    this.pythonServiceUrl =
      this.configService.get<string>('ETL_PYTHON_SERVICE_URL') ||
      this.configService.get<string>('PYTHON_SERVICE_URL') ||
      'http://localhost:8001';
    
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
    const { sourceSchema, connectionConfig, limit = 500, offset = 0, cursor, syncMode = 'full', checkpoint } = options;
    
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
            timeout: 60000, // 60 seconds for collection
          },
        ),
      );

      return {
        rows: response.data.rows || [],
        totalRows: response.data.total_rows,
        nextCursor: response.data.next_cursor,
        hasMore: response.data.has_more || false,
        metadata: response.data.metadata,
      };
    } catch (error: any) {
      this.logger.error(`Collection failed: ${error.message}`, error.stack);
      throw new Error(`Collection failed: ${error.message}`);
    }
  }

  /**
   * Transform data
   */
  async transform(options: {
    rows: any[];
    columnMappings: ColumnMapping[];
    transformations?: Transformation[];
  }): Promise<{
    transformedRows: any[];
    errors: any[];
  }> {
    const { rows, columnMappings, transformations } = options;
    
    try {
      const response = await firstValueFrom(
        this.httpService.post(
          `${this.pythonServiceUrl}/transform`,
          {
            rows,
            column_mappings: columnMappings,
            transformations: transformations || [],
          },
          {
            timeout: 30000,
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
    columnMappings?: ColumnMapping[];
  }): Promise<WriteResult> {
    const { destinationSchema, connectionConfig, rows, writeMode, upsertKey, columnMappings } = options;
    
    try {
      // Get destination data source type
      const destDataSource = await this.getDataSourceType(destinationSchema.dataSourceId!);
      const destType = this.normalizeSourceType(destDataSource);
      
      // Transform data first if column mappings provided
      let transformedRows = rows;
      if (columnMappings && columnMappings.length > 0) {
        const transformResult = await this.transform({
          rows,
          columnMappings,
        });
        transformedRows = transformResult.transformedRows;
      }
      
      const response = await firstValueFrom(
        this.httpService.post(
          `${this.pythonServiceUrl}/emit/${destType}`,
          {
            destination_type: destType,
            connection_config: connectionConfig,
            destination_config: {}, // Destination-specific config (not stored in schema)
            table_name: destinationSchema.destinationTable,
            schema_name: destinationSchema.destinationSchema || undefined,
            rows: transformedRows,
            write_mode: writeMode,
            upsert_key: upsertKey || [],
            column_mappings: [],
          },
          {
            timeout: 60000, // 60 seconds for emission
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
