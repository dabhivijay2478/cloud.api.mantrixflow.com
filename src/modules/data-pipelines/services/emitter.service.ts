/**
 * Emitter Service
 * Generic service for emitting/writing data to any destination type
 * Works with postgres, mysql, mongodb, s3, api, etc.
 */

import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { ConnectionService } from '../../data-sources/connection.service';
import { DataSourceRepository } from '../../data-sources/repositories/data-source.repository';
import type { ColumnMapping, SchemaValidationResult, WriteResult } from '../types/common.types';
import type { PipelineDestinationSchema } from '../../../database/schemas';

@Injectable()
export class EmitterService {
  private readonly logger = new Logger(EmitterService.name);

  constructor(
    private readonly connectionService: ConnectionService,
    private readonly dataSourceRepository: DataSourceRepository,
  ) {}

  /**
   * Write data to destination
   */
  async emit(options: {
    destinationSchema: PipelineDestinationSchema;
    organizationId: string;
    userId: string;
    rows: any[];
    writeMode: 'append' | 'upsert' | 'replace';
    upsertKey?: string[];
  }): Promise<WriteResult> {
    const { destinationSchema, organizationId, userId, rows, writeMode, upsertKey } = options;

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

    // Route to appropriate emitter based on destination type
    switch (dataSource.sourceType) {
      case 'postgres':
      case 'mysql':
        return this.emitToDatabase(destinationSchema, connectionConfig, rows, writeMode, upsertKey);
      case 'mongodb':
        return this.emitToMongoDB(destinationSchema, connectionConfig, rows, writeMode, upsertKey);
      case 's3':
        return this.emitToS3(destinationSchema, connectionConfig, rows, writeMode);
      case 'api':
        return this.emitToAPI(destinationSchema, connectionConfig, rows);
      default:
        throw new BadRequestException(`Unsupported destination type: ${dataSource.sourceType}`);
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

    // Route to appropriate validator
    switch (dataSource.sourceType) {
      case 'postgres':
      case 'mysql':
        return this.validateDatabaseSchema(destinationSchema, connectionConfig, columnMappings);
      case 'mongodb':
        return this.validateMongoDBSchema(destinationSchema, connectionConfig, columnMappings);
      case 's3':
        return this.validateS3Schema(destinationSchema, connectionConfig, columnMappings);
      case 'api':
        return this.validateAPISchema(destinationSchema, connectionConfig, columnMappings);
      default:
        throw new BadRequestException(`Unsupported destination type: ${dataSource.sourceType}`);
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

    // Route to appropriate table creator
    switch (dataSource.sourceType) {
      case 'postgres':
      case 'mysql':
        return this.createDatabaseTable(destinationSchema, connectionConfig, columnMappings);
      case 'mongodb':
        // MongoDB doesn't need table creation
        return { created: false, tableName: destinationSchema.destinationTable };
      case 's3':
        // S3 doesn't need table creation
        return { created: false, tableName: destinationSchema.destinationTable };
      case 'api':
        // API doesn't need table creation
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

    // Route to appropriate checker
    switch (dataSource.sourceType) {
      case 'postgres':
      case 'mysql':
        return this.databaseTableExists(destinationSchema, connectionConfig);
      case 'mongodb':
        // MongoDB collections always exist
        return true;
      case 's3':
        // S3 buckets/keys always exist
        return true;
      case 'api':
        // API endpoints always exist
        return true;
      default:
        return false;
    }
  }

  /**
   * Emit to database (PostgreSQL, MySQL)
   */
  private async emitToDatabase(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    rows: any[],
    writeMode: 'append' | 'upsert' | 'replace',
    upsertKey?: string[],
  ): Promise<WriteResult> {
    // TODO: Implement database emission
    // This should use appropriate database client library
    this.logger.log(
      `Emitting ${rows.length} rows to database: ${destinationSchema.destinationSchema}.${destinationSchema.destinationTable}`,
    );

    // Placeholder
    return {
      rowsWritten: 0,
      rowsSkipped: 0,
      rowsFailed: 0,
    };
  }

  /**
   * Emit to MongoDB
   */
  private async emitToMongoDB(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    rows: any[],
    writeMode: 'append' | 'upsert' | 'replace',
    upsertKey?: string[],
  ): Promise<WriteResult> {
    // TODO: Implement MongoDB emission
    this.logger.log(
      `Emitting ${rows.length} rows to MongoDB: ${destinationSchema.destinationTable}`,
    );
    return {
      rowsWritten: 0,
      rowsSkipped: 0,
      rowsFailed: 0,
    };
  }

  /**
   * Emit to S3
   */
  private async emitToS3(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    rows: any[],
    writeMode: 'append' | 'upsert' | 'replace',
  ): Promise<WriteResult> {
    // TODO: Implement S3 emission
    this.logger.log(`Emitting ${rows.length} rows to S3: ${destinationSchema.destinationTable}`);
    return {
      rowsWritten: 0,
      rowsSkipped: 0,
      rowsFailed: 0,
    };
  }

  /**
   * Emit to API
   */
  private async emitToAPI(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    rows: any[],
  ): Promise<WriteResult> {
    // TODO: Implement API emission
    this.logger.log(`Emitting ${rows.length} rows to API`);
    return {
      rowsWritten: 0,
      rowsSkipped: 0,
      rowsFailed: 0,
    };
  }

  /**
   * Validate database schema
   */
  private async validateDatabaseSchema(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    columnMappings: ColumnMapping[],
  ): Promise<SchemaValidationResult> {
    // TODO: Implement database schema validation
    return {
      valid: true,
      errors: [],
      warnings: [],
    };
  }

  /**
   * Validate MongoDB schema
   */
  private async validateMongoDBSchema(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    columnMappings: ColumnMapping[],
  ): Promise<SchemaValidationResult> {
    // TODO: Implement MongoDB schema validation
    return {
      valid: true,
      errors: [],
      warnings: [],
    };
  }

  /**
   * Validate S3 schema
   */
  private async validateS3Schema(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    columnMappings: ColumnMapping[],
  ): Promise<SchemaValidationResult> {
    // TODO: Implement S3 schema validation
    return {
      valid: true,
      errors: [],
      warnings: [],
    };
  }

  /**
   * Validate API schema
   */
  private async validateAPISchema(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    columnMappings: ColumnMapping[],
  ): Promise<SchemaValidationResult> {
    // TODO: Implement API schema validation
    return {
      valid: true,
      errors: [],
      warnings: [],
    };
  }

  /**
   * Create database table
   */
  private async createDatabaseTable(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
    columnMappings: ColumnMapping[],
  ): Promise<{ created: boolean; tableName: string }> {
    // TODO: Implement database table creation
    this.logger.log(
      `Creating table: ${destinationSchema.destinationSchema}.${destinationSchema.destinationTable}`,
    );
    return {
      created: false,
      tableName: destinationSchema.destinationTable,
    };
  }

  /**
   * Check if database table exists
   */
  private async databaseTableExists(
    destinationSchema: PipelineDestinationSchema,
    connectionConfig: any,
  ): Promise<boolean> {
    // TODO: Implement database table existence check
    return false;
  }
}
