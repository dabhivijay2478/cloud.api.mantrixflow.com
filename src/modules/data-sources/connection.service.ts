/**
 * Connection Service
 * Business logic for data source connection management
 * Handles encryption, validation, testing, and schema discovery for all connection types
 */

import {
  BadRequestException,
  ForbiddenException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { Pool } from 'pg';
import { EncryptionService } from '../../common/encryption/encryption.service';
import type { DataSourceConnection } from '../../database/schemas/data-sources';
import { ActivityLogService } from '../activity-logs/activity-log.service';
import { CONNECTION_ACTIONS } from '../activity-logs/constants/activity-log-types';
import { OrganizationRoleService } from '../organizations/services/organization-role.service';
import { DataSourceRepository } from './repositories/data-source.repository';
import { DataSourceConnectionRepository } from './repositories/data-source-connection.repository';
import type {
  APIConfig,
  BigQueryConfig,
  MongoDBConfig,
  MySQLConfig,
  PostgresConfig,
  S3Config,
  SnowflakeConfig,
} from '../../database/schemas/data-sources/data-source-connections.schema';

export interface CreateConnectionDto {
  connectionType: string;
  config: Record<string, any>;
}

export interface UpdateConnectionDto {
  config?: Record<string, any>;
  status?: 'active' | 'inactive' | 'error' | 'testing';
}

@Injectable()
export class ConnectionService {
  private readonly logger = new Logger(ConnectionService.name);
  private readonly pythonServiceUrl: string;

  constructor(
    private readonly connectionRepository: DataSourceConnectionRepository,
    private readonly dataSourceRepository: DataSourceRepository,
    private readonly encryptionService: EncryptionService,
    private readonly activityLogService: ActivityLogService,
    private readonly roleService: OrganizationRoleService,
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
  ) {
    this.pythonServiceUrl =
      this.configService.get<string>('ETL_PYTHON_SERVICE_URL') ||
      this.configService.get<string>('PYTHON_SERVICE_URL') ||
      'http://localhost:8001';
  }

  /**
   * Encrypt sensitive fields in config based on connection type
   */
  private encryptConfig(connectionType: string, config: Record<string, any>): Record<string, any> {
    const encrypted = { ...config };

    switch (connectionType) {
      case 'postgres':
      case 'mysql':
        if (encrypted.password) {
          encrypted.password = this.encryptionService.encrypt(encrypted.password);
        }
        if (encrypted.ssl?.ca_cert) {
          encrypted.ssl.ca_cert = this.encryptionService.encrypt(encrypted.ssl.ca_cert);
        }
        if (encrypted.ssl?.client_cert) {
          encrypted.ssl.client_cert = this.encryptionService.encrypt(encrypted.ssl.client_cert);
        }
        if (encrypted.ssl?.client_key) {
          encrypted.ssl.client_key = this.encryptionService.encrypt(encrypted.ssl.client_key);
        }
        if (encrypted.ssh_tunnel?.private_key) {
          encrypted.ssh_tunnel.private_key = this.encryptionService.encrypt(
            encrypted.ssh_tunnel.private_key,
          );
        }
        break;
      case 'mongodb':
        if (encrypted.connection_string) {
          encrypted.connection_string = this.encryptionService.encrypt(encrypted.connection_string);
        }
        if (encrypted.password) {
          encrypted.password = this.encryptionService.encrypt(encrypted.password);
        }
        break;
      case 's3':
        if (encrypted.access_key_id) {
          encrypted.access_key_id = this.encryptionService.encrypt(encrypted.access_key_id);
        }
        if (encrypted.secret_access_key) {
          encrypted.secret_access_key = this.encryptionService.encrypt(encrypted.secret_access_key);
        }
        break;
      case 'api':
        if (encrypted.auth_token) {
          encrypted.auth_token = this.encryptionService.encrypt(encrypted.auth_token);
        }
        if (encrypted.api_key) {
          encrypted.api_key = this.encryptionService.encrypt(encrypted.api_key);
        }
        break;
      case 'bigquery':
        if (encrypted.credentials?.private_key) {
          encrypted.credentials.private_key = this.encryptionService.encrypt(
            encrypted.credentials.private_key,
          );
        }
        break;
      case 'snowflake':
        if (encrypted.password) {
          encrypted.password = this.encryptionService.encrypt(encrypted.password);
        }
        break;
    }

    return encrypted;
  }

  /**
   * Decrypt sensitive fields in config based on connection type
   */
  private decryptConfig(connectionType: string, config: Record<string, any>): Record<string, any> {
    const decrypted = { ...config };

    try {
      switch (connectionType) {
        case 'postgres':
        case 'mysql':
          if (decrypted.password) {
            decrypted.password = this.encryptionService.decrypt(decrypted.password);
          }
          if (decrypted.ssl?.ca_cert) {
            decrypted.ssl.ca_cert = this.encryptionService.decrypt(decrypted.ssl.ca_cert);
          }
          if (decrypted.ssl?.client_cert) {
            decrypted.ssl.client_cert = this.encryptionService.decrypt(decrypted.ssl.client_cert);
          }
          if (decrypted.ssl?.client_key) {
            decrypted.ssl.client_key = this.encryptionService.decrypt(decrypted.ssl.client_key);
          }
          if (decrypted.ssh_tunnel?.private_key) {
            decrypted.ssh_tunnel.private_key = this.encryptionService.decrypt(
              decrypted.ssh_tunnel.private_key,
            );
          }
          break;
        case 'mongodb':
          if (decrypted.connection_string) {
            decrypted.connection_string = this.encryptionService.decrypt(
              decrypted.connection_string,
            );
          }
          if (decrypted.password) {
            decrypted.password = this.encryptionService.decrypt(decrypted.password);
          }
          break;
        case 's3':
          if (decrypted.access_key_id) {
            decrypted.access_key_id = this.encryptionService.decrypt(decrypted.access_key_id);
          }
          if (decrypted.secret_access_key) {
            decrypted.secret_access_key = this.encryptionService.decrypt(
              decrypted.secret_access_key,
            );
          }
          break;
        case 'api':
          if (decrypted.auth_token) {
            decrypted.auth_token = this.encryptionService.decrypt(decrypted.auth_token);
          }
          if (decrypted.api_key) {
            decrypted.api_key = this.encryptionService.decrypt(decrypted.api_key);
          }
          break;
        case 'bigquery':
          if (decrypted.credentials?.private_key) {
            decrypted.credentials.private_key = this.encryptionService.decrypt(
              decrypted.credentials.private_key,
            );
          }
          break;
        case 'snowflake':
          if (decrypted.password) {
            decrypted.password = this.encryptionService.decrypt(decrypted.password);
          }
          break;
      }
    } catch (error) {
      this.logger.error(
        `Failed to decrypt config for ${connectionType}`,
        error instanceof Error ? error.stack : String(error),
      );
      throw new BadRequestException('Failed to decrypt connection credentials');
    }

    return decrypted;
  }

  /**
   * Mask sensitive fields in config for API responses
   */
  maskSensitiveFields(connectionType: string, config: Record<string, any>): Record<string, any> {
    const masked = JSON.parse(JSON.stringify(config)); // Deep clone

    switch (connectionType) {
      case 'postgres':
      case 'mysql':
        if (masked.password) {
          masked.password = '****';
        }
        if (masked.ssl?.ca_cert) {
          masked.ssl.ca_cert = '****';
        }
        if (masked.ssl?.client_cert) {
          masked.ssl.client_cert = '****';
        }
        if (masked.ssl?.client_key) {
          masked.ssl.client_key = '****';
        }
        if (masked.ssh_tunnel?.private_key) {
          masked.ssh_tunnel.private_key = '****';
        }
        break;
      case 'mongodb':
        if (masked.connection_string) {
          // Mask password in connection string
          masked.connection_string = masked.connection_string.replace(
            /:\/\/[^:]+:[^@]+@/,
            '://****:****@',
          );
        }
        if (masked.password) {
          masked.password = '****';
        }
        break;
      case 's3':
        if (masked.access_key_id) {
          masked.access_key_id = `${masked.access_key_id.substring(0, 4)}****`;
        }
        if (masked.secret_access_key) {
          masked.secret_access_key = '****';
        }
        break;
      case 'api':
        if (masked.auth_token) {
          masked.auth_token = '****';
        }
        if (masked.api_key) {
          masked.api_key = '****';
        }
        break;
      case 'bigquery':
        if (masked.credentials?.private_key) {
          masked.credentials.private_key = '****';
        }
        break;
      case 'snowflake':
        if (masked.password) {
          masked.password = '****';
        }
        break;
    }

    return masked;
  }

  /**
   * Validate connection config structure based on type
   */
  validateConnectionConfig(connectionType: string, config: Record<string, any>): void {
    switch (connectionType) {
      case 'postgres':
        this.validatePostgresConfig(config as PostgresConfig);
        break;
      case 'mysql':
        this.validateMySQLConfig(config as MySQLConfig);
        break;
      case 'mongodb':
        this.validateMongoDBConfig(config as MongoDBConfig);
        break;
      case 's3':
        this.validateS3Config(config as S3Config);
        break;
      case 'api':
        this.validateAPIConfig(config as APIConfig);
        break;
      case 'bigquery':
        this.validateBigQueryConfig(config as BigQueryConfig);
        break;
      case 'snowflake':
        this.validateSnowflakeConfig(config as SnowflakeConfig);
        break;
      default:
        throw new BadRequestException(`Unsupported connection type: ${connectionType}`);
    }
  }

  private validatePostgresConfig(config: PostgresConfig): void {
    if (!config.host || !config.port || !config.database || !config.username || !config.password) {
      throw new BadRequestException(
        'PostgreSQL config requires: host, port, database, username, password',
      );
    }
    if (typeof config.port !== 'number' || config.port < 1 || config.port > 65535) {
      throw new BadRequestException('Port must be a number between 1 and 65535');
    }
  }

  private validateMySQLConfig(config: MySQLConfig): void {
    if (!config.host || !config.port || !config.database || !config.username || !config.password) {
      throw new BadRequestException(
        'MySQL config requires: host, port, database, username, password',
      );
    }
    if (typeof config.port !== 'number' || config.port < 1 || config.port > 65535) {
      throw new BadRequestException('Port must be a number between 1 and 65535');
    }
  }

  private validateMongoDBConfig(config: MongoDBConfig): void {
    if (!config.connection_string && (!config.host || !config.database)) {
      throw new BadRequestException(
        'MongoDB config requires: connection_string OR (host and database)',
      );
    }
  }

  private validateS3Config(config: S3Config): void {
    if (!config.bucket || !config.region || !config.access_key_id || !config.secret_access_key) {
      throw new BadRequestException(
        'S3 config requires: bucket, region, access_key_id, secret_access_key',
      );
    }
  }

  private validateAPIConfig(config: APIConfig): void {
    if (!config.base_url || !config.auth_type) {
      throw new BadRequestException('API config requires: base_url, auth_type');
    }
    if (!['bearer', 'api_key', 'oauth2', 'basic'].includes(config.auth_type)) {
      throw new BadRequestException('auth_type must be one of: bearer, api_key, oauth2, basic');
    }
  }

  private validateBigQueryConfig(config: BigQueryConfig): void {
    if (!config.project_id || !config.dataset || !config.credentials) {
      throw new BadRequestException('BigQuery config requires: project_id, dataset, credentials');
    }
    if (!config.credentials.private_key || !config.credentials.client_email) {
      throw new BadRequestException('BigQuery credentials require: private_key, client_email');
    }
  }

  private validateSnowflakeConfig(config: SnowflakeConfig): void {
    if (
      !config.account ||
      !config.username ||
      !config.password ||
      !config.warehouse ||
      !config.database ||
      !config.schema
    ) {
      throw new BadRequestException(
        'Snowflake config requires: account, username, password, warehouse, database, schema',
      );
    }
  }

  /**
   * Create or update connection for a data source
   * AUTHORIZATION: Only ADMIN or OWNER can configure connections
   */
  async createOrUpdateConnection(
    organizationId: string,
    dataSourceId: string,
    userId: string,
    dto: CreateConnectionDto,
  ): Promise<DataSourceConnection> {
    // Verify data source exists and belongs to organization
    const dataSource = await this.dataSourceRepository.findById(dataSourceId);
    if (!dataSource) {
      throw new NotFoundException(`Data source with ID "${dataSourceId}" not found`);
    }
    if (dataSource.organizationId !== organizationId) {
      throw new ForbiddenException('Data source does not belong to this organization');
    }

    // Verify connection type matches data source type
    if (dto.connectionType !== dataSource.sourceType) {
      throw new BadRequestException(
        `Connection type "${dto.connectionType}" does not match data source type "${dataSource.sourceType}"`,
      );
    }

    // AUTHORIZATION: Check if user can manage data sources
    const canManage = await this.roleService.canManageDataSources(userId, organizationId);
    if (!canManage) {
      throw new ForbiddenException('Only OWNER, ADMIN, and EDITOR can configure connections');
    }

    // Validate config structure
    this.validateConnectionConfig(dto.connectionType, dto.config);

    // Encrypt sensitive fields
    const encryptedConfig = this.encryptConfig(dto.connectionType, dto.config);

    // Check if connection already exists
    const existing = await this.connectionRepository.findByDataSourceId(dataSourceId);

    if (existing) {
      // Update existing connection
      const updated = await this.connectionRepository.updateByDataSourceId(dataSourceId, {
        connectionType: dto.connectionType,
        config: encryptedConfig,
        status: 'inactive', // Reset status when config changes
      });

      // Log activity
      try {
        await this.activityLogService.logConnectionAction(
          organizationId,
          userId,
          CONNECTION_ACTIONS.UPDATED,
          updated.id,
          dataSource.name,
        );
      } catch (error) {
        this.logger.error(
          'Failed to log connection update activity',
          error instanceof Error ? error.stack : String(error),
        );
      }

      return updated;
    } else {
      // Create new connection
      const connection = await this.connectionRepository.create({
        dataSourceId,
        connectionType: dto.connectionType,
        config: encryptedConfig,
        status: 'inactive',
      });

      // Log activity
      try {
        await this.activityLogService.logConnectionAction(
          organizationId,
          userId,
          CONNECTION_ACTIONS.CREATED,
          connection.id,
          dataSource.name,
        );
      } catch (error) {
        this.logger.error(
          'Failed to log connection creation activity',
          error instanceof Error ? error.stack : String(error),
        );
      }

      return connection;
    }
  }

  /**
   * Get connection for a data source
   * Returns masked config for display (sensitive fields hidden)
   */
  async getConnection(
    organizationId: string,
    dataSourceId: string,
    userId: string,
    includeSensitive: boolean = false,
  ): Promise<DataSourceConnection | null> {
    // Verify data source exists and belongs to organization
    const dataSource = await this.dataSourceRepository.findById(dataSourceId);
    if (!dataSource) {
      throw new NotFoundException(`Data source with ID "${dataSourceId}" not found`);
    }
    if (dataSource.organizationId !== organizationId) {
      throw new ForbiddenException('Data source does not belong to this organization');
    }

    // AUTHORIZATION: Check if user can view organization
    const canView = await this.roleService.canViewOrganization(userId, organizationId);
    if (!canView) {
      throw new ForbiddenException('You are not a member of this organization');
    }

    const connection = await this.connectionRepository.findByDataSourceId(dataSourceId);
    if (!connection) {
      return null;
    }

    // Mask sensitive fields if not including sensitive data
    if (!includeSensitive) {
      return {
        ...connection,
        config: this.maskSensitiveFields(connection.connectionType, connection.config as any),
      } as DataSourceConnection;
    }

    // For sensitive access, check if user has EDITOR+ role
    const canManage = await this.roleService.canManageDataSources(userId, organizationId);
    if (!canManage) {
      throw new ForbiddenException('Only OWNER, ADMIN, and EDITOR can view connection credentials');
    }

    // Log activity
    try {
      await this.activityLogService.logConnectionAction(
        organizationId,
        userId,
        CONNECTION_ACTIONS.VIEWED,
        connection.id,
        dataSource.name,
      );
    } catch (error) {
      this.logger.error(
        'Failed to log connection view activity',
        error instanceof Error ? error.stack : String(error),
      );
    }

    return connection;
  }

  /**
   * Delete connection for a data source
   * AUTHORIZATION: Only ADMIN or OWNER can delete connections
   */
  async deleteConnection(
    organizationId: string,
    dataSourceId: string,
    userId: string,
  ): Promise<void> {
    // Verify data source exists and belongs to organization
    const dataSource = await this.dataSourceRepository.findById(dataSourceId);
    if (!dataSource) {
      throw new NotFoundException(`Data source with ID "${dataSourceId}" not found`);
    }
    if (dataSource.organizationId !== organizationId) {
      throw new ForbiddenException('Data source does not belong to this organization');
    }

    // AUTHORIZATION: Check if user can manage data sources
    const canManage = await this.roleService.canManageDataSources(userId, organizationId);
    if (!canManage) {
      throw new ForbiddenException('Only OWNER, ADMIN, and EDITOR can delete connections');
    }

    // Check if connection exists
    const connection = await this.connectionRepository.findByDataSourceId(dataSourceId);
    if (!connection) {
      throw new NotFoundException('Connection not found for this data source');
    }

    // Delete connection
    await this.connectionRepository.deleteByDataSourceId(dataSourceId);

    // Log activity
    try {
      await this.activityLogService.logConnectionAction(
        organizationId,
        userId,
        CONNECTION_ACTIONS.DELETED,
        connection.id,
        dataSource.name,
      );
    } catch (error) {
      this.logger.error(
        'Failed to log connection deletion activity',
        error instanceof Error ? error.stack : String(error),
      );
    }
  }

  /**
   * Get decrypted connection config for actual use
   * AUTHORIZATION: Only EDITOR+ can get decrypted config
   */
  async getDecryptedConnection(
    organizationId: string,
    dataSourceId: string,
    userId: string,
  ): Promise<Record<string, any>> {
    const connection = await this.getConnection(organizationId, dataSourceId, userId, true);
    if (!connection) {
      throw new NotFoundException('Connection not found for this data source');
    }

    // Decrypt config
    return this.decryptConfig(connection.connectionType, connection.config as any);
  }

  /**
   * Normalize source type for Python service
   */
  private normalizeSourceTypeForPython(connectionType: string): string {
    const normalized = connectionType.toLowerCase();
    if (normalized === 'postgres' || normalized === 'pgvector' || normalized === 'redshift') {
      return 'postgresql';
    }
    if (normalized === 'mysql') {
      return 'mysql';
    }
    if (normalized === 'mongodb') {
      return 'mongodb';
    }
    return normalized;
  }

  /**
   * Test connection
   * AUTHORIZATION: Only EDITOR+ can test connections
   * Now calls Python service for actual connection testing
   */
  async testConnection(
    organizationId: string,
    dataSourceId: string,
    userId: string,
    authToken?: string,
  ): Promise<{ success: boolean; message: string; details?: any }> {
    // Verify data source exists
    const dataSource = await this.dataSourceRepository.findById(dataSourceId);
    if (!dataSource) {
      throw new NotFoundException(`Data source with ID "${dataSourceId}" not found`);
    }
    if (dataSource.organizationId !== organizationId) {
      throw new ForbiddenException('Data source does not belong to this organization');
    }

    // AUTHORIZATION: Check if user can manage data sources
    const canManage = await this.roleService.canManageDataSources(userId, organizationId);
    if (!canManage) {
      throw new ForbiddenException('Only OWNER, ADMIN, and EDITOR can test connections');
    }

    const connection = await this.connectionRepository.findByDataSourceId(dataSourceId);
    if (!connection) {
      throw new NotFoundException('Connection not configured for this data source');
    }

    // Update status to testing
    await this.connectionRepository.updateByDataSourceId(dataSourceId, {
      status: 'testing',
    });

    let testResult: { success: boolean; message: string; details?: any };

    try {
      // Decrypt config
      const decryptedConfig = this.decryptConfig(
        connection.connectionType,
        connection.config as any,
      );

      // Call Python service to test connection
      // Get auth token from request context (we'll need to pass it)
      // For now, we'll call Python service directly - it will handle auth via JWT
      const sourceType = this.normalizeSourceTypeForPython(connection.connectionType);

      // Build request payload for Python service
      const pythonRequest: any = {
        type: sourceType,
      };

      // Map connection config to Python service format
      if (sourceType === 'mongodb') {
        if (decryptedConfig.connection_string) {
          pythonRequest.connection_string_mongo = decryptedConfig.connection_string;
        } else {
          pythonRequest.host = decryptedConfig.host || 'localhost';
          pythonRequest.port = decryptedConfig.port || 27017;
          pythonRequest.database = decryptedConfig.database || '';
          pythonRequest.username = decryptedConfig.username;
          pythonRequest.password = decryptedConfig.password;
          if (decryptedConfig.auth_source) {
            pythonRequest.auth_source = decryptedConfig.auth_source;
          }
          if (decryptedConfig.replica_set) {
            pythonRequest.replica_set = decryptedConfig.replica_set;
          }
          if (decryptedConfig.tls !== undefined) {
            pythonRequest.tls = decryptedConfig.tls;
          }
        }
      } else {
        // SQL databases (PostgreSQL, MySQL)
        if (decryptedConfig.connection_string) {
          pythonRequest.connection_string = decryptedConfig.connection_string;
        } else {
          pythonRequest.host = decryptedConfig.host || 'localhost';
          pythonRequest.port = decryptedConfig.port || (sourceType === 'postgresql' ? 5432 : 3306);
          pythonRequest.database = decryptedConfig.database || '';
          pythonRequest.username = decryptedConfig.username || '';
          pythonRequest.password = decryptedConfig.password || '';
          if (decryptedConfig.ssl) {
            pythonRequest.ssl = decryptedConfig.ssl;
          }
        }
      }

      // Call Python service
      this.logger.log(`Calling Python service to test ${sourceType} connection`);

      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
      };

      if (authToken) {
        headers['Authorization'] = `Bearer ${authToken}`;
      }

      let pythonResponse;
      try {
        pythonResponse = await firstValueFrom(
          this.httpService.post(`${this.pythonServiceUrl}/test-connection`, pythonRequest, {
            headers,
            timeout: 30000,
          }),
        );
      } catch (error: any) {
        const errorMessage =
          error?.response?.data?.detail ||
          error?.response?.data?.error ||
          error?.message ||
          'Failed to connect to Python service';
        this.logger.error(`Python service call failed: ${errorMessage}`, error?.stack);
        throw new BadRequestException(`Connection test failed: ${errorMessage}`);
      }

      // Map Python response to our format
      testResult = {
        success: pythonResponse.data.success || false,
        message:
          pythonResponse.data.message || pythonResponse.data.error || 'Connection test completed',
        details: pythonResponse.data.details || {
          version: pythonResponse.data.version,
          response_time_ms: pythonResponse.data.response_time_ms,
        },
      };

      // Update connection status and test result
      await this.connectionRepository.updateByDataSourceId(dataSourceId, {
        status: testResult.success ? 'active' : 'error',
        lastConnectedAt: testResult.success ? new Date() : undefined,
        lastError: testResult.success ? null : testResult.message,
        testResult: testResult as any,
      });

      // Log activity
      try {
        await this.activityLogService.logConnectionAction(
          organizationId,
          userId,
          CONNECTION_ACTIONS.TESTED,
          connection.id,
          dataSource.name,
          {
            success: testResult.success,
            message: testResult.message,
          },
        );
      } catch (error) {
        this.logger.error(
          'Failed to log connection test activity',
          error instanceof Error ? error.stack : String(error),
        );
      }

      return testResult;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      await this.connectionRepository.updateByDataSourceId(dataSourceId, {
        status: 'error',
        lastError: errorMessage,
        testResult: { success: false, message: errorMessage } as any,
      });

      // Log activity
      try {
        await this.activityLogService.logConnectionAction(
          organizationId,
          userId,
          CONNECTION_ACTIONS.TESTED,
          connection.id,
          dataSource.name,
          {
            success: false,
            error: errorMessage,
          },
        );
      } catch (logError) {
        this.logger.error(
          'Failed to log connection test failure activity',
          logError instanceof Error ? logError.stack : String(logError),
        );
      }

      throw new BadRequestException(`Connection test failed: ${errorMessage}`);
    }
  }

  /**
   * Test PostgreSQL connection
   */
  /**
   * Test connection with explicit configuration (ad-hoc)
   * Publicly accessible for pre-save testing
   */
  async testConnectionConfig(
    connectionType: string,
    config: Record<string, any>,
  ): Promise<{ success: boolean; message: string; details?: any }> {
    this.logger.log(`[testConnectionConfig] Testing connection type: ${connectionType}`);

    switch (connectionType) {
      // SQL Databases
      case 'postgres':
      case 'pgvector':
        return this.testPostgresConnection(config as PostgresConfig);

      case 'mysql':
        return this.testMySQLConnection(config as MySQLConfig);

      case 'mongodb':
        return this.testMongoDBConnection(config as MongoDBConfig);

      case 's3':
      case 's3-datalake':
        return this.testS3Connection(config as S3Config);

      case 'api':
        return this.testAPIConnection(config as APIConfig);

      // SQL databases that use similar connection pattern to postgres
      case 'redshift':
        // Redshift is PostgreSQL-compatible
        return this.testPostgresConnection(config as PostgresConfig);

      case 'mssql':
      case 'clickhouse':
      case 'snowflake':
      case 'snowflake-cortex':
      case 'bigquery':
      case 'databricks':
      case 'azure-blob-storage':
      case 'pinecone':
      case 'milvus':
      case 'weaviate':
      case 'customer-io':
      case 'hubspot':
      case 'salesforce':
      case 'google-sheets':
      case 'excel':
        // Return placeholder for not-yet-implemented connections
        return {
          success: false,
          message: `Connection test for ${connectionType} is not yet implemented. The connection configuration has been saved but cannot be verified at this time.`,
          details: {
            connectionType,
            status: 'not_implemented',
            configReceived: Object.keys(config),
          },
        };

      default:
        this.logger.warn(`Unsupported connection type: ${connectionType}`);
        throw new BadRequestException(
          `Unsupported connection type: ${connectionType}. Supported types: postgres, mysql, mongodb, s3, api, redshift, mssql, clickhouse, snowflake, bigquery, databricks, azure-blob-storage, pinecone, milvus, weaviate`,
        );
    }
  }

  /**
   * Test PostgreSQL connection
   */
  private async testPostgresConnection(config: PostgresConfig): Promise<{
    success: boolean;
    message: string;
    details?: any;
  }> {
    try {
      const pool = new Pool({
        host: config.host,
        port: config.port,
        user: config.username,
        password: config.password,
        database: config.database,
        ssl: config.ssl?.enabled
          ? {
              rejectUnauthorized: false,
              ca: config.ssl.ca_cert,
              cert: config.ssl.client_cert,
              key: config.ssl.client_key,
            }
          : false,
        connectionTimeoutMillis: 5000, // 5s timeout
      });

      const client = await pool.connect();
      try {
        await client.query('SELECT 1');
        return {
          success: true,
          message: 'Successfully connected to PostgreSQL database',
        };
      } finally {
        client.release();
        await pool.end();
      }
    } catch (error) {
      this.logger.error(`PostgreSQL connection test failed: ${error.message}`);
      return {
        success: false,
        message: `Connection failed: ${error.message}`,
        details: error,
      };
    }
  }

  /**
   * Test MySQL connection
   */
  private async testMySQLConnection(_config: MySQLConfig): Promise<{
    success: boolean;
    message: string;
    details?: any;
  }> {
    // TODO: Implement actual MySQL connection test
    return {
      success: true,
      message: 'MySQL connection test not yet implemented',
    };
  }

  /**
   * Test MongoDB connection
   * Supports both connection string (Atlas SRV) and individual host/port config
   */
  private async testMongoDBConnection(config: MongoDBConfig): Promise<{
    success: boolean;
    message: string;
    details?: any;
  }> {
    // Import mongodb driver dynamically to avoid issues if not installed
    let MongoClient: any;
    try {
      const mongodb = await import('mongodb');
      MongoClient = mongodb.MongoClient;
    } catch {
      return {
        success: false,
        message: 'MongoDB driver not installed. Run: npm install mongodb',
        details: { error: 'DRIVER_NOT_INSTALLED' },
      };
    }

    try {
      let connectionString: string;

      // Build connection string from config
      if (config.connection_string) {
        // Use provided connection string (Atlas SRV format)
        connectionString = config.connection_string;
        this.logger.log(`Testing MongoDB connection with connection string`);
      } else if (config.host) {
        // Build connection string from individual parts
        const auth =
          config.username && config.password
            ? `${encodeURIComponent(config.username)}:${encodeURIComponent(config.password)}@`
            : '';
        const port = config.port || 27017;
        const authSource = config.auth_source ? `?authSource=${config.auth_source}` : '';
        connectionString = `mongodb://${auth}${config.host}:${port}/${config.database}${authSource}`;
        this.logger.log(`Testing MongoDB connection to ${config.host}:${port}`);
      } else {
        return {
          success: false,
          message: 'Either connection_string or host is required',
          details: { error: 'INVALID_CONFIG' },
        };
      }

      // Connection options
      const options: any = {
        serverSelectionTimeoutMS: 10000, // 10 second timeout
        connectTimeoutMS: 10000,
      };

      // Add TLS if specified
      if (config.tls) {
        options.tls = true;
      }

      // Create client and connect
      const client = new MongoClient(connectionString, options);

      await client.connect();

      // Try to ping the database to verify connection
      const adminDb = client.db('admin');
      const result = await adminDb.command({ ping: 1 });

      // Get server info
      const serverInfo = await adminDb.command({ serverStatus: 1 }).catch(() => null);

      await client.close();

      return {
        success: true,
        message: 'MongoDB connection successful',
        details: {
          ping: result.ok === 1 ? 'ok' : 'failed',
          version: serverInfo?.version || 'Unknown',
          host: serverInfo?.host || 'Connected',
        },
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.logger.error(`MongoDB connection test failed: ${errorMessage}`);

      return {
        success: false,
        message: `Connection failed: ${errorMessage}`,
        details: {
          error: errorMessage,
          code: (error as any)?.code,
          name: (error as any)?.name,
        },
      };
    }
  }

  /**
   * Test S3 connection
   */
  private async testS3Connection(_config: S3Config): Promise<{
    success: boolean;
    message: string;
    details?: any;
  }> {
    // TODO: Implement actual S3 connection test
    return {
      success: true,
      message: 'S3 connection test not yet implemented',
    };
  }

  /**
   * Test API connection
   */
  private async testAPIConnection(_config: APIConfig): Promise<{
    success: boolean;
    message: string;
    details?: any;
  }> {
    // TODO: Implement actual API connection test
    return {
      success: true,
      message: 'API connection test not yet implemented',
    };
  }

  /**
   * Discover schema for a connection
   * AUTHORIZATION: Only EDITOR+ can discover schemas
   */
  async discoverSchema(organizationId: string, dataSourceId: string, userId: string): Promise<any> {
    // Verify data source exists
    const dataSource = await this.dataSourceRepository.findById(dataSourceId);
    if (!dataSource) {
      throw new NotFoundException(`Data source with ID "${dataSourceId}" not found`);
    }
    if (dataSource.organizationId !== organizationId) {
      throw new ForbiddenException('Data source does not belong to this organization');
    }

    // AUTHORIZATION: Check if user can manage data sources
    const canManage = await this.roleService.canManageDataSources(userId, organizationId);
    if (!canManage) {
      throw new ForbiddenException('Only OWNER, ADMIN, and EDITOR can discover schemas');
    }

    const connection = await this.connectionRepository.findByDataSourceId(dataSourceId);
    if (!connection) {
      throw new NotFoundException('Connection not configured for this data source');
    }

    // Decrypt config
    const decryptedConfig = this.decryptConfig(connection.connectionType, connection.config as any);

    const type = (connection.connectionType || dataSource.sourceType || '').toLowerCase();
    this.logger.log(`Discovering schema for type: ${type}`);

    let schema: any = {};

    switch (type) {
      case 'postgres':
      case 'pgvector':
      case 'redshift':
        schema = await this.discoverPostgresSchema(decryptedConfig as PostgresConfig);
        break;
      case 'mongodb':
        schema = await this.discoverMongoDBSchema(decryptedConfig as MongoDBConfig);
        break;
      // Add other types here
      default:
        this.logger.warn(`Unsupported connection type for schema discovery: ${type}`);
        schema = {
          message: `Unsupported connection type. API Resolved type: '${type}'`,
          debug: {
            connectionType: connection.connectionType,
            dataSourceType: dataSource.sourceType,
          },
        };
        break;
    }

    // Update schema cache
    await this.connectionRepository.updateByDataSourceId(dataSourceId, {
      schemaCache: schema,
      schemaCachedAt: new Date(),
    });

    // Log activity
    try {
      await this.activityLogService.logConnectionAction(
        organizationId,
        userId,
        CONNECTION_ACTIONS.SCHEMA_CACHED,
        connection.id,
        dataSource.name,
      );
    } catch (error) {
      this.logger.error(
        'Failed to log schema discovery activity',
        error instanceof Error ? error.stack : String(error),
      );
    }

    return schema;
  }

  /**
   * Discover PostgreSQL schema
   */
  private async discoverPostgresSchema(config: PostgresConfig): Promise<any> {
    const pool = new Pool({
      host: config.host,
      port: config.port,
      user: config.username,
      password: config.password,
      database: config.database,
      ssl: config.ssl ? { rejectUnauthorized: false } : undefined,
      connectionTimeoutMillis: 10000,
    });

    try {
      const client = await pool.connect();
      try {
        this.logger.log(`Connecting to Postgres database: ${config.database}`);

        // 1. Get Schemas
        const schemasResult = await client.query(`
          SELECT schema_name 
          FROM information_schema.schemata 
          WHERE schema_name NOT IN ('information_schema', 'pg_catalog') 
          AND schema_name NOT LIKE 'pg_toast%' 
          AND schema_name NOT LIKE 'pg_temp%'
          ORDER BY schema_name
        `);

        this.logger.log(`Found ${schemasResult.rows.length} schemas in Postgres`);

        const result = { schemas: [] as any[] };

        for (const row of schemasResult.rows) {
          const schemaName = row.schema_name;

          // 2. Get Tables for Schema
          const tablesResult = await client.query(
            `
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = $1
            ORDER BY table_name
          `,
            [schemaName],
          );

          // 3. Get Columns for Schema
          const columnsResult = await client.query(
            `
            SELECT table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = $1
            ORDER BY table_name, ordinal_position
          `,
            [schemaName],
          );

          // Group columns by table
          const columnsMap = new Map<string, any[]>();
          for (const col of columnsResult.rows) {
            if (!columnsMap.has(col.table_name)) {
              columnsMap.set(col.table_name, []);
            }
            columnsMap.get(col.table_name)?.push({
              name: col.column_name,
              type: col.data_type,
              nullable: col.is_nullable === 'YES',
            });
          }

          const tables = tablesResult.rows.map((t) => ({
            name: t.table_name,
            type: t.table_type === 'BASE TABLE' ? 'table' : 'view',
            columns: columnsMap.get(t.table_name) || [],
          }));

          this.logger.log(`Schema '${schemaName}': Found ${tables.length} tables`);

          result.schemas.push({
            name: schemaName,
            tables: tables,
          });
        }

        return result;
      } finally {
        client.release();
      }
    } catch (error) {
      this.logger.error(
        `Failed to discover Postgres schema: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw new BadRequestException(
        `Failed to discover schema: ${error instanceof Error ? error.message : 'Unknown error'}`,
      );
    } finally {
      await pool.end();
    }
  }

  /**
   * Discover MongoDB schema (collections and their field structure)
   * For NoSQL databases, we sample documents to infer the schema
   */
  private async discoverMongoDBSchema(config: MongoDBConfig): Promise<any> {
    // Import mongodb driver dynamically
    let MongoClient: any;
    try {
      const mongodb = await import('mongodb');
      MongoClient = mongodb.MongoClient;
    } catch {
      throw new BadRequestException('MongoDB driver not installed. Run: npm install mongodb');
    }

    let client: any = null;

    try {
      let connectionString: string;
      let databaseName: string | undefined;

      // Build connection string from config
      if (config.connection_string) {
        connectionString = config.connection_string;
        // Try to extract database name from connection string
        const dbMatch = connectionString.match(/\/([^/?]+)(\?|$)/);
        databaseName = dbMatch?.[1] || config.database;
        this.logger.log(`Discovering MongoDB schema using connection string`);
      } else if (config.host) {
        const auth =
          config.username && config.password
            ? `${encodeURIComponent(config.username)}:${encodeURIComponent(config.password)}@`
            : '';
        const port = config.port || 27017;
        const authSource = config.auth_source ? `?authSource=${config.auth_source}` : '';
        databaseName = config.database;
        connectionString = `mongodb://${auth}${config.host}:${port}/${databaseName}${authSource}`;
        this.logger.log(`Discovering MongoDB schema for ${config.host}:${port}/${databaseName}`);
      } else {
        throw new BadRequestException('Either connection_string or host is required');
      }

      // Connection options
      const options: any = {
        serverSelectionTimeoutMS: 10000,
        connectTimeoutMS: 10000,
      };

      if (config.tls) {
        options.tls = true;
      }

      // Connect
      client = new MongoClient(connectionString, options);
      await client.connect();

      // Get database - use specified database or list all
      const adminDb = client.db('admin');

      // Get list of databases
      const dbList = await adminDb.admin().listDatabases();
      this.logger.log(`Found ${dbList.databases.length} databases`);

      const result = {
        type: 'mongodb',
        databases: [] as any[],
      };

      // If a specific database is specified, only discover that one
      const databasesToDiscover =
        databaseName && databaseName !== 'admin'
          ? [{ name: databaseName }]
          : dbList.databases.filter((db: any) => !['admin', 'local', 'config'].includes(db.name));

      for (const dbInfo of databasesToDiscover) {
        const db = client.db(dbInfo.name);

        // Get collections
        const collections = await db.listCollections().toArray();
        this.logger.log(`Database '${dbInfo.name}': Found ${collections.length} collections`);

        const collectionsData: any[] = [];

        for (const coll of collections) {
          const collection = db.collection(coll.name);

          // Get sample documents to infer schema
          const sampleDocs = await collection.find({}).limit(10).toArray();
          const documentCount = await collection.countDocuments();

          // Infer fields from sample documents
          const fieldsMap = new Map<string, { types: Set<string>; nullable: boolean }>();

          for (const doc of sampleDocs) {
            this.inferFieldsFromDocument(doc, '', fieldsMap);
          }

          // Convert to array
          const fields = Array.from(fieldsMap.entries()).map(([name, info]) => ({
            name,
            type: Array.from(info.types).join(' | '),
            nullable: info.nullable,
          }));

          collectionsData.push({
            name: coll.name,
            type: 'collection',
            documentCount,
            sampleSize: sampleDocs.length,
            fields,
          });
        }

        result.databases.push({
          name: dbInfo.name,
          collections: collectionsData,
        });
      }

      return result;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.logger.error(`Failed to discover MongoDB schema: ${errorMessage}`);
      throw new BadRequestException(`Failed to discover schema: ${errorMessage}`);
    } finally {
      if (client) {
        await client.close();
      }
    }
  }

  /**
   * Helper to infer field structure from a MongoDB document
   */
  private inferFieldsFromDocument(
    doc: any,
    prefix: string,
    fieldsMap: Map<string, { types: Set<string>; nullable: boolean }>,
  ): void {
    for (const [key, value] of Object.entries(doc)) {
      const fieldName = prefix ? `${prefix}.${key}` : key;

      if (!fieldsMap.has(fieldName)) {
        fieldsMap.set(fieldName, { types: new Set(), nullable: false });
      }

      const fieldInfo = fieldsMap.get(fieldName)!;

      if (value === null || value === undefined) {
        fieldInfo.nullable = true;
        fieldInfo.types.add('null');
      } else if (Array.isArray(value)) {
        fieldInfo.types.add('array');
        // Sample first element for array type inference
        if (value.length > 0 && typeof value[0] === 'object' && value[0] !== null) {
          // Don't recurse into arrays of objects to avoid explosion
          fieldInfo.types.add(`array<object>`);
        } else if (value.length > 0) {
          fieldInfo.types.add(`array<${typeof value[0]}>`);
        }
      } else if (value instanceof Date) {
        fieldInfo.types.add('date');
      } else if (typeof value === 'object') {
        fieldInfo.types.add('object');
        // Recurse into nested objects (limit depth)
        if (prefix.split('.').length < 3) {
          this.inferFieldsFromDocument(value, fieldName, fieldsMap);
        }
      } else {
        fieldInfo.types.add(typeof value);
      }
    }
  }
}
