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

  constructor(
    private readonly connectionRepository: DataSourceConnectionRepository,
    private readonly dataSourceRepository: DataSourceRepository,
    private readonly encryptionService: EncryptionService,
    private readonly activityLogService: ActivityLogService,
    private readonly roleService: OrganizationRoleService,
  ) {}

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
   * Test connection
   * AUTHORIZATION: Only EDITOR+ can test connections
   */
  async testConnection(
    organizationId: string,
    dataSourceId: string,
    userId: string,
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

      // Test connection based on type
      switch (connection.connectionType) {
        case 'postgres':
          testResult = await this.testPostgresConnection(decryptedConfig as PostgresConfig);
          break;
        case 'mysql':
          testResult = await this.testMySQLConnection(decryptedConfig as MySQLConfig);
          break;
        case 'mongodb':
          testResult = await this.testMongoDBConnection(decryptedConfig as MongoDBConfig);
          break;
        case 's3':
          testResult = await this.testS3Connection(decryptedConfig as S3Config);
          break;
        case 'api':
          testResult = await this.testAPIConnection(decryptedConfig as APIConfig);
          break;
        default:
          throw new BadRequestException(
            `Connection testing not yet implemented for type: ${connection.connectionType}`,
          );
      }

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
  private async testPostgresConnection(_config: PostgresConfig): Promise<{
    success: boolean;
    message: string;
    details?: any;
  }> {
    // TODO: Implement actual PostgreSQL connection test
    // For now, return success if config is valid
    return {
      success: true,
      message: 'PostgreSQL connection test not yet implemented',
    };
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
   */
  private async testMongoDBConnection(_config: MongoDBConfig): Promise<{
    success: boolean;
    message: string;
    details?: any;
  }> {
    // TODO: Implement actual MongoDB connection test
    return {
      success: true,
      message: 'MongoDB connection test not yet implemented',
    };
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

    // TODO: Implement schema discovery based on connection type
    // For now, return empty schema
    const schema = {};

    // Update schema cache
    await this.connectionRepository.updateByDataSourceId(dataSourceId, {
      schemaCache: schema as any,
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
}
