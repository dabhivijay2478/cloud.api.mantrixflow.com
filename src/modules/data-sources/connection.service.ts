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
  OnModuleInit,
} from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { Pool } from 'pg';
import { normalizeEtlBaseUrl } from '../../common/utils/etl-url';
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
export class ConnectionService implements OnModuleInit {
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
    this.pythonServiceUrl = normalizeEtlBaseUrl(
      this.configService.get<string>('ETL_PYTHON_SERVICE_URL') ??
        this.configService.get<string>('PYTHON_SERVICE_URL'),
    );
    if (!this.pythonServiceUrl) {
      throw new Error(
        'ETL_PYTHON_SERVICE_URL or PYTHON_SERVICE_URL must be set in environment (e.g. in apps/api/.env)',
      );
    }
  }

  onModuleInit(): void {
    // Best-effort background migration for legacy plaintext credentials.
    void this.migrateLegacyConnectionConfigs();
  }

  /**
   * Encrypt sensitive fields in config based on connection type
   */
  private encryptConfig(connectionType: string, config: Record<string, any>): Record<string, any> {
    const encrypted = { ...config };

    switch (connectionType) {
      case 'postgres':
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
      default:
        // Airbyte API-based connectors (github, stripe, hubspot, etc.)
        this.encryptApiConnectorFields(encrypted);
        break;
    }

    return encrypted;
  }

  /** Decrypt common sensitive fields for API-based Airbyte connectors */
  private decryptApiConnectorFields(config: Record<string, any>, connectionType: string): void {
    if (config.api_key) {
      config.api_key = this.decryptFieldWithLegacySupport(
        config.api_key,
        connectionType,
        'api_key',
      );
    }
    if (config.credentials_json) {
      config.credentials_json = this.decryptFieldWithLegacySupport(
        config.credentials_json,
        connectionType,
        'credentials_json',
      );
    }
    if (config.credentials?.personal_access_token) {
      config.credentials.personal_access_token = this.decryptFieldWithLegacySupport(
        config.credentials.personal_access_token,
        connectionType,
        'credentials.personal_access_token',
      );
    }
    if (config.credentials?.access_token) {
      config.credentials.access_token = this.decryptFieldWithLegacySupport(
        config.credentials.access_token,
        connectionType,
        'credentials.access_token',
      );
    }
    if (config.credentials?.api_token) {
      config.credentials.api_token = this.decryptFieldWithLegacySupport(
        config.credentials.api_token,
        connectionType,
        'credentials.api_token',
      );
    }
    if (config.password) {
      config.password = this.decryptFieldWithLegacySupport(
        config.password,
        connectionType,
        'password',
      );
    }
    if (config.client_secret) {
      config.client_secret = this.decryptFieldWithLegacySupport(
        config.client_secret,
        connectionType,
        'client_secret',
      );
    }
  }

  /** Encrypt common sensitive fields for API-based Airbyte connectors */
  private encryptApiConnectorFields(config: Record<string, any>): void {
    if (config.api_key) {
      config.api_key = this.encryptionService.encrypt(config.api_key);
    }
    if (config.credentials_json) {
      config.credentials_json = this.encryptionService.encrypt(config.credentials_json);
    }
    if (config.credentials?.personal_access_token) {
      config.credentials.personal_access_token = this.encryptionService.encrypt(
        config.credentials.personal_access_token,
      );
    }
    if (config.credentials?.access_token) {
      config.credentials.access_token = this.encryptionService.encrypt(
        config.credentials.access_token,
      );
    }
    if (config.credentials?.api_token) {
      config.credentials.api_token = this.encryptionService.encrypt(config.credentials.api_token);
    }
    if (config.password) {
      config.password = this.encryptionService.encrypt(config.password);
    }
    if (config.client_secret) {
      config.client_secret = this.encryptionService.encrypt(config.client_secret);
    }
  }

  private maybeDecryptIncomingEncrypted(value: unknown): unknown {
    if (typeof value !== 'string') {
      return value;
    }

    if (!this.isEncryptedFormat(value)) {
      return value;
    }

    try {
      return this.encryptionService.decrypt(value);
    } catch (error) {
      this.logger.warn(
        `Received encrypted-looking value that could not be decrypted: ${error instanceof Error ? error.message : String(error)}`,
      );
      return value;
    }
  }

  /**
   * Normalize incoming payload so create/update remains idempotent even if
   * clients accidentally post already-encrypted fields.
   */
  private normalizeIncomingConfig(
    connectionType: string,
    config: Record<string, any>,
  ): Record<string, any> {
    const normalized = { ...config };

    switch (connectionType) {
      case 'postgres':
        if (normalized.password) {
          normalized.password = this.maybeDecryptIncomingEncrypted(normalized.password);
        }
        if (normalized.ssl?.ca_cert) {
          normalized.ssl.ca_cert = this.maybeDecryptIncomingEncrypted(normalized.ssl.ca_cert);
        }
        if (normalized.ssl?.client_cert) {
          normalized.ssl.client_cert = this.maybeDecryptIncomingEncrypted(
            normalized.ssl.client_cert,
          );
        }
        if (normalized.ssl?.client_key) {
          normalized.ssl.client_key = this.maybeDecryptIncomingEncrypted(normalized.ssl.client_key);
        }
        if (normalized.ssh_tunnel?.private_key) {
          normalized.ssh_tunnel.private_key = this.maybeDecryptIncomingEncrypted(
            normalized.ssh_tunnel.private_key,
          );
        }
        break;
      case 's3':
        if (normalized.access_key_id) {
          normalized.access_key_id = this.maybeDecryptIncomingEncrypted(normalized.access_key_id);
        }
        if (normalized.secret_access_key) {
          normalized.secret_access_key = this.maybeDecryptIncomingEncrypted(
            normalized.secret_access_key,
          );
        }
        break;
      case 'api':
        if (normalized.auth_token) {
          normalized.auth_token = this.maybeDecryptIncomingEncrypted(normalized.auth_token);
        }
        if (normalized.api_key) {
          normalized.api_key = this.maybeDecryptIncomingEncrypted(normalized.api_key);
        }
        break;
      case 'bigquery':
        if (normalized.credentials?.private_key) {
          normalized.credentials.private_key = this.maybeDecryptIncomingEncrypted(
            normalized.credentials.private_key,
          );
        }
        break;
      case 'snowflake':
        if (normalized.password) {
          normalized.password = this.maybeDecryptIncomingEncrypted(normalized.password);
        }
        break;
      default:
        this.normalizeApiConnectorFields(normalized);
        break;
    }

    return normalized;
  }

  /** Normalize incoming config for API-based Airbyte connectors */
  private normalizeApiConnectorFields(config: Record<string, any>): void {
    if (config.api_key) {
      config.api_key = this.maybeDecryptIncomingEncrypted(config.api_key);
    }
    if (config.credentials_json) {
      config.credentials_json = this.maybeDecryptIncomingEncrypted(config.credentials_json);
    }
    if (config.credentials?.personal_access_token) {
      config.credentials.personal_access_token = this.maybeDecryptIncomingEncrypted(
        config.credentials.personal_access_token,
      );
    }
    if (config.credentials?.access_token) {
      config.credentials.access_token = this.maybeDecryptIncomingEncrypted(
        config.credentials.access_token,
      );
    }
    if (config.credentials?.api_token) {
      config.credentials.api_token = this.maybeDecryptIncomingEncrypted(
        config.credentials.api_token,
      );
    }
    if (config.password) {
      config.password = this.maybeDecryptIncomingEncrypted(config.password);
    }
    if (config.client_secret) {
      config.client_secret = this.maybeDecryptIncomingEncrypted(config.client_secret);
    }
  }

  /**
   * Decrypt sensitive fields in config based on connection type
   */
  private decryptFieldWithLegacySupport(
    value: unknown,
    connectionType: string,
    fieldPath: string,
  ): unknown {
    if (typeof value !== 'string' || value.length === 0) {
      return value;
    }

    try {
      return this.encryptionService.decrypt(value);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);

      // Backward compatibility: older/alternate writers stored plaintext values.
      if (errorMessage.includes('Invalid encryption format')) {
        this.logger.warn(
          `Using plaintext fallback for ${connectionType}.${fieldPath}. Consider re-saving this connection to re-encrypt credentials.`,
        );
        return value;
      }

      throw error;
    }
  }

  private isEncryptedFormat(value: unknown): boolean {
    if (typeof value !== 'string' || value.length === 0) {
      return false;
    }

    const parts = value.split(':');
    if (parts.length !== 4) {
      return false;
    }

    try {
      const [saltB64, ivB64, tagB64, ciphertextB64] = parts;
      const salt = Buffer.from(saltB64, 'base64');
      const iv = Buffer.from(ivB64, 'base64');
      const tag = Buffer.from(tagB64, 'base64');
      const ciphertext = Buffer.from(ciphertextB64, 'base64');

      if (salt.length !== 64) return false;
      if (iv.length !== 16) return false;
      if (tag.length !== 16) return false;
      if (ciphertext.length === 0) return false;

      return true;
    } catch {
      return false;
    }
  }

  private hasLegacySensitiveValues(connectionType: string, config: Record<string, any>): boolean {
    const checks: unknown[] = [];
    switch (connectionType) {
      case 'postgres':
        checks.push(
          config.password,
          config.ssl?.ca_cert,
          config.ssl?.client_cert,
          config.ssl?.client_key,
          config.ssh_tunnel?.private_key,
        );
        break;
      case 's3':
        checks.push(config.access_key_id, config.secret_access_key);
        break;
      case 'api':
        checks.push(config.auth_token, config.api_key);
        break;
      case 'bigquery':
        checks.push(config.credentials?.private_key);
        break;
      case 'snowflake':
        checks.push(config.password);
        break;
      default:
        checks.push(
          config.api_key,
          config.credentials?.personal_access_token,
          config.credentials?.access_token,
          config.credentials?.api_token,
          config.password,
          config.client_secret,
        );
        break;
    }

    return checks.some((value) => {
      if (typeof value !== 'string' || value.length === 0) {
        return false;
      }
      return !this.isEncryptedFormat(value);
    });
  }

  private async migrateLegacyConnectionConfigs(): Promise<void> {
    try {
      const allConnections = await this.connectionRepository.findAll();
      if (allConnections.length === 0) {
        return;
      }

      let migratedCount = 0;

      for (const connection of allConnections) {
        const rawConfig = connection.config as Record<string, any> | null;
        if (!rawConfig || typeof rawConfig !== 'object') {
          continue;
        }

        if (!this.hasLegacySensitiveValues(connection.connectionType, rawConfig)) {
          continue;
        }

        try {
          const decryptedConfig = this.decryptConfig(connection.connectionType, rawConfig);
          const encryptedConfig = this.encryptConfig(connection.connectionType, decryptedConfig);
          await this.connectionRepository.update(connection.id, {
            config: encryptedConfig,
          });
          migratedCount += 1;
        } catch (error) {
          this.logger.warn(
            `Skipping legacy credential migration for connection ${connection.id}: ${error instanceof Error ? error.message : String(error)}`,
          );
        }
      }

      if (migratedCount > 0) {
        this.logger.log(
          `Migrated ${migratedCount} connection(s) with legacy plaintext credentials`,
        );
      }
    } catch (error) {
      this.logger.warn(
        `Failed to run legacy credential migration on startup: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }

  private decryptConfig(connectionType: string, config: Record<string, any>): Record<string, any> {
    const decrypted = { ...config };

    try {
      switch (connectionType) {
        case 'postgres':
          if (decrypted.password) {
            decrypted.password = this.decryptFieldWithLegacySupport(
              decrypted.password,
              connectionType,
              'password',
            );
          }
          if (decrypted.ssl?.ca_cert) {
            decrypted.ssl.ca_cert = this.decryptFieldWithLegacySupport(
              decrypted.ssl.ca_cert,
              connectionType,
              'ssl.ca_cert',
            );
          }
          if (decrypted.ssl?.client_cert) {
            decrypted.ssl.client_cert = this.decryptFieldWithLegacySupport(
              decrypted.ssl.client_cert,
              connectionType,
              'ssl.client_cert',
            );
          }
          if (decrypted.ssl?.client_key) {
            decrypted.ssl.client_key = this.decryptFieldWithLegacySupport(
              decrypted.ssl.client_key,
              connectionType,
              'ssl.client_key',
            );
          }
          if (decrypted.ssh_tunnel?.private_key) {
            decrypted.ssh_tunnel.private_key = this.decryptFieldWithLegacySupport(
              decrypted.ssh_tunnel.private_key,
              connectionType,
              'ssh_tunnel.private_key',
            );
          }
          break;
        case 's3':
          if (decrypted.access_key_id) {
            decrypted.access_key_id = this.decryptFieldWithLegacySupport(
              decrypted.access_key_id,
              connectionType,
              'access_key_id',
            );
          }
          if (decrypted.secret_access_key) {
            decrypted.secret_access_key = this.decryptFieldWithLegacySupport(
              decrypted.secret_access_key,
              connectionType,
              'secret_access_key',
            );
          }
          break;
        case 'api':
          if (decrypted.auth_token) {
            decrypted.auth_token = this.decryptFieldWithLegacySupport(
              decrypted.auth_token,
              connectionType,
              'auth_token',
            );
          }
          if (decrypted.api_key) {
            decrypted.api_key = this.decryptFieldWithLegacySupport(
              decrypted.api_key,
              connectionType,
              'api_key',
            );
          }
          break;
        case 'bigquery':
          if (decrypted.credentials?.private_key) {
            decrypted.credentials.private_key = this.decryptFieldWithLegacySupport(
              decrypted.credentials.private_key,
              connectionType,
              'credentials.private_key',
            );
          }
          break;
        case 'snowflake':
          if (decrypted.password) {
            decrypted.password = this.decryptFieldWithLegacySupport(
              decrypted.password,
              connectionType,
              'password',
            );
          }
          break;
        default:
          this.decryptApiConnectorFields(decrypted, connectionType);
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
      default:
        this.maskApiConnectorFields(masked);
        break;
    }

    return masked;
  }

  /** Mask sensitive fields for API-based Airbyte connectors */
  private maskApiConnectorFields(config: Record<string, any>): void {
    if (config.api_key) config.api_key = '****';
    if (config.credentials?.personal_access_token)
      config.credentials.personal_access_token = '****';
    if (config.credentials?.access_token) config.credentials.access_token = '****';
    if (config.credentials?.api_token) config.credentials.api_token = '****';
    if (config.password) config.password = '****';
    if (config.client_secret) config.client_secret = '****';
  }

  /**
   * Validate connection config structure based on type.
   * Airbyte connectors use generic validation when no specific validator exists.
   */
  validateConnectionConfig(connectionType: string, config: Record<string, any>): void {
    switch (connectionType) {
      case 'postgres':
        this.validatePostgresConfig(config as PostgresConfig);
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
        // Allow all Airbyte connectors with minimal validation; ETL validates on test/discover
        if (!config || typeof config !== 'object') {
          throw new BadRequestException('config must be a non-empty object');
        }
        break;
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

    // Verify connection type matches data source type (normalize aliases like "postgres" → "postgresql")
    if (
      this.normalizeSourceTypeForPython(dto.connectionType) !==
      this.normalizeSourceTypeForPython(dataSource.sourceType)
    ) {
      throw new BadRequestException(
        `Connection type "${dto.connectionType}" does not match data source type "${dataSource.sourceType}"`,
      );
    }

    // AUTHORIZATION: Check if user can manage data sources
    const canManage = await this.roleService.canManageDataSources(userId, organizationId);
    if (!canManage) {
      throw new ForbiddenException('Only OWNER, ADMIN, and EDITOR can configure connections');
    }

    // Normalize any encrypted-looking input to avoid double-encryption writes.
    const normalizedConfig = this.normalizeIncomingConfig(dto.connectionType, dto.config);

    // Validate config structure
    this.validateConnectionConfig(dto.connectionType, normalizedConfig);

    // Encrypt sensitive fields
    const encryptedConfig = this.encryptConfig(dto.connectionType, normalizedConfig);

    let existing: DataSourceConnection | null;
    try {
      existing = await this.connectionRepository.findByDataSourceId(dataSourceId);
    } catch (err: any) {
      const cause = err?.cause?.message ?? err?.message ?? String(err);
      this.logger.error(`Connection query failed: ${cause}`, err?.stack);
      throw new BadRequestException(
        `Failed to load connection: ${cause}. Ensure database migrations are applied (run: bun run db:migrate).`,
      );
    }

    try {
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
    } catch (err: any) {
      const cause = err?.cause?.message ?? err?.message ?? String(err);
      this.logger.error(`Connection create/update failed: ${cause}`, err?.stack);
      throw new BadRequestException(
        `Failed to save connection: ${cause}. Ensure database migrations are applied (run: bun run db:migrate).`,
      );
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

    // Return decrypted credentials for sensitive consumers (schema discovery, test connection, etc.).
    const rawConfig = connection.config as Record<string, any>;
    const hasLegacyPlaintext = this.hasLegacySensitiveValues(connection.connectionType, rawConfig);
    const decryptedConfig = this.decryptConfig(connection.connectionType, rawConfig);

    // Auto-migrate legacy plaintext to encrypted format.
    if (hasLegacyPlaintext) {
      try {
        const encryptedConfig = this.encryptConfig(connection.connectionType, decryptedConfig);
        await this.connectionRepository.updateByDataSourceId(dataSourceId, {
          config: encryptedConfig,
        });
        this.logger.log(
          `Migrated legacy plaintext credentials to encrypted format for data source ${dataSourceId}`,
        );
      } catch (error) {
        this.logger.warn(
          `Failed to auto-migrate legacy plaintext credentials for ${dataSourceId}: ${error}`,
        );
      }
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

    return {
      ...connection,
      config: decryptedConfig,
    } as DataSourceConnection;
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

    // Call ETL cleanup to drop replication slot before deleting (for postgres CDC)
    const slotName = connection.replicationSlotName;
    if (slotName && this.pythonServiceUrl) {
      try {
        const decryptedConfig = this.decryptConfig(
          connection.connectionType,
          connection.config as Record<string, any>,
        );
        const sourceType = this.normalizeSourceTypeForPython(connection.connectionType);
        await firstValueFrom(
          this.httpService.post(
            `${this.pythonServiceUrl}/cleanup/connection`,
            {
              source_type: sourceType,
              connection_config: decryptedConfig,
              replication_slot_name: slotName,
            },
            {
              headers: { 'Content-Type': 'application/json' },
              timeout: 15000,
            },
          ),
        );
      } catch (err) {
        this.logger.warn(
          `CDC cleanup failed (proceeding with delete): ${err instanceof Error ? err.message : String(err)}`,
        );
      }
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

    return (connection.config as Record<string, any>) || {};
  }

  /**
   * Get connection type for registry lookup (tap/target executable).
   * AUTHORIZATION: Same as getConnection.
   */
  async getConnectionType(
    organizationId: string,
    dataSourceId: string,
    userId: string,
  ): Promise<string> {
    const connection = await this.getConnection(organizationId, dataSourceId, userId, false);
    if (!connection) {
      throw new NotFoundException('Connection not found for this data source');
    }
    return connection.connectionType || 'postgres';
  }

  /**
   * Normalize source type for Python service
   */
  private normalizeSourceTypeForPython(connectionType: string): string {
    const normalized = connectionType.toLowerCase();
    if (normalized === 'postgres' || normalized === 'pgvector' || normalized === 'redshift') {
      return 'postgresql';
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

      // Call Python service to test connection (PostgreSQL only)
      const sourceType = this.normalizeSourceTypeForPython(connection.connectionType);
      const pythonRequest = {
        connection_config: decryptedConfig,
        source_config: decryptedConfig,
        source_type: sourceType,
      };

      this.logger.log(`Calling Python service to test ${sourceType} connection`);

      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
      };

      if (authToken) {
        headers.Authorization = `Bearer ${authToken}`;
      }

      let pythonResponse: any;
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
      case 'postgres':
      case 'pgvector':
      case 'redshift':
        return this.testPostgresViaEtl(config as PostgresConfig);

      case 's3':
      case 's3-datalake':
        return this.testS3Connection(config as S3Config);

      case 'api':
        return this.testAPIConnection(config as APIConfig);

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
          `Unsupported connection type: ${connectionType}. Supported types: postgres, pgvector, redshift, s3, api`,
        );
    }
  }

  /**
   * Test PostgreSQL connection via ETL (tap-postgres --test)
   * Routes to ETL server so logs appear; falls back to Node.js pg if ETL unreachable
   */
  private async testPostgresViaEtl(config: PostgresConfig): Promise<{
    success: boolean;
    message: string;
    details?: any;
  }> {
    if (!this.pythonServiceUrl) {
      return this.testPostgresConnection(config);
    }
    try {
      const sourceType = 'source-postgres';
      const pythonRequest = {
        connection_config: config,
        source_config: config,
        source_type: sourceType,
      };
      this.logger.log(`Calling ETL test-connection for %s`, config.host || '?');
      const res = await firstValueFrom(
        this.httpService.post(`${this.pythonServiceUrl}/test-connection`, pythonRequest, {
          headers: { 'Content-Type': 'application/json' },
          timeout: 30000,
        }),
      );
      const data = res.data as { success?: boolean; error?: string; message?: string };
      const success = data?.success === true;
      return {
        success,
        message: success ? 'Successfully connected to PostgreSQL database' : (data?.error || data?.message || 'Connection test failed'),
        details: data,
      };
    } catch (error: any) {
      const detail = error?.response?.data?.detail ?? error?.response?.data?.error ?? error?.message;
      this.logger.warn(`ETL test-connection failed, falling back to Node.js pg: ${detail}`);
      return this.testPostgresConnection(config);
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

}
