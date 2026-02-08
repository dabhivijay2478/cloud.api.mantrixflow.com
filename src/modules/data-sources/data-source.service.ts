/**
 * Data Source Service
 * Business logic for data source management
 * Supports multiple data source types (PostgreSQL, MySQL, MongoDB, S3, APIs, etc.)
 */

import {
  BadRequestException,
  ForbiddenException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import type { DataSource } from '../../database/schemas/data-sources';
import { ActivityLogService } from '../activity-logs/activity-log.service';
import { DATASOURCE_ACTIONS, ENTITY_TYPES } from '../activity-logs/constants/activity-log-types';
import { OrganizationRoleService } from '../organizations/services/organization-role.service';
import { DataSourceRepository } from './repositories/data-source.repository';

export interface CreateDataSourceDto {
  name: string;
  description?: string;
  sourceType: string;
  metadata?: Record<string, unknown>;
}

export interface UpdateDataSourceDto {
  name?: string;
  description?: string;
  isActive?: boolean;
  metadata?: Record<string, unknown>;
}

@Injectable()
export class DataSourceService {
  private readonly logger = new Logger(DataSourceService.name);

  // Supported data source types
  private readonly supportedTypes = [
    'postgres',
    'mysql',
    'mongodb',
    's3',
    'api',
    'bigquery',
    'snowflake',
    'csv',
  ];

  constructor(
    private readonly dataSourceRepository: DataSourceRepository,
    private readonly activityLogService: ActivityLogService,
    private readonly roleService: OrganizationRoleService,
  ) {}

  /**
   * Get supported data source types
   */
  getSupportedTypes(): string[] {
    return [...this.supportedTypes];
  }

  /**
   * Validate data source type
   */
  private validateSourceType(sourceType: string): void {
    if (!this.supportedTypes.includes(sourceType)) {
      throw new BadRequestException(
        `Unsupported source type: ${sourceType}. Supported types: ${this.supportedTypes.join(', ')}`,
      );
    }
  }

  /**
   * Create a new data source
   * AUTHORIZATION: Only ADMIN or OWNER can create data sources
   */
  async createDataSource(
    organizationId: string,
    userId: string,
    dto: CreateDataSourceDto,
  ): Promise<DataSource> {
    // Validate source type
    this.validateSourceType(dto.sourceType);

    // AUTHORIZATION: Check if user can manage data sources
    const canManage = await this.roleService.canManageDataSources(userId, organizationId);
    if (!canManage) {
      throw new ForbiddenException('Only OWNER, ADMIN, and EDITOR can create data sources');
    }

    // Check for duplicate name in organization
    const existing = await this.dataSourceRepository.findByName(organizationId, dto.name);
    if (existing && !existing.deletedAt) {
      throw new BadRequestException(
        `Data source with name "${dto.name}" already exists in this organization`,
      );
    }

    // Create data source
    const dataSource = await this.dataSourceRepository.create({
      organizationId,
      name: dto.name,
      description: dto.description,
      sourceType: dto.sourceType,
      isActive: true,
      metadata: dto.metadata || null,
      createdBy: userId,
    });

    // Log activity
    try {
      await this.activityLogService.logDataSourceAction(
        organizationId,
        userId,
        DATASOURCE_ACTIONS.CREATED,
        dataSource.id,
        dataSource.name,
        {
          sourceType: dto.sourceType,
        },
      );
    } catch (error) {
      this.logger.error(
        'Failed to log data source creation',
        error instanceof Error ? error.stack : String(error),
      );
    }

    return dataSource;
  }

  /**
   * List all data sources for an organization
   */
  async listDataSources(
    organizationId: string,
    userId: string,
    filters?: {
      sourceType?: string;
      isActive?: boolean;
    },
  ): Promise<DataSource[]> {
    // AUTHORIZATION: Check if user can view organization
    const canView = await this.roleService.canViewOrganization(userId, organizationId);
    if (!canView) {
      throw new ForbiddenException('You are not a member of this organization');
    }

    const dataSources = await this.dataSourceRepository.findByOrganization(organizationId, filters);

    // Log activity
    try {
      await this.activityLogService.logActivity({
        organizationId,
        userId,
        actionType: DATASOURCE_ACTIONS.VIEWED,
        entityType: ENTITY_TYPES.DATASOURCE,
        entityId: null,
        message: 'Data sources listed',
        metadata: {
          count: dataSources.length,
          filters,
        },
      });
    } catch (error) {
      this.logger.error(
        'Failed to log data sources list activity',
        error instanceof Error ? error.stack : String(error),
      );
    }

    return dataSources;
  }

  /**
   * List data sources with pagination
   */
  async listDataSourcesPaginated(
    organizationId: string,
    userId: string,
    filters?: { sourceType?: string; isActive?: boolean },
    limit: number = 20,
    offset: number = 0,
  ) {
    const canView = await this.roleService.canViewOrganization(userId, organizationId);
    if (!canView) {
      throw new ForbiddenException('You are not a member of this organization');
    }

    return this.dataSourceRepository.findByOrganizationPaginated(organizationId, limit, offset, filters);
  }

  /**
   * Get data source by ID
   */
  async getDataSourceById(
    organizationId: string,
    dataSourceId: string,
    userId: string,
  ): Promise<DataSource> {
    // AUTHORIZATION: Check if user can view organization
    const canView = await this.roleService.canViewOrganization(userId, organizationId);
    if (!canView) {
      throw new ForbiddenException('You are not a member of this organization');
    }

    const dataSource = await this.dataSourceRepository.findById(dataSourceId);
    if (!dataSource) {
      throw new NotFoundException(`Data source with ID "${dataSourceId}" not found`);
    }

    // Verify data source belongs to organization
    if (dataSource.organizationId !== organizationId) {
      throw new ForbiddenException('Data source does not belong to this organization');
    }

    // Log activity
    try {
      await this.activityLogService.logDataSourceAction(
        organizationId,
        userId,
        DATASOURCE_ACTIONS.VIEWED,
        dataSourceId,
        dataSource.name,
      );
    } catch (error) {
      this.logger.error(
        'Failed to log data source view activity',
        error instanceof Error ? error.stack : String(error),
      );
    }

    return dataSource;
  }

  /**
   * Update data source
   * AUTHORIZATION: Only ADMIN or OWNER can update data sources
   */
  async updateDataSource(
    organizationId: string,
    dataSourceId: string,
    userId: string,
    dto: UpdateDataSourceDto,
  ): Promise<DataSource> {
    const dataSource = await this.getDataSourceById(organizationId, dataSourceId, userId);

    // AUTHORIZATION: Check if user can manage data sources
    const canManage = await this.roleService.canManageDataSources(userId, organizationId);
    if (!canManage) {
      throw new ForbiddenException('Only OWNER, ADMIN, and EDITOR can update data sources');
    }

    // Check for duplicate name if name is being updated
    if (dto.name && dto.name !== dataSource.name) {
      const existing = await this.dataSourceRepository.findByName(organizationId, dto.name);
      if (existing && existing.id !== dataSourceId && !existing.deletedAt) {
        throw new BadRequestException(
          `Data source with name "${dto.name}" already exists in this organization`,
        );
      }
    }

    // Track changes for activity log
    const changes: Record<string, any> = {};
    if (dto.name && dto.name !== dataSource.name) {
      changes.name = { from: dataSource.name, to: dto.name };
    }
    if (dto.isActive !== undefined && dto.isActive !== dataSource.isActive) {
      changes.isActive = { from: dataSource.isActive, to: dto.isActive };
    }

    // Update data source
    const updated = await this.dataSourceRepository.update(dataSourceId, dto);

    // Log activity
    try {
      await this.activityLogService.logDataSourceAction(
        organizationId,
        userId,
        DATASOURCE_ACTIONS.UPDATED,
        dataSourceId,
        updated.name,
        {
          changes,
        },
      );
    } catch (error) {
      this.logger.error(
        'Failed to log data source update activity',
        error instanceof Error ? error.stack : String(error),
      );
    }

    return updated;
  }

  /**
   * Delete data source (soft delete)
   * AUTHORIZATION: Only ADMIN or OWNER can delete data sources
   */
  async deleteDataSource(
    organizationId: string,
    dataSourceId: string,
    userId: string,
  ): Promise<void> {
    const dataSource = await this.getDataSourceById(organizationId, dataSourceId, userId);

    // AUTHORIZATION: Check if user can manage data sources
    const canManage = await this.roleService.canManageDataSources(userId, organizationId);
    if (!canManage) {
      throw new ForbiddenException('Only OWNER, ADMIN, and EDITOR can delete data sources');
    }

    // TODO: Check if data source is used by any active pipelines
    // If used, prevent deletion or require confirmation

    // Soft delete
    await this.dataSourceRepository.softDelete(dataSourceId);

    // Log activity
    try {
      await this.activityLogService.logDataSourceAction(
        organizationId,
        userId,
        DATASOURCE_ACTIONS.DELETED,
        dataSourceId,
        dataSource.name,
      );
    } catch (error) {
      this.logger.error(
        'Failed to log data source deletion activity',
        error instanceof Error ? error.stack : String(error),
      );
    }
  }

  /**
   * Validate data source configuration based on type
   */
  validateDataSourceConfig(sourceType: string, config: Record<string, any>): void {
    this.validateSourceType(sourceType);

    // Type-specific validation
    switch (sourceType) {
      case 'postgres':
      case 'mysql':
        if (
          !config.host ||
          !config.port ||
          !config.database ||
          !config.username ||
          !config.password
        ) {
          throw new BadRequestException(
            'PostgreSQL/MySQL config requires: host, port, database, username, password',
          );
        }
        break;
      case 'mongodb':
        if (!config.connection_string && (!config.host || !config.database)) {
          throw new BadRequestException(
            'MongoDB config requires: connection_string OR (host and database)',
          );
        }
        break;
      case 's3':
        if (
          !config.bucket ||
          !config.region ||
          !config.access_key_id ||
          !config.secret_access_key
        ) {
          throw new BadRequestException(
            'S3 config requires: bucket, region, access_key_id, secret_access_key',
          );
        }
        break;
      case 'api':
        if (!config.base_url || !config.auth_type) {
          throw new BadRequestException('API config requires: base_url, auth_type');
        }
        break;
      default:
        // For other types, basic validation
        if (!config || typeof config !== 'object') {
          throw new BadRequestException('Config must be a valid object');
        }
    }
  }
}
