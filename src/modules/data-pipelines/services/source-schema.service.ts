/**
 * Source Schema Service
 * Business logic for pipeline source schema management
 * Works with all data source types for schema discovery
 */

import {
  BadRequestException,
  ForbiddenException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import type { PipelineSourceSchema } from '../../../database/schemas';
import { ActivityLogService } from '../../activity-logs/activity-log.service';
import { SOURCE_SCHEMA_ACTIONS } from '../../activity-logs/constants/activity-log-types';
import { DataSourceRepository } from '../../data-sources/repositories/data-source.repository';
import { OrganizationRoleService } from '../../organizations/services/organization-role.service';
import { CollectorService } from './collector.service';
import { PipelineSourceSchemaRepository } from '../repositories/pipeline-source-schema.repository';
import type { CreateSourceSchemaDto, UpdateSourceSchemaDto } from '../dto';
import type { ColumnInfo, ValidationResult } from '../types/common.types';

/**
 * Internal DTO with organization context
 */
export interface CreateSourceSchemaInput extends CreateSourceSchemaDto {
  organizationId: string;
}

@Injectable()
export class SourceSchemaService {
  private readonly logger = new Logger(SourceSchemaService.name);

  constructor(
    private readonly sourceSchemaRepository: PipelineSourceSchemaRepository,
    private readonly dataSourceRepository: DataSourceRepository,
    private readonly collectorService: CollectorService,
    private readonly activityLogService: ActivityLogService,
    private readonly roleService: OrganizationRoleService,
  ) {}

  /**
   * Create source schema
   */
  async create(
    dto: CreateSourceSchemaInput,
    userId: string,
  ): Promise<PipelineSourceSchema> {
    const { organizationId, dataSourceId, sourceType } = dto;

    // AUTHORIZATION
    await this.checkManagePermission(userId, organizationId);

    // Validate data source if provided
    if (dataSourceId) {
      const dataSource = await this.dataSourceRepository.findById(dataSourceId);
      if (!dataSource) {
        throw new NotFoundException(`Data source ${dataSourceId} not found`);
      }
      if (dataSource.organizationId !== organizationId) {
        throw new ForbiddenException('Data source does not belong to this organization');
      }
      // Validate source type matches data source type
      if (dataSource.sourceType !== sourceType) {
        throw new BadRequestException(
          `Source type '${sourceType}' does not match data source type '${dataSource.sourceType}'`,
        );
      }
    }

    // Validate that we have either a data source or source config
    if (!dataSourceId && (!dto.sourceConfig || Object.keys(dto.sourceConfig).length === 0)) {
      throw new BadRequestException('Either dataSourceId or sourceConfig is required');
    }

    // Validate that we have a source table or query
    if (!dto.sourceTable && !dto.sourceQuery) {
      throw new BadRequestException('Either sourceTable or sourceQuery is required');
    }

    const schema = await this.sourceSchemaRepository.create({
      organizationId,
      sourceType,
      dataSourceId: dataSourceId || null,
      sourceConfig: dto.sourceConfig || null,
      sourceSchema: dto.sourceSchema || null,
      sourceTable: dto.sourceTable || null,
      sourceQuery: dto.sourceQuery || null,
      name: dto.name || `${sourceType}_source_${Date.now()}`,
      isActive: true,
    });

    // Log activity
    await this.activityLogService.logActivity({
      organizationId,
      userId,
      actionType: SOURCE_SCHEMA_ACTIONS.CREATED,
      entityType: 'pipeline_source_schema',
      entityId: schema.id,
      message: `Source schema created: ${schema.name || schema.id}`,
      metadata: {
        sourceType,
        dataSourceId,
        sourceTable: dto.sourceTable,
      },
    });

    this.logger.log(`Source schema created: ${schema.id}`);
    return schema;
  }

  /**
   * Get source schema by ID
   */
  async findById(
    id: string,
    organizationId?: string,
    userId?: string,
  ): Promise<PipelineSourceSchema | null> {
    const schema = await this.sourceSchemaRepository.findById(id);

    if (schema && organizationId && schema.organizationId !== organizationId) {
      throw new ForbiddenException('Source schema does not belong to this organization');
    }

    if (schema && userId) {
      await this.checkViewPermission(userId, schema.organizationId);
    }

    return schema;
  }

  /**
   * Get source schemas by organization
   */
  async findByOrganization(
    organizationId: string,
    userId?: string,
  ): Promise<PipelineSourceSchema[]> {
    if (userId) {
      await this.checkViewPermission(userId, organizationId);
    }

    return await this.sourceSchemaRepository.findByOrganization(organizationId);
  }

  /**
   * Update source schema
   */
  async update(
    id: string,
    updates: UpdateSourceSchemaDto,
    userId: string,
  ): Promise<PipelineSourceSchema> {
    const schema = await this.sourceSchemaRepository.findById(id);
    if (!schema) {
      throw new NotFoundException(`Source schema ${id} not found`);
    }

    // AUTHORIZATION
    await this.checkManagePermission(userId, schema.organizationId);

    const updated = await this.sourceSchemaRepository.update(id, {
      ...updates,
      updatedAt: new Date(),
    });

    // Log activity
    await this.activityLogService.logActivity({
      organizationId: schema.organizationId,
      userId,
      actionType: SOURCE_SCHEMA_ACTIONS.UPDATED,
      entityType: 'pipeline_source_schema',
      entityId: id,
      message: `Source schema updated: ${schema.name || id}`,
      metadata: { changes: Object.keys(updates) },
    });

    return updated;
  }

  /**
   * Discover schema from source
   * Uses CollectorService to discover columns, primary keys, and row count
   */
  async discoverSchema(
    id: string,
    userId: string,
  ): Promise<{
    schema: PipelineSourceSchema;
    discovered: {
      columns: ColumnInfo[];
      primaryKeys: string[];
      estimatedRowCount?: number;
    };
  }> {
    const schema = await this.sourceSchemaRepository.findById(id);
    if (!schema) {
      throw new NotFoundException(`Source schema ${id} not found`);
    }

    // AUTHORIZATION
    await this.checkManagePermission(userId, schema.organizationId);

    if (!schema.dataSourceId) {
      throw new BadRequestException('Source schema must have a data source ID for discovery');
    }

    // Discover schema using collector
    const discovered = await this.collectorService.discoverSchema({
      sourceSchema: schema,
      organizationId: schema.organizationId,
      userId,
    });

    // Update schema with discovered information
    const updated = await this.sourceSchemaRepository.update(id, {
      discoveredColumns: discovered.columns as any,
      primaryKeys: discovered.primaryKeys as any,
      estimatedRowCount: discovered.estimatedRowCount as any,
      lastDiscoveredAt: new Date(),
      validationResult: {
        valid: true,
        errors: [],
        warnings: [],
        validatedAt: new Date().toISOString(),
      } as any,
    });

    // Log activity
    await this.activityLogService.logActivity({
      organizationId: schema.organizationId,
      userId,
      actionType: SOURCE_SCHEMA_ACTIONS.DISCOVERED,
      entityType: 'pipeline_source_schema',
      entityId: id,
      message: `Schema discovered: ${discovered.columns.length} columns, ${discovered.primaryKeys.length} primary keys`,
      metadata: {
        columnsCount: discovered.columns.length,
        primaryKeysCount: discovered.primaryKeys.length,
        estimatedRowCount: discovered.estimatedRowCount,
      },
    });

    this.logger.log(
      `Schema discovered for ${id}: ${discovered.columns.length} columns, ~${discovered.estimatedRowCount} rows`,
    );

    return { schema: updated, discovered };
  }

  /**
   * Validate source schema configuration
   */
  async validateSchema(id: string, userId: string): Promise<ValidationResult> {
    const schema = await this.sourceSchemaRepository.findById(id);
    if (!schema) {
      throw new NotFoundException(`Source schema ${id} not found`);
    }

    await this.checkViewPermission(userId, schema.organizationId);

    const errors: string[] = [];
    const warnings: string[] = [];

    // Check required fields
    if (!schema.sourceType) {
      errors.push('Source type is required');
    }

    if (!schema.dataSourceId && !schema.sourceConfig) {
      errors.push('Either dataSourceId or sourceConfig is required');
    }

    if (!schema.sourceTable && !schema.sourceQuery) {
      errors.push('Either sourceTable or sourceQuery is required');
    }

    // Check if data source exists
    if (schema.dataSourceId) {
      const dataSource = await this.dataSourceRepository.findById(schema.dataSourceId);
      if (!dataSource) {
        errors.push('Referenced data source does not exist');
      } else if (!dataSource.isActive) {
        warnings.push('Referenced data source is inactive');
      }
    }

    // Check discovered columns
    const discoveredColumns = schema.discoveredColumns as any[];
    if (!discoveredColumns || discoveredColumns.length === 0) {
      warnings.push('Schema has not been discovered - run schema discovery');
    }

    // Update validation result
    const validationResult = {
      valid: errors.length === 0,
      errors,
      warnings,
      validatedAt: new Date().toISOString(),
    };

    await this.sourceSchemaRepository.update(id, {
      validationResult: validationResult as any,
    });

    // Log activity
    await this.activityLogService.logActivity({
      organizationId: schema.organizationId,
      userId,
      actionType: SOURCE_SCHEMA_ACTIONS.VALIDATED,
      entityType: 'pipeline_source_schema',
      entityId: id,
      message: validationResult.valid ? 'Schema validation passed' : 'Schema validation failed',
      metadata: { errorsCount: errors.length, warningsCount: warnings.length },
    });

    return validationResult;
  }

  /**
   * Preview source data (sample)
   */
  async previewData(
    id: string,
    userId: string,
    limit: number = 10,
  ): Promise<{ rows: any[]; columns: ColumnInfo[] }> {
    const schema = await this.sourceSchemaRepository.findById(id);
    if (!schema) {
      throw new NotFoundException(`Source schema ${id} not found`);
    }

    await this.checkViewPermission(userId, schema.organizationId);

    if (!schema.dataSourceId) {
      throw new BadRequestException('Source schema must have a data source ID for preview');
    }

    const result = await this.collectorService.collect({
      sourceSchema: schema,
      organizationId: schema.organizationId,
      userId,
      limit: Math.min(limit, 100), // Cap at 100 rows for preview
    });

    // Get or infer columns
    const columns = (schema.discoveredColumns as ColumnInfo[]) || [];

    return {
      rows: result.rows,
      columns,
    };
  }

  /**
   * Delete source schema (soft delete)
   */
  async delete(id: string, userId: string): Promise<void> {
    const schema = await this.sourceSchemaRepository.findById(id);
    if (!schema) {
      throw new NotFoundException(`Source schema ${id} not found`);
    }

    // AUTHORIZATION
    await this.checkManagePermission(userId, schema.organizationId);

    // TODO: Check if schema is used by any active pipelines
    // If used, prevent deletion or require confirmation

    await this.sourceSchemaRepository.delete(id);

    // Log activity
    await this.activityLogService.logActivity({
      organizationId: schema.organizationId,
      userId,
      actionType: SOURCE_SCHEMA_ACTIONS.DELETED,
      entityType: 'pipeline_source_schema',
      entityId: id,
      message: `Source schema deleted: ${schema.name || id}`,
    });

    this.logger.log(`Source schema deleted: ${id}`);
  }

  // ============================================================================
  // AUTHORIZATION HELPERS
  // ============================================================================

  private async checkViewPermission(userId: string, organizationId: string): Promise<void> {
    const canView = await this.roleService.canViewOrganization(userId, organizationId);
    if (!canView) {
      throw new ForbiddenException('You are not a member of this organization');
    }
  }

  private async checkManagePermission(userId: string, organizationId: string): Promise<void> {
    const canManage = await this.roleService.canManageDataSources(userId, organizationId);
    if (!canManage) {
      throw new ForbiddenException('Only OWNER, ADMIN, and EDITOR can manage source schemas');
    }
  }
}
