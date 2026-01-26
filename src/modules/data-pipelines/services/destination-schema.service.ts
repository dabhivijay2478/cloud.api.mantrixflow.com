/**
 * Destination Schema Service
 * Business logic for pipeline destination schema management
 * Supports schema validation, table creation, and column mapping management
 */

import {
  BadRequestException,
  ForbiddenException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import type { PipelineDestinationSchema } from '../../../database/schemas';
import { ActivityLogService } from '../../activity-logs/activity-log.service';
import { DESTINATION_SCHEMA_ACTIONS } from '../../activity-logs/constants/activity-log-types';
import { DataSourceRepository } from '../../data-sources/repositories/data-source.repository';
import { OrganizationRoleService } from '../../organizations/services/organization-role.service';
import { PythonETLService } from './python-etl.service';
import { ConnectionService } from '../../data-sources/connection.service';
import { PipelineDestinationSchemaRepository } from '../repositories/pipeline-destination-schema.repository';
import type { CreateDestinationSchemaDto, UpdateDestinationSchemaDto } from '../dto';
import { WriteMode } from '../dto/create-destination-schema.dto';
import type { SchemaValidationResult, ValidationResult } from '../types/common.types';

/**
 * Internal DTO with organization context
 */
export interface CreateDestinationSchemaInput extends CreateDestinationSchemaDto {
  organizationId: string;
}

@Injectable()
export class DestinationSchemaService {
  private readonly logger = new Logger(DestinationSchemaService.name);

  constructor(
    private readonly destinationSchemaRepository: PipelineDestinationSchemaRepository,
    private readonly dataSourceRepository: DataSourceRepository,
    readonly _pythonETLService: PythonETLService,
    readonly _connectionService: ConnectionService,
    private readonly activityLogService: ActivityLogService,
    private readonly roleService: OrganizationRoleService,
  ) {}

  /**
   * Create destination schema
   */
  async create(
    dto: CreateDestinationSchemaInput,
    userId: string,
  ): Promise<PipelineDestinationSchema> {
    const { organizationId, dataSourceId, destinationTable } = dto;

    // AUTHORIZATION
    await this.checkManagePermission(userId, organizationId);

    // Validate data source
    const dataSource = await this.dataSourceRepository.findById(dataSourceId);
    if (!dataSource) {
      throw new NotFoundException(`Data source ${dataSourceId} not found`);
    }
    if (dataSource.organizationId !== organizationId) {
      throw new ForbiddenException('Data source does not belong to this organization');
    }

    // Validate that transformScript is provided
    if (!dto.transformScript || !dto.transformScript.trim()) {
      throw new BadRequestException('transformScript is required');
    }

    // Validate upsert configuration
    if (dto.writeMode === WriteMode.UPSERT && (!dto.upsertKey || dto.upsertKey.length === 0)) {
      throw new BadRequestException('Upsert mode requires upsert key columns');
    }

    const schema = await this.destinationSchemaRepository.create({
      organizationId,
      dataSourceId,
      destinationSchema: dto.destinationSchema || 'public',
      destinationTable,
      destinationTableExists: dto.destinationTableExists || false,
      transformScript: dto.transformScript || null,
      writeMode: (dto.writeMode as string) || 'append',
      upsertKey: (dto.upsertKey as string[]) || null,
      name: dto.name || `${destinationTable}_destination`,
      isActive: true,
    });

    // Log activity
    await this.activityLogService.logActivity({
      organizationId,
      userId,
      actionType: DESTINATION_SCHEMA_ACTIONS.CREATED,
      entityType: 'pipeline_destination_schema',
      entityId: schema.id,
      message: `Destination schema created: ${schema.name || schema.id}`,
      metadata: {
        dataSourceId,
        destinationTable,
        writeMode: dto.writeMode || 'append',
      },
    });

    this.logger.log(`Destination schema created: ${schema.id}`);
    return schema;
  }

  /**
   * Get destination schema by ID
   */
  async findById(
    id: string,
    organizationId?: string,
    userId?: string,
  ): Promise<PipelineDestinationSchema | null> {
    const schema = await this.destinationSchemaRepository.findById(id);

    if (schema && organizationId && schema.organizationId !== organizationId) {
      throw new ForbiddenException('Destination schema does not belong to this organization');
    }

    if (schema && userId) {
      await this.checkViewPermission(userId, schema.organizationId);
    }

    return schema;
  }

  /**
   * Get destination schemas by organization
   */
  async findByOrganization(
    organizationId: string,
    userId?: string,
  ): Promise<PipelineDestinationSchema[]> {
    if (userId) {
      await this.checkViewPermission(userId, organizationId);
    }

    return await this.destinationSchemaRepository.findByOrganization(organizationId);
  }

  /**
   * Update destination schema
   */
  async update(
    id: string,
    updates: UpdateDestinationSchemaDto,
    userId: string,
  ): Promise<PipelineDestinationSchema> {
    const schema = await this.destinationSchemaRepository.findById(id);
    if (!schema) {
      throw new NotFoundException(`Destination schema ${id} not found`);
    }

    // AUTHORIZATION
    await this.checkManagePermission(userId, schema.organizationId);

    // Validate upsert configuration
    const newWriteMode = updates.writeMode || schema.writeMode;
    const newUpsertKey = updates.upsertKey || (schema.upsertKey as string[]);
    if (newWriteMode === 'upsert' && (!newUpsertKey || newUpsertKey.length === 0)) {
      throw new BadRequestException('Upsert mode requires upsert key columns');
    }

    const updated = await this.destinationSchemaRepository.update(id, {
      ...updates,
      transformScript: updates.transformScript !== undefined ? updates.transformScript : undefined,
      upsertKey: (updates.upsertKey as string[]) || undefined,
      updatedAt: new Date(),
    });

    // Log activity
    await this.activityLogService.logActivity({
      organizationId: schema.organizationId,
      userId,
      actionType: DESTINATION_SCHEMA_ACTIONS.UPDATED,
      entityType: 'pipeline_destination_schema',
      entityId: id,
      message: `Destination schema updated: ${schema.name || id}`,
      metadata: { changes: Object.keys(updates) },
    });

    return updated;
  }

  /**
   * Validate destination schema against actual database schema
   * Note: Full validation now handled by Python service via discover-schema endpoint
   */
  async validateSchema(id: string, userId: string): Promise<SchemaValidationResult> {
    const schema = await this.destinationSchemaRepository.findById(id);
    if (!schema) {
      throw new NotFoundException(`Destination schema ${id} not found`);
    }

    // AUTHORIZATION
    await this.checkManagePermission(userId, schema.organizationId);

    if (!schema.dataSourceId) {
      return {
        valid: false,
        errors: ['Destination schema must have a data source ID'],
      };
    }

    // Basic validation - full schema validation can be done via Python service discover-schema
    const errors: string[] = [];
    const warnings: string[] = [];

    if (!schema.destinationTable) {
      errors.push('Destination table is required');
    }

    if (!schema.transformScript || !schema.transformScript.trim()) {
      warnings.push('No transform script defined');
    }

    const validationResult: SchemaValidationResult = {
      valid: errors.length === 0,
      errors,
      warnings: warnings.length > 0 ? warnings : undefined,
    };

    // Update schema with validation result (include validatedAt for DestinationSchemaValidationResult)
    await this.destinationSchemaRepository.update(id, {
      validationResult: {
        ...validationResult,
        validatedAt: new Date().toISOString(),
      } as any,
      lastValidatedAt: new Date(),
    });

    // Log activity
    await this.activityLogService.logActivity({
      organizationId: schema.organizationId,
      userId,
      actionType: DESTINATION_SCHEMA_ACTIONS.VALIDATED,
      entityType: 'pipeline_destination_schema',
      entityId: id,
      message: validationResult.valid ? 'Schema validation passed' : 'Schema validation failed',
      metadata: {
        valid: validationResult.valid,
        errorsCount: validationResult.errors.length,
      },
    });

    return validationResult;
  }

  /**
   * Check if destination table exists
   * Note: This can be implemented by calling Python service discover-schema endpoint
   * For now, returns the cached value
   */
  async checkTableExists(id: string, userId: string): Promise<boolean> {
    const schema = await this.destinationSchemaRepository.findById(id);
    if (!schema) {
      throw new NotFoundException(`Destination schema ${id} not found`);
    }

    await this.checkViewPermission(userId, schema.organizationId);

    // TODO: Implement via Python service discover-schema endpoint
    // For now, return cached value
    const exists = schema.destinationTableExists || false;

    return exists;
  }

  /**
   * Create destination table based on column mappings
   * Note: Table creation is now handled by Python service during emit operation
   * This method is kept for backward compatibility but table creation happens automatically
   */
  async createTable(id: string, userId: string): Promise<{ created: boolean; tableName: string }> {
    const schema = await this.destinationSchemaRepository.findById(id);
    if (!schema) {
      throw new NotFoundException(`Destination schema ${id} not found`);
    }

    // AUTHORIZATION
    await this.checkManagePermission(userId, schema.organizationId);

    if (!schema.transformScript || !schema.transformScript.trim()) {
      throw new BadRequestException('Transform script is required to create table');
    }

    // Table creation is now handled automatically by Python service during emit
    // This method is kept for API compatibility
    // The table will be created on first emit if it doesn't exist

    this.logger.log(
      `Table creation will happen automatically on first emit for: ${schema.destinationTable}`,
    );

    return {
      created: false, // Table creation deferred to emit operation
      tableName: schema.destinationTable,
    };
  }

  /**
   * Sync column mappings from source schema
   * Automatically generates column mappings based on source columns
   */
  async syncFromSource(
    id: string,
    sourceSchemaId: string,
    userId: string,
    options?: {
      includeAllColumns?: boolean;
      preserveExisting?: boolean;
    },
  ): Promise<PipelineDestinationSchema> {
    const schema = await this.destinationSchemaRepository.findById(id);
    if (!schema) {
      throw new NotFoundException(`Destination schema ${id} not found`);
    }

    // AUTHORIZATION
    await this.checkManagePermission(userId, schema.organizationId);

    // TODO: Get source schema columns and generate mappings
    // This would integrate with SourceSchemaService to get discovered columns
    // For now, this is a placeholder
    void sourceSchemaId;
    void options;

    throw new BadRequestException('Sync from source not yet implemented');
  }

  /**
   * Validate configuration
   */
  async validateConfiguration(id: string, userId: string): Promise<ValidationResult> {
    const schema = await this.destinationSchemaRepository.findById(id);
    if (!schema) {
      throw new NotFoundException(`Destination schema ${id} not found`);
    }

    await this.checkViewPermission(userId, schema.organizationId);

    const errors: string[] = [];
    const warnings: string[] = [];

    // Check required fields
    if (!schema.dataSourceId) {
      errors.push('Data source ID is required');
    }
    if (!schema.destinationTable) {
      errors.push('Destination table is required');
    }

    // Check data source exists
    if (schema.dataSourceId) {
      const dataSource = await this.dataSourceRepository.findById(schema.dataSourceId);
      if (!dataSource) {
        errors.push('Referenced data source does not exist');
      } else if (!dataSource.isActive) {
        warnings.push('Referenced data source is inactive');
      }
    }

    // Check transform script
    if (!schema.transformScript || !schema.transformScript.trim()) {
      warnings.push('No transform script defined');
    }

    // Check upsert configuration
    if ((schema.writeMode as string) === 'upsert') {
      const upsertKey = schema.upsertKey as string[];
      if (!upsertKey || upsertKey.length === 0) {
        errors.push('Upsert mode requires upsert key columns');
      }
    }

    return {
      valid: errors.length === 0,
      errors,
      warnings: warnings.length > 0 ? warnings : undefined,
    };
  }

  /**
   * Delete destination schema (soft delete)
   */
  async delete(id: string, userId: string): Promise<void> {
    const schema = await this.destinationSchemaRepository.findById(id);
    if (!schema) {
      throw new NotFoundException(`Destination schema ${id} not found`);
    }

    // AUTHORIZATION
    await this.checkManagePermission(userId, schema.organizationId);

    // TODO: Check if schema is used by any active pipelines

    await this.destinationSchemaRepository.delete(id);

    // Log activity
    await this.activityLogService.logActivity({
      organizationId: schema.organizationId,
      userId,
      actionType: DESTINATION_SCHEMA_ACTIONS.DELETED,
      entityType: 'pipeline_destination_schema',
      entityId: id,
      message: `Destination schema deleted: ${schema.name || id}`,
    });

    this.logger.log(`Destination schema deleted: ${id}`);
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
      throw new ForbiddenException('Only OWNER, ADMIN, and EDITOR can manage destination schemas');
    }
  }
}
