/**
 * Destination Schema Service
 * Business logic for pipeline destination schema management
 */

import { BadRequestException, Injectable, Logger, NotFoundException } from '@nestjs/common';
import type {
  NewPipelineDestinationSchema,
  PipelineDestinationSchema,
} from '../../../database/schemas';
import { ActivityLogService } from '../../activity-logs/activity-log.service';
import { DESTINATION_SCHEMA_ACTIONS } from '../../activity-logs/constants/activity-log-types';
import { DataSourceRepository } from '../../data-sources/repositories/data-source.repository';
import { EmitterService } from './emitter.service';
import { PipelineDestinationSchemaRepository } from '../repositories/pipeline-destination-schema.repository';
import type { ColumnMapping } from '../types/common.types';

export interface CreateDestinationSchemaDto {
  organizationId: string;
  dataSourceId: string;
  destinationSchema?: string;
  destinationTable: string;
  destinationTableExists?: boolean;
  columnMappings?: ColumnMapping[];
  writeMode?: 'append' | 'upsert' | 'replace';
  upsertKey?: string[];
  name?: string;
}

export interface UpdateDestinationSchemaDto {
  name?: string;
  destinationSchema?: string;
  destinationTable?: string;
  columnMappings?: ColumnMapping[];
  writeMode?: 'append' | 'upsert' | 'replace';
  upsertKey?: string[];
}

@Injectable()
export class DestinationSchemaService {
  private readonly logger = new Logger(DestinationSchemaService.name);

  constructor(
    private readonly destinationSchemaRepository: PipelineDestinationSchemaRepository,
    private readonly dataSourceRepository: DataSourceRepository,
    private readonly emitterService: EmitterService,
    private readonly activityLogService: ActivityLogService,
  ) {}

  /**
   * Create destination schema
   */
  async create(
    dto: CreateDestinationSchemaDto,
    userId: string,
  ): Promise<PipelineDestinationSchema> {
    // Validate data source
    const dataSource = await this.dataSourceRepository.findById(dto.dataSourceId);
    if (!dataSource) {
      throw new NotFoundException(`Data source ${dto.dataSourceId} not found`);
    }
    if (dataSource.organizationId !== dto.organizationId) {
      throw new BadRequestException('Data source does not belong to this organization');
    }

    const schema = await this.destinationSchemaRepository.create({
      organizationId: dto.organizationId,
      dataSourceId: dto.dataSourceId,
      destinationSchema: dto.destinationSchema || 'public',
      destinationTable: dto.destinationTable,
      destinationTableExists: dto.destinationTableExists || false,
      columnMappings: (dto.columnMappings as any) || null,
      writeMode: dto.writeMode || 'append',
      upsertKey: (dto.upsertKey as any) || null,
      name: dto.name || null,
      isActive: true,
    });

    // Log activity
    await this.activityLogService.logActivity({
      organizationId: dto.organizationId,
      userId,
      actionType: DESTINATION_SCHEMA_ACTIONS.CREATED,
      entityType: 'pipeline_destination_schema',
      entityId: schema.id,
      message: `Destination schema created: ${schema.name || schema.id}`,
    });

    return schema;
  }

  /**
   * Get destination schema by ID
   */
  async findById(id: string): Promise<PipelineDestinationSchema | null> {
    return await this.destinationSchemaRepository.findById(id);
  }

  /**
   * Get destination schemas by organization
   */
  async findByOrganization(organizationId: string): Promise<PipelineDestinationSchema[]> {
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

    const updated = await this.destinationSchemaRepository.update(id, updates);

    // Log activity
    await this.activityLogService.logActivity({
      organizationId: schema.organizationId,
      userId,
      actionType: DESTINATION_SCHEMA_ACTIONS.UPDATED,
      entityType: 'pipeline_destination_schema',
      entityId: id,
      message: `Destination schema updated: ${schema.name || id}`,
      metadata: { changes: updates },
    });

    return updated;
  }

  /**
   * Validate destination schema
   */
  async validateSchema(id: string, userId: string): Promise<any> {
    const schema = await this.destinationSchemaRepository.findById(id);
    if (!schema) {
      throw new NotFoundException(`Destination schema ${id} not found`);
    }

    if (!schema.dataSourceId) {
      throw new BadRequestException('Destination schema must have a data source ID');
    }

    const columnMappings = (schema.columnMappings as ColumnMapping[]) || [];

    // Validate using emitter
    const validationResult = await this.emitterService.validateSchema({
      destinationSchema: schema,
      organizationId: schema.organizationId,
      userId,
      columnMappings,
    });

    // Update schema with validation result
    await this.destinationSchemaRepository.update(id, {
      validationResult: validationResult as any,
      lastValidatedAt: new Date(),
    });

    // Log activity
    await this.activityLogService.logActivity({
      organizationId: schema.organizationId,
      userId,
      actionType: DESTINATION_SCHEMA_ACTIONS.VALIDATED,
      entityType: 'pipeline_destination_schema',
      entityId: id,
      message: `Destination schema validated: ${schema.name || id}`,
      metadata: {
        valid: validationResult.valid,
        errorsCount: validationResult.errors.length,
      },
    });

    return validationResult;
  }

  /**
   * Delete destination schema
   */
  async delete(id: string, userId: string): Promise<void> {
    const schema = await this.destinationSchemaRepository.findById(id);
    if (!schema) {
      throw new NotFoundException(`Destination schema ${id} not found`);
    }

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
  }
}
