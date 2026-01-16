/**
 * Source Schema Service
 * Business logic for pipeline source schema management
 */

import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import type { PipelineSourceSchema } from '../../../database/schemas';
import { ActivityLogService } from '../../activity-logs/activity-log.service';
import { SOURCE_SCHEMA_ACTIONS } from '../../activity-logs/constants/activity-log-types';
import { DataSourceRepository } from '../../data-sources/repositories/data-source.repository';
import { CollectorService } from './collector.service';
import { PipelineSourceSchemaRepository } from '../repositories/pipeline-source-schema.repository';

export interface CreateSourceSchemaDto {
  organizationId: string;
  sourceType: string;
  dataSourceId?: string;
  sourceConfig?: any;
  sourceSchema?: string;
  sourceTable?: string;
  sourceQuery?: string;
  name?: string;
}

export interface UpdateSourceSchemaDto {
  name?: string;
  sourceSchema?: string;
  sourceTable?: string;
  sourceQuery?: string;
  sourceConfig?: any;
}

@Injectable()
export class SourceSchemaService {
  constructor(
    private readonly sourceSchemaRepository: PipelineSourceSchemaRepository,
    private readonly dataSourceRepository: DataSourceRepository,
    private readonly collectorService: CollectorService,
    private readonly activityLogService: ActivityLogService,
  ) {}

  /**
   * Create source schema
   */
  async create(dto: CreateSourceSchemaDto, userId: string): Promise<PipelineSourceSchema> {
    // Validate data source if provided
    if (dto.dataSourceId) {
      const dataSource = await this.dataSourceRepository.findById(dto.dataSourceId);
      if (!dataSource) {
        throw new NotFoundException(`Data source ${dto.dataSourceId} not found`);
      }
      if (dataSource.organizationId !== dto.organizationId) {
        throw new BadRequestException('Data source does not belong to this organization');
      }
    }

    const schema = await this.sourceSchemaRepository.create({
      organizationId: dto.organizationId,
      sourceType: dto.sourceType,
      dataSourceId: dto.dataSourceId || null,
      sourceConfig: dto.sourceConfig || null,
      sourceSchema: dto.sourceSchema || null,
      sourceTable: dto.sourceTable || null,
      sourceQuery: dto.sourceQuery || null,
      name: dto.name || null,
      isActive: true,
    });

    // Log activity
    await this.activityLogService.logActivity({
      organizationId: dto.organizationId,
      userId,
      actionType: SOURCE_SCHEMA_ACTIONS.CREATED,
      entityType: 'pipeline_source_schema',
      entityId: schema.id,
      message: `Source schema created: ${schema.name || schema.id}`,
    });

    return schema;
  }

  /**
   * Get source schema by ID
   */
  async findById(id: string): Promise<PipelineSourceSchema | null> {
    return await this.sourceSchemaRepository.findById(id);
  }

  /**
   * Get source schemas by organization
   */
  async findByOrganization(organizationId: string): Promise<PipelineSourceSchema[]> {
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

    const updated = await this.sourceSchemaRepository.update(id, updates);

    // Log activity
    await this.activityLogService.logActivity({
      organizationId: schema.organizationId,
      userId,
      actionType: SOURCE_SCHEMA_ACTIONS.UPDATED,
      entityType: 'pipeline_source_schema',
      entityId: id,
      message: `Source schema updated: ${schema.name || id}`,
      metadata: { changes: updates },
    });

    return updated;
  }

  /**
   * Discover schema from source
   */
  async discoverSchema(id: string, userId: string): Promise<PipelineSourceSchema> {
    const schema = await this.sourceSchemaRepository.findById(id);
    if (!schema) {
      throw new NotFoundException(`Source schema ${id} not found`);
    }

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
    });

    // Log activity
    await this.activityLogService.logActivity({
      organizationId: schema.organizationId,
      userId,
      actionType: SOURCE_SCHEMA_ACTIONS.DISCOVERED,
      entityType: 'pipeline_source_schema',
      entityId: id,
      message: `Source schema discovered: ${schema.name || id}`,
      metadata: {
        columnsCount: discovered.columns.length,
        primaryKeysCount: discovered.primaryKeys.length,
      },
    });

    return updated;
  }

  /**
   * Delete source schema
   */
  async delete(id: string, userId: string): Promise<void> {
    const schema = await this.sourceSchemaRepository.findById(id);
    if (!schema) {
      throw new NotFoundException(`Source schema ${id} not found`);
    }

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
  }
}
