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
import type {
  DestinationSchemaValidationResult,
  PipelineDestinationSchema,
} from '../../../database/schemas';
import type { DiscoveredColumn } from '../../../database/schemas/data-pipelines/source-schemas/pipeline-source-schemas.schema';
import { ActivityLogService } from '../../activity-logs/activity-log.service';
import { DESTINATION_SCHEMA_ACTIONS } from '../../activity-logs/constants/activity-log-types';
import { ConnectionService } from '../../data-sources/connection.service';
import { DataSourceRepository } from '../../data-sources/repositories/data-source.repository';
import { OrganizationRoleService } from '../../organizations/services/organization-role.service';
import type { CreateDestinationSchemaDto, UpdateDestinationSchemaDto } from '../dto';
import { WriteMode } from '../dto/create-destination-schema.dto';
import { PipelineRepository } from '../repositories/pipeline.repository';
import { PipelineDestinationSchemaRepository } from '../repositories/pipeline-destination-schema.repository';
import { PipelineSourceSchemaRepository } from '../repositories/pipeline-source-schema.repository';
import type { SchemaValidationResult, ValidationResult } from '../types/common.types';
import { validateColumnTypeCompatibility } from '../types/common.types';
import { parseTransformOutputMappings } from '../utils/transform-parser';
import { PythonETLService } from './python-etl.service';

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
    private readonly pipelineRepository: PipelineRepository,
    private readonly sourceSchemaRepository: PipelineSourceSchemaRepository,
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

    // Validate transform: customSql/dbtModel required when transformType is dbt; transformScript when script
    const rawType = dto.transformType?.trim() || 'script';
    const transformType = rawType.toLowerCase();
    if (transformType === 'dbt') {
      const hasDbt = dto.dbtModel?.trim() || dto.customSql?.trim();
      if (!hasDbt) {
        throw new BadRequestException(
          'Transform is required: set customSql or dbtModel when transformType is dbt',
        );
      }
    }
    if (transformType === 'script') {
      const hasScript = (dto as { transformScript?: string }).transformScript?.trim();
      if (!hasScript) {
        throw new BadRequestException(
          'Transform script is required: set transformScript when transformType is script',
        );
      }
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
      transformType: transformType,
      dbtModel: dto.dbtModel || null,
      customSql: dto.customSql || null,
      transformScript: (dto as { transformScript?: string }).transformScript || null,
      writeMode: (dto.writeMode as string) || 'upsert',
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
        writeMode: dto.writeMode || 'upsert',
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
   * Get destination schemas by organization with pagination
   */
  async findByOrganizationPaginated(
    organizationId: string,
    userId: string | undefined,
    limit: number = 20,
    offset: number = 0,
  ) {
    if (userId) {
      await this.checkViewPermission(userId, organizationId);
    }

    return this.destinationSchemaRepository.findByOrganizationPaginated(
      organizationId,
      limit,
      offset,
    );
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
   * Validate destination schema against actual database schema.
   * When the destination table already exists, discovers its columns via the
   * Python ETL and compares them against the linked source schema's
   * discoveredColumns to catch type incompatibilities early.
   */
  async validateSchema(
    id: string,
    userId: string,
    sourceSchemaId?: string,
  ): Promise<SchemaValidationResult> {
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

    const errors: string[] = [];
    const warnings: string[] = [];
    let missingColumns: string[] | undefined;
    let typeMismatches: SchemaValidationResult['typeMismatches'];

    if (!schema.destinationTable) {
      errors.push('Destination table is required');
    }

    const schemaTransformType = (schema.transformType || 'dlt').toLowerCase();
    if (schemaTransformType === 'dbt' && !schema.customSql?.trim() && !schema.dbtModel?.trim()) {
      warnings.push('No custom SQL or dbt model defined');
    }

    // --- Column type compatibility check ---
    if (schema.destinationTableExists && schema.destinationTable && errors.length === 0) {
      try {
        const sourceColumns = await this.resolveSourceColumns(
          id,
          schema.organizationId,
          sourceSchemaId,
        );

        if (sourceColumns && sourceColumns.length > 0) {
          const destColumns = await this.discoverDestinationColumns(
            schema.dataSourceId,
            schema.organizationId,
            schema.destinationSchema ?? 'public',
            schema.destinationTable,
            userId,
          );

          if (destColumns && destColumns.length > 0) {
            const compat = validateColumnTypeCompatibility(
              sourceColumns.map((c) => ({ name: c.name, dataType: c.dataType })),
              destColumns.map((c) => ({ name: c.name, dataType: c.dataType })),
            );
            errors.push(...compat.errors);
            warnings.push(...compat.warnings);
            if (compat.typeMismatches.length > 0) typeMismatches = compat.typeMismatches;
            if (compat.missingColumns.length > 0) missingColumns = compat.missingColumns;
          }
        }
      } catch (err: any) {
        this.logger.warn(`Column type check skipped: ${err?.message ?? err}`);
        warnings.push(
          'Could not verify column type compatibility — validation will be retried at sync time',
        );
      }
    }

    const validationResult: SchemaValidationResult = {
      valid: errors.length === 0,
      errors,
      warnings: warnings.length > 0 ? warnings : undefined,
      missingColumns,
      typeMismatches,
    };

    const resultWithTimestamp: DestinationSchemaValidationResult = {
      ...validationResult,
      warnings: validationResult.warnings ?? [],
      missingColumns: validationResult.missingColumns,
      typeMismatches: validationResult.typeMismatches,
      validatedAt: new Date().toISOString(),
    };
    await this.destinationSchemaRepository.update(id, {
      validationResult: resultWithTimestamp,
      lastValidatedAt: new Date(),
    });

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
        typeMismatchCount: typeMismatches?.length ?? 0,
      },
    });

    return validationResult;
  }

  /**
   * Preview transformed output for the pipeline linked to this destination schema.
   * This reads from the source and applies the saved transform without writing.
   */
  async previewData(
    id: string,
    userId: string,
    limit: number = 10,
  ): Promise<{
    rows: Record<string, unknown>[];
    columns: Array<{
      name: string;
      type: string;
      nullable: boolean;
      primaryKey?: boolean;
    }>;
  }> {
    const schema = await this.destinationSchemaRepository.findById(id);
    if (!schema) {
      throw new NotFoundException(`Destination schema ${id} not found`);
    }

    await this.checkViewPermission(userId, schema.organizationId);

    const linkedPipeline = await this.findLinkedPipeline(id, schema.organizationId);
    if (!linkedPipeline?.sourceSchemaId) {
      throw new BadRequestException(
        'Destination schema preview requires a linked pipeline with a source schema',
      );
    }

    const sourceSchema = await this.sourceSchemaRepository.findById(linkedPipeline.sourceSchemaId);
    if (!sourceSchema?.dataSourceId) {
      throw new BadRequestException('Linked source schema is missing a data source connection');
    }

    const connectionConfig = await this._connectionService.getDecryptedConnection(
      schema.organizationId,
      sourceSchema.dataSourceId,
      userId,
    );

    const preview = await this._pythonETLService.preview({
      sourceSchema,
      connectionConfig,
      limit,
      destinationSchema: schema,
    });

    return {
      rows: preview.records as Record<string, unknown>[],
      columns: this.buildPreviewColumns(
        preview.records as Record<string, unknown>[],
        preview.columns,
        (sourceSchema.discoveredColumns as DiscoveredColumn[] | null | undefined) ?? null,
        schema.transformScript,
        (schema.upsertKey as string[] | null | undefined) ?? null,
      ),
    };
  }

  /**
   * Resolve source columns for the pipeline that references this destination schema.
   * Tries the explicitly-provided sourceSchemaId first, then looks up the pipeline.
   */
  private async resolveSourceColumns(
    destSchemaId: string,
    organizationId: string,
    sourceSchemaId?: string,
  ): Promise<DiscoveredColumn[] | null> {
    if (sourceSchemaId) {
      const src = await this.sourceSchemaRepository.findById(sourceSchemaId);
      if (src?.discoveredColumns) return src.discoveredColumns as DiscoveredColumn[];
    }

    const linked = await this.findLinkedPipeline(destSchemaId, organizationId);
    if (!linked?.sourceSchemaId) return null;

    const src = await this.sourceSchemaRepository.findById(linked.sourceSchemaId);
    return (src?.discoveredColumns as DiscoveredColumn[]) ?? null;
  }

  private async findLinkedPipeline(destSchemaId: string, organizationId: string) {
    return this.pipelineRepository.findByDestinationSchemaId(destSchemaId, organizationId);
  }

  private buildPreviewColumns(
    rows: Record<string, unknown>[],
    previewColumns: string[],
    sourceColumns: DiscoveredColumn[] | null,
    transformScript: string | null | undefined,
    upsertKey: string[] | null,
  ): Array<{
    name: string;
    type: string;
    nullable: boolean;
    primaryKey?: boolean;
  }> {
    const columnNames = previewColumns.length
      ? previewColumns
      : Array.from(new Set(rows.flatMap((row) => Object.keys(row))));
    const sourceColumnsByName = new Map(
      (sourceColumns ?? []).map((column) => [column.name, column]),
    );
    const mappedColumns = parseTransformOutputMappings(transformScript ?? '');
    const upsertKeys = new Set(upsertKey ?? []);

    return columnNames.map((name) => {
      const sourceType = this.normalizeColumnType(
        sourceColumnsByName.get(mappedColumns.get(name) ?? '')?.dataType,
      );
      const sampleValue = this.findSampleValue(rows, name);

      return {
        name,
        type: sourceType ?? this.inferColumnType(sampleValue),
        nullable: rows.length === 0 || rows.some((row) => !(name in row) || row[name] == null),
        ...(upsertKeys.has(name) ? { primaryKey: true } : {}),
      };
    });
  }

  private findSampleValue(rows: Record<string, unknown>[], columnName: string): unknown {
    for (const row of rows) {
      const value = row[columnName];
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  private inferColumnType(value: unknown): string {
    if (typeof value === 'boolean') {
      return 'boolean';
    }
    if (typeof value === 'number') {
      return Number.isInteger(value) ? 'integer' : 'number';
    }
    if (Array.isArray(value)) {
      return 'array';
    }
    if (value && typeof value === 'object') {
      return 'object';
    }
    return 'string';
  }

  private normalizeColumnType(type: string | null | undefined): string | null {
    if (!type) {
      return null;
    }

    const normalized = type.toLowerCase();
    if (normalized.includes('bool')) {
      return 'boolean';
    }
    if (normalized.includes('int') || normalized === 'serial' || normalized === 'bigserial') {
      return 'integer';
    }
    if (
      normalized.includes('numeric') ||
      normalized.includes('decimal') ||
      normalized.includes('double') ||
      normalized.includes('float') ||
      normalized.includes('real')
    ) {
      return 'number';
    }
    if (normalized.includes('json') || normalized.includes('object')) {
      return 'object';
    }
    if (normalized.includes('array')) {
      return 'array';
    }
    return 'string';
  }

  /**
   * Discover the actual column types of an existing destination table via the
   * Python ETL /discover endpoint.
   */
  private async discoverDestinationColumns(
    dataSourceId: string,
    organizationId: string,
    destSchema: string,
    destTable: string,
    userId: string,
  ): Promise<Array<{ name: string; dataType: string }>> {
    const connectionConfig = await this._connectionService.getDecryptedConnection(
      organizationId,
      dataSourceId,
      'system',
    );

    const fakeSourceSchema = {
      sourceSchema: destSchema,
      sourceTable: destTable,
    } as any;

    const result = await this._pythonETLService.discoverSchema({
      sourceSchema: fakeSourceSchema,
      connectionConfig,
      organizationId,
      userId,
    });

    return result.columns.map((c) => ({ name: c.name, dataType: c.dataType }));
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
   * For dlt: sync only to existing tables—no table creation. Returns no-op.
   * For dbt: requires customSql or dbtModel to create table.
   */
  async createTable(id: string, userId: string): Promise<{ created: boolean; tableName: string }> {
    const schema = await this.destinationSchemaRepository.findById(id);
    if (!schema) {
      throw new NotFoundException(`Destination schema ${id} not found`);
    }

    // AUTHORIZATION
    await this.checkManagePermission(userId, schema.organizationId);

    const schemaTransformType = (schema.transformType || 'dlt').toLowerCase();

    // dlt: sync to existing tables only—no table creation
    if (schemaTransformType === 'dlt') {
      this.logger.log(
        `dlt syncs only to existing tables; no table creation for: ${schema.destinationTable}`,
      );
      return {
        created: false,
        tableName: schema.destinationTable,
      };
    }

    // dbt: customSql/dbtModel required
    if (schemaTransformType === 'dbt' && !schema.customSql?.trim() && !schema.dbtModel?.trim()) {
      throw new BadRequestException(
        'Custom SQL or dbt model is required to create table when transformType is dbt',
      );
    }

    // dbt: table creation handled by dbt run (kept for API compatibility)
    return {
      created: false,
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

    // customSql/dbtModel only required for dbt; dlt creates tables automatically
    const schemaTransformType = (schema.transformType || 'dlt').toLowerCase();
    if (schemaTransformType === 'dbt' && !schema.customSql?.trim() && !schema.dbtModel?.trim()) {
      warnings.push('No custom SQL or dbt model defined');
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
