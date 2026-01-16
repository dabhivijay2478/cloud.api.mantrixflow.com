/**
 * Pipeline Service
 * Main service for managing data pipelines
 * Works with all data source types using generic collector, transformer, and emitter
 */

import { BadRequestException, Injectable, Logger, NotFoundException } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import type {
  PipelineDestinationSchema,
  PipelineSourceSchema,
  Pipeline,
  PipelineRun,
} from '../../../database/schemas';
import { ActivityLogService } from '../../activity-logs/activity-log.service';
import {
  PIPELINE_ACTIONS,
  PIPELINE_RUN_ACTIONS,
  SOURCE_SCHEMA_ACTIONS,
  DESTINATION_SCHEMA_ACTIONS,
} from '../../activity-logs/constants/activity-log-types';
import { ConnectionService } from '../../data-sources/connection.service';
import { DataSourceRepository } from '../../data-sources/repositories/data-source.repository';
import { CollectorService } from './collector.service';
import { EmitterService } from './emitter.service';
import { TransformerService } from './transformer.service';
import type {
  ColumnMapping,
  DryRunResult,
  PipelineRunResult,
  ValidationResult,
} from '../types/common.types';
import { PipelineRepository } from '../repositories/pipeline.repository';
import { PipelineSourceSchemaRepository } from '../repositories/pipeline-source-schema.repository';
import { PipelineDestinationSchemaRepository } from '../repositories/pipeline-destination-schema.repository';

export interface CreatePipelineDto {
  organizationId: string;
  userId: string;
  name: string;
  description?: string;
  sourceSchemaId: string;
  destinationSchemaId: string;
  transformations?: any[];
  syncMode?: 'full' | 'incremental';
  incrementalColumn?: string;
  syncFrequency?: 'manual' | 'hourly' | 'daily' | 'weekly';
}

export interface UpdatePipelineDto {
  name?: string;
  description?: string;
  status?: 'active' | 'paused' | 'error';
  syncMode?: 'full' | 'incremental';
  incrementalColumn?: string;
  syncFrequency?: 'manual' | 'hourly' | 'daily' | 'weekly';
  transformations?: any[];
}

@Injectable()
export class PipelineService {
  private readonly logger = new Logger(PipelineService.name);

  constructor(
    private readonly pipelineRepository: PipelineRepository,
    private readonly sourceSchemaRepository: PipelineSourceSchemaRepository,
    private readonly destinationSchemaRepository: PipelineDestinationSchemaRepository,
    private readonly connectionService: ConnectionService,
    private readonly dataSourceRepository: DataSourceRepository,
    private readonly collectorService: CollectorService,
    private readonly transformerService: TransformerService,
    private readonly emitterService: EmitterService,
    private readonly activityLogService: ActivityLogService,
  ) {}

  /**
   * Create pipeline
   */
  async createPipeline(dto: CreatePipelineDto): Promise<Pipeline> {
    // Validate source schema exists
    const sourceSchema = await this.sourceSchemaRepository.findById(dto.sourceSchemaId);
    if (!sourceSchema) {
      throw new NotFoundException(`Source schema ${dto.sourceSchemaId} not found`);
    }
    if (sourceSchema.organizationId !== dto.organizationId) {
      throw new BadRequestException('Source schema does not belong to this organization');
    }

    // Validate destination schema exists
    const destinationSchema = await this.destinationSchemaRepository.findById(
      dto.destinationSchemaId,
    );
    if (!destinationSchema) {
      throw new NotFoundException(`Destination schema ${dto.destinationSchemaId} not found`);
    }
    if (destinationSchema.organizationId !== dto.organizationId) {
      throw new BadRequestException('Destination schema does not belong to this organization');
    }

    // Validate data sources exist and are accessible
    if (sourceSchema.dataSourceId) {
      const sourceDataSource = await this.dataSourceRepository.findById(sourceSchema.dataSourceId);
      if (!sourceDataSource || sourceDataSource.organizationId !== dto.organizationId) {
        throw new BadRequestException('Source data source not found or not accessible');
      }
    }

    const destDataSource = await this.dataSourceRepository.findById(destinationSchema.dataSourceId);
    if (!destDataSource || destDataSource.organizationId !== dto.organizationId) {
      throw new BadRequestException('Destination data source not found or not accessible');
    }

    // Check for duplicate name
    const existing = await this.pipelineRepository.findByNameAndOrganizationId(
      dto.name,
      dto.organizationId,
    );
    if (existing && !existing.deletedAt) {
      throw new BadRequestException(`Pipeline with name "${dto.name}" already exists`);
    }

    // Create pipeline
    const pipeline = await this.pipelineRepository.create({
      organizationId: dto.organizationId,
      createdBy: dto.userId,
      name: dto.name,
      description: dto.description,
      sourceSchemaId: dto.sourceSchemaId,
      destinationSchemaId: dto.destinationSchemaId,
      transformations: dto.transformations || null,
      syncMode: dto.syncMode || 'full',
      incrementalColumn: dto.incrementalColumn || null,
      syncFrequency: dto.syncFrequency || 'manual',
      status: 'active',
    });

    // Log activity
    await this.activityLogService.logPipelineAction(
      dto.organizationId,
      dto.userId,
      PIPELINE_ACTIONS.CREATED,
      pipeline.id,
      pipeline.name,
      {
        sourceSchemaId: dto.sourceSchemaId,
        destinationSchemaId: dto.destinationSchemaId,
        syncMode: pipeline.syncMode,
      },
    );

    this.logger.log(`Pipeline created: ${pipeline.id} - ${pipeline.name}`);
    return pipeline;
  }

  /**
   * Get pipelines by organization
   */
  async findByOrganization(organizationId: string): Promise<Pipeline[]> {
    const pipelines = await this.pipelineRepository.findByOrganization(organizationId);
    return pipelines;
  }

  /**
   * Get pipeline by ID
   */
  async findById(id: string, organizationId?: string): Promise<Pipeline | null> {
    return await this.pipelineRepository.findById(id, organizationId);
  }

  /**
   * Get pipeline with schemas
   */
  async findByIdWithSchemas(
    id: string,
    organizationId?: string,
  ): Promise<{
    pipeline: Pipeline;
    sourceSchema: PipelineSourceSchema;
    destinationSchema: PipelineDestinationSchema;
  } | null> {
    return await this.pipelineRepository.findByIdWithSchemas(id, organizationId);
  }

  /**
   * Update pipeline
   */
  async updatePipeline(id: string, updates: UpdatePipelineDto, userId: string): Promise<Pipeline> {
    const pipeline = await this.pipelineRepository.findById(id);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${id} not found`);
    }

    const updated = await this.pipelineRepository.update(id, updates);

    // Log activity
    await this.activityLogService.logPipelineAction(
      pipeline.organizationId,
      userId,
      PIPELINE_ACTIONS.UPDATED,
      id,
      pipeline.name,
      {
        changes: updates,
      },
    );

    this.logger.log(`Pipeline updated: ${id}`);
    return updated;
  }

  /**
   * Delete pipeline
   */
  async deletePipeline(id: string, userId: string): Promise<void> {
    const pipeline = await this.pipelineRepository.findById(id);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${id} not found`);
    }

    await this.pipelineRepository.delete(id);

    // Log activity
    await this.activityLogService.logPipelineAction(
      pipeline.organizationId,
      userId,
      PIPELINE_ACTIONS.UPDATED, // Use UPDATED for soft delete
      id,
      pipeline.name,
      {
        action: 'deleted',
      },
    );

    this.logger.log(`Pipeline deleted: ${id}`);
  }

  /**
   * Run pipeline
   */
  async runPipeline(
    pipelineId: string,
    userId: string,
    triggerType: 'manual' | 'scheduled' | 'api' = 'manual',
  ): Promise<PipelineRun> {
    const pipelineWithSchemas = await this.pipelineRepository.findByIdWithSchemas(pipelineId);
    if (!pipelineWithSchemas) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    const { pipeline, sourceSchema, destinationSchema } = pipelineWithSchemas;

    // Create run record
    const run = await this.pipelineRepository.createRun({
      pipelineId,
      organizationId: pipeline.organizationId,
      status: 'pending',
      jobState: 'pending',
      triggerType,
      triggeredBy: userId,
      startedAt: new Date(),
    });

    // Log activity
    await this.activityLogService.logPipelineRunAction(
      pipeline.organizationId,
      userId,
      PIPELINE_RUN_ACTIONS.STARTED,
      run.id,
      pipelineId,
      pipeline.name,
      {
        triggerType,
      },
    );

    // Execute pipeline asynchronously
    this.executePipelineAsync(run.id, pipeline, sourceSchema, destinationSchema, userId).catch(
      (error) => {
        this.logger.error(`Pipeline execution failed: ${error.message}`, error.stack);
      },
    );

    return run;
  }

  /**
   * Execute pipeline asynchronously
   */
  private async executePipelineAsync(
    runId: string,
    pipeline: Pipeline,
    sourceSchema: PipelineSourceSchema,
    destinationSchema: PipelineDestinationSchema,
    userId: string,
  ): Promise<void> {
    const startTime = Date.now();

    try {
      // Update run status
      await this.pipelineRepository.updateRun(runId, {
        status: 'running',
        jobState: 'running',
      });

      // Get connections
      if (!sourceSchema.dataSourceId || !destinationSchema.dataSourceId) {
        throw new BadRequestException('Source and destination must have data source IDs');
      }

      // Collect data from source
      const sourceData = await this.collectorService.collect({
        sourceSchema,
        organizationId: pipeline.organizationId,
        userId,
        limit: 1000,
      });

      // Transform data
      const columnMappings = (destinationSchema.columnMappings as ColumnMapping[]) || [];
      const transformations = (pipeline.transformations as any[]) || [];

      const transformedData = await this.transformerService.transform(
        sourceData.rows,
        columnMappings,
        transformations,
      );

      // Emit data to destination
      const writeResult = await this.emitterService.emit({
        destinationSchema,
        organizationId: pipeline.organizationId,
        userId,
        rows: transformedData,
        writeMode: (destinationSchema.writeMode as 'append' | 'upsert' | 'replace') || 'append',
        upsertKey: (destinationSchema.upsertKey as string[]) || undefined,
      });

      // Update run with results
      const durationSeconds = Math.floor((Date.now() - startTime) / 1000);
      await this.pipelineRepository.updateRun(runId, {
        status: 'success',
        jobState: 'completed',
        rowsRead: sourceData.rows.length,
        rowsWritten: writeResult.rowsWritten,
        rowsSkipped: writeResult.rowsSkipped,
        rowsFailed: writeResult.rowsFailed,
        completedAt: new Date(),
        durationSeconds,
      });

      // Update pipeline statistics
      await this.pipelineRepository.update(pipeline.id, {
        lastRunAt: new Date(),
        lastRunStatus: 'success',
        totalRowsProcessed: (pipeline.totalRowsProcessed || 0) + writeResult.rowsWritten,
        totalRunsSuccessful: (pipeline.totalRunsSuccessful || 0) + 1,
      });

      // Log activity
      await this.activityLogService.logPipelineRunAction(
        pipeline.organizationId,
        userId,
        PIPELINE_RUN_ACTIONS.COMPLETED,
        runId,
        pipeline.id,
        pipeline.name,
        {
          rowsWritten: writeResult.rowsWritten,
          rowsSkipped: writeResult.rowsSkipped,
          rowsFailed: writeResult.rowsFailed,
          durationSeconds,
        },
      );

      this.logger.log(
        `Pipeline ${pipeline.id} run ${runId} completed: ${writeResult.rowsWritten} rows written`,
      );
    } catch (error) {
      const durationSeconds = Math.floor((Date.now() - startTime) / 1000);
      const errorMessage = error instanceof Error ? error.message : String(error);

      await this.pipelineRepository.updateRun(runId, {
        status: 'failed',
        jobState: 'failed',
        completedAt: new Date(),
        durationSeconds,
        errorMessage,
        errorStack: error instanceof Error ? error.stack : undefined,
      });

      await this.pipelineRepository.update(pipeline.id, {
        lastRunAt: new Date(),
        lastRunStatus: 'failed',
        lastError: errorMessage,
        totalRunsFailed: (pipeline.totalRunsFailed || 0) + 1,
      });

      // Log activity
      await this.activityLogService.logPipelineRunAction(
        pipeline.organizationId,
        userId,
        PIPELINE_RUN_ACTIONS.FAILED,
        runId,
        pipeline.id,
        pipeline.name,
        {
          error: errorMessage,
          durationSeconds,
        },
      );

      this.logger.error(`Pipeline ${pipeline.id} run ${runId} failed: ${errorMessage}`);
      throw error;
    }
  }

  /**
   * Pause pipeline
   */
  async pausePipeline(pipelineId: string, userId: string): Promise<Pipeline> {
    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    const updated = await this.pipelineRepository.update(pipelineId, {
      status: 'paused',
      migrationState: 'pending',
      nextSyncAt: null,
    });

    await this.activityLogService.logPipelineAction(
      pipeline.organizationId,
      userId,
      PIPELINE_ACTIONS.RUN_PAUSED,
      pipelineId,
      pipeline.name,
    );

    return updated;
  }

  /**
   * Resume pipeline
   */
  async resumePipeline(pipelineId: string, userId: string): Promise<Pipeline> {
    const pipeline = await this.pipelineRepository.findById(pipelineId);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    const updated = await this.pipelineRepository.update(pipelineId, {
      status: 'active',
      nextSyncAt: new Date(),
    });

    await this.activityLogService.logPipelineAction(
      pipeline.organizationId,
      userId,
      PIPELINE_ACTIONS.RUN_RESUMED,
      pipelineId,
      pipeline.name,
    );

    return updated;
  }

  /**
   * Validate pipeline configuration
   */
  async validatePipeline(pipelineId: string): Promise<ValidationResult> {
    const pipelineWithSchemas = await this.pipelineRepository.findByIdWithSchemas(pipelineId);
    if (!pipelineWithSchemas) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    const { pipeline, sourceSchema, destinationSchema } = pipelineWithSchemas;

    const errors: string[] = [];
    const warnings: string[] = [];

    // Validate source schema
    if (!sourceSchema.dataSourceId) {
      errors.push('Source schema must have a data source');
    }

    // Validate destination schema
    if (!destinationSchema.dataSourceId) {
      errors.push('Destination schema must have a data source');
    }

    // Validate column mappings
    const columnMappings = (destinationSchema.columnMappings as ColumnMapping[]) || [];
    const validation = this.transformerService.validate(
      columnMappings,
      (pipeline.transformations as any[]) || undefined,
    );

    errors.push(...validation.errors);
    if (validation.warnings) {
      warnings.push(...validation.warnings);
    }

    return {
      valid: errors.length === 0,
      errors,
      warnings,
    };
  }

  /**
   * Dry run pipeline
   */
  async dryRunPipeline(pipelineId: string): Promise<DryRunResult> {
    const pipelineWithSchemas = await this.pipelineRepository.findByIdWithSchemas(pipelineId);
    if (!pipelineWithSchemas) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    const { pipeline, sourceSchema, destinationSchema } = pipelineWithSchemas;

    // Collect sample data (limit to 10 rows for dry run)
    const sourceData = await this.collectorService.collect({
      sourceSchema,
      organizationId: pipeline.organizationId,
      userId: pipeline.createdBy,
      limit: 10,
    });

    // Transform sample data
    const columnMappings = (destinationSchema.columnMappings as ColumnMapping[]) || [];
    const transformations = (pipeline.transformations as any[]) || [];

    const transformedSample = await this.transformerService.transform(
      sourceData.rows,
      columnMappings,
      transformations,
    );

    return {
      wouldWrite: transformedSample.length,
      sourceRowCount: sourceData.totalRows,
      sampleRows: transformedSample,
      errors: [],
    };
  }

  /**
   * Get pipeline runs
   */
  async getPipelineRuns(
    pipelineId: string,
    limit: number = 20,
    offset: number = 0,
  ): Promise<PipelineRun[]> {
    return await this.pipelineRepository.findRunsByPipeline(pipelineId, limit, offset);
  }

  /**
   * Get pipeline run by ID
   */
  async getPipelineRunById(runId: string): Promise<PipelineRun | null> {
    return await this.pipelineRepository.findRunById(runId);
  }

  /**
   * Get pipeline statistics
   */
  async getPipelineStats(pipelineId: string): Promise<{
    totalRowsProcessed: number;
    totalRunsSuccessful: number;
    totalRunsFailed: number;
    lastSuccessfulRun?: Date;
    averageDuration: number;
  }> {
    return await this.pipelineRepository.getStats(pipelineId);
  }
}
