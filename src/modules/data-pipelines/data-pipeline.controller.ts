/**
 * Data Pipeline Controller
 * REST API endpoints for data pipeline management
 */

import {
  BadRequestException,
  Body,
  Controller,
  Delete,
  Get,
  HttpCode,
  HttpException,
  HttpStatus,
  NotFoundException,
  Param,
  Patch,
  Post,
  Query,
  Request,
  UseGuards,
} from '@nestjs/common';
import type { Request as ExpressRequest } from 'express';

// Type declarations are imported via tsconfig
type ExpressRequestType = ExpressRequest;

import {
  ApiBearerAuth,
  ApiOperation,
  ApiParam,
  ApiQuery,
  ApiResponse,
  ApiTags,
} from '@nestjs/swagger';
import {
  createDeleteResponse,
  createListResponse,
  createSuccessResponse,
} from '../../common/dto/api-response.dto';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import { createErrorResponse } from '../data-sources/postgres/utils/error-mapper.util';
import { CreatePipelineDto, UpdatePipelineDto } from './dto/create-pipeline.dto';
import { PostgresPipelineService } from './postgres-pipeline.service';

@ApiTags('data-pipelines')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard)
@Controller('api/data-pipelines')
export class DataPipelineController {
  constructor(private readonly pipelineService: PostgresPipelineService) {}

  /**
   * Create pipeline
   */
  @Post()
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: 'Create data pipeline',
    description: 'Create a new data pipeline to sync data from a source to PostgreSQL destination.',
  })
  @ApiResponse({
    status: 201,
    description: 'Pipeline created successfully',
  })
  async createPipeline(
    @Body() dto: CreatePipelineDto,
    @Request() req: ExpressRequestType,
    @Query('orgId') orgIdParam?: string,
  ) {
    try {
      const finalOrgId = orgIdParam || req?.user?.orgId;
      const finalUserId = req?.user?.id;

      if (!finalOrgId) {
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter or ensure you are authenticated.',
        );
      }

      if (!finalUserId) {
        throw new BadRequestException('User ID is required. Please ensure you are authenticated.');
      }

      // Validate UUID format
      const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
      if (!uuidRegex.test(finalOrgId)) {
        throw new BadRequestException('Invalid Organization ID format. Must be a valid UUID v4.');
      }

      if (!uuidRegex.test(finalUserId)) {
        throw new BadRequestException('Invalid User ID format. Must be a valid UUID v4.');
      }

      // Transform fieldMappings from object to array format if needed
      const transformedCollectors: Array<{
        id: string;
        sourceId: string;
        selectedTables: string[];
        transformers?: Array<{
          id: string;
          name: string;
          collectorId?: string;
          emitterId?: string;
          fieldMappings?: Array<{ source: string; destination: string }>;
        }>;
      }> | undefined = dto.collectors?.map((collector) => ({
        id: collector.id,
        sourceId: collector.sourceId,
        selectedTables: collector.selectedTables,
        transformers: collector.transformers?.map((transformer) => {
          // Convert object format to array format if needed
          let fieldMappings: Array<{ source: string; destination: string }> | undefined = undefined;
          if (transformer.fieldMappings) {
            if (Array.isArray(transformer.fieldMappings)) {
              fieldMappings = transformer.fieldMappings;
            } else {
              // Convert Record<string, string> to Array<{ source: string; destination: string }>
              fieldMappings = Object.entries(transformer.fieldMappings as Record<string, string>).map(([source, destination]) => ({
                source,
                destination: destination as string,
              }));
            }
          }
          return {
            id: transformer.id,
            name: transformer.name,
            collectorId: transformer.collectorId,
            emitterId: transformer.emitterId,
            fieldMappings,
          };
        }),
      }));

      // Create pipeline with schemas
      const pipeline = await this.pipelineService.createPipeline({
        orgId: finalOrgId,
        userId: finalUserId,
        name: dto.name,
        description: dto.description,
        sourceType: dto.sourceType,
        sourceConnectionId: dto.sourceConnectionId,
        sourceConfig: dto.sourceConfig,
        sourceSchema: dto.sourceSchema,
        sourceTable: dto.sourceTable,
        sourceQuery: dto.sourceQuery,
        destinationConnectionId: dto.destinationConnectionId,
        destinationSchema: dto.destinationSchema || 'public',
        destinationTable: dto.destinationTable,
        columnMappings: dto.columnMappings,
        transformations: dto.transformations,
        writeMode: dto.writeMode || 'append',
        upsertKey: dto.upsertKey,
        syncMode: dto.syncMode || 'full',
        incrementalColumn: dto.incrementalColumn,
        syncFrequency: dto.syncFrequency || 'manual',
        collectors: transformedCollectors,
        emitters: dto.emitters,
      });

      return createSuccessResponse(pipeline, 'Pipeline created successfully', HttpStatus.CREATED, {
        pipelineId: pipeline.id,
        pipelineName: pipeline.name,
      });
    } catch (error) {
      // For pipeline creation errors, try to extract the actual PostgreSQL error
      // Drizzle wraps PostgreSQL errors, so we need to check the cause
      const drizzleError = error as any;
      const pgError = drizzleError?.cause || drizzleError;

      // Check if it's a BadRequestException (from our service layer)
      if (error instanceof BadRequestException) {
        throw error;
      }

      // Try to extract PostgreSQL error details
      const pgErrorCode = pgError?.code;
      const pgErrorDetail = pgError?.detail;
      const pgErrorConstraint = pgError?.constraint;
      const pgErrorMessage = pgError?.message || drizzleError?.message || 'Unknown error';

      // Log the actual error for debugging
      console.error('Pipeline creation error:', {
        errorCode: pgErrorCode,
        errorDetail: pgErrorDetail,
        errorConstraint: pgErrorConstraint,
        errorMessage: pgErrorMessage,
        fullError: error,
      });

      // If we have a PostgreSQL error code, handle it specifically
      if (pgErrorCode === '23503') {
        // Foreign key constraint violation
        throw new HttpException(
          {
            code: 'PG_CONSTRAINT_001',
            message: `Foreign key constraint violation: ${pgErrorDetail || pgErrorMessage}`,
            details: {
              constraint: pgErrorConstraint,
              detail: pgErrorDetail,
            },
            suggestion:
              'The source or destination schema may not exist in the database. Please ensure all database migrations have been run.',
          },
          HttpStatus.BAD_REQUEST,
        );
      }

      if (pgErrorCode === '23502') {
        // NOT NULL constraint violation
        throw new HttpException(
          {
            code: 'PG_CONSTRAINT_002',
            message: `Required field is missing: ${pgErrorDetail || pgErrorMessage}`,
            details: {
              constraint: pgErrorConstraint,
              detail: pgErrorDetail,
            },
            suggestion: 'Please check that all required fields are provided.',
          },
          HttpStatus.BAD_REQUEST,
        );
      }

      if (pgErrorCode === '23505') {
        // Unique constraint violation
        throw new HttpException(
          {
            code: 'PG_CONSTRAINT_003',
            message: `Unique constraint violation: ${pgErrorDetail || pgErrorMessage}`,
            details: {
              constraint: pgErrorConstraint,
              detail: pgErrorDetail,
            },
            suggestion: 'A pipeline with this configuration may already exist.',
          },
          HttpStatus.BAD_REQUEST,
        );
      }

      // For other errors, use the standard error response but include PostgreSQL details
      const errorResponse = createErrorResponse(error);
      if (pgErrorCode || pgErrorDetail) {
        errorResponse.error.details = {
          ...errorResponse.error.details,
          postgresErrorCode: pgErrorCode,
          postgresErrorDetail: pgErrorDetail,
          postgresErrorConstraint: pgErrorConstraint,
        };
      }
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * List pipelines
   */
  @Get()
  @ApiOperation({
    summary: 'List all pipelines',
    description: 'Get all data pipelines for the organization.',
  })
  @ApiQuery({
    name: 'orgId',
    required: false,
    description: 'Organization ID',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'List of pipelines',
  })
  async listPipelines(@Request() req: ExpressRequestType, @Query('orgId') orgIdParam?: string) {
    try {
      const finalOrgId = orgIdParam || req?.user?.orgId;

      if (!finalOrgId) {
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter or ensure you are authenticated.',
        );
      }

      // Validate UUID format
      const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
      if (!uuidRegex.test(finalOrgId)) {
        throw new BadRequestException('Invalid Organization ID format. Must be a valid UUID v4.');
      }

      const pipelines = await this.pipelineService.findPipelinesByOrg(finalOrgId);

      return createListResponse(pipelines, `Found ${pipelines.length} pipeline(s)`, {
        total: pipelines.length,
        limit: pipelines.length,
        offset: 0,
        hasMore: false,
      });
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Get pipeline by ID
   */
  @Get(':id')
  @ApiOperation({
    summary: 'Get pipeline details',
    description: 'Retrieve detailed information about a specific pipeline.',
  })
  @ApiParam({
    name: 'id',
    description: 'Pipeline ID',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Pipeline details',
  })
  async getPipeline(
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
    @Query('orgId') orgIdParam?: string,
  ) {
    try {
      const finalOrgId = orgIdParam || req?.user?.orgId;

      if (!finalOrgId) {
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter or ensure you are authenticated.',
        );
      }

      // Validate UUID format
      const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
      if (!uuidRegex.test(finalOrgId)) {
        throw new BadRequestException('Invalid Organization ID format. Must be a valid UUID v4.');
      }

      const pipeline = await this.pipelineService.findPipelineById(id, finalOrgId);

      if (!pipeline) {
        throw new NotFoundException(`Pipeline ${id} not found`);
      }

      return createSuccessResponse(pipeline, 'Pipeline retrieved successfully', HttpStatus.OK, {
        pipelineId: pipeline.id,
        pipelineName: pipeline.name,
        status: pipeline.status,
      });
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Update pipeline
   */
  @Patch(':id')
  @ApiOperation({
    summary: 'Update pipeline',
    description: 'Update pipeline configuration.',
  })
  @ApiParam({
    name: 'id',
    description: 'Pipeline ID',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Pipeline updated successfully',
  })
  async updatePipeline(
    @Param('id') id: string,
    @Body() updates: UpdatePipelineDto,
    @Request() req: ExpressRequestType,
    @Query('orgId') orgId?: string,
  ) {
    try {
      const _finalOrgId = orgId || req?.user?.orgId;

      // If collectors/emitters are provided, update the transformations JSONB field
      if (updates.collectors || updates.emitters) {
        const existingPipeline = await this.pipelineService.findPipelineById(id);
        if (!existingPipeline) {
          throw new NotFoundException(`Pipeline ${id} not found`);
        }

        // Build new transformations object
        const existingTransformations = (existingPipeline.transformations as any) || {};
        const newTransformations = {
          ...existingTransformations,
          collectors: updates.collectors || existingTransformations.collectors || [],
          emitters: updates.emitters || existingTransformations.emitters || [],
        };

        // Update with transformations
        const userId = req.user?.id;
        const updated = await this.pipelineService.updatePipeline(
          id,
          {
            ...updates,
            transformations: newTransformations,
          },
          userId,
        );

        return createSuccessResponse(updated, 'Pipeline updated successfully', HttpStatus.OK, {
          pipelineId: updated.id,
          updatedFields: Object.keys(updates),
        });
      }

      // Regular update without transformations
      const userId = req.user?.id;
      const updated = await this.pipelineService.updatePipeline(id, updates, userId);

      return createSuccessResponse(updated, 'Pipeline updated successfully', HttpStatus.OK, {
        pipelineId: updated.id,
        updatedFields: Object.keys(updates),
      });
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Delete pipeline
   */
  @Delete(':id')
  @ApiOperation({
    summary: 'Delete pipeline',
    description: 'Delete a pipeline and optionally drop the destination table.',
  })
  @ApiParam({
    name: 'id',
    description: 'Pipeline ID',
    type: String,
  })
  @ApiQuery({
    name: 'dropTable',
    required: false,
    description: 'Whether to drop the destination table',
    type: Boolean,
  })
  @ApiResponse({
    status: 200,
    description: 'Pipeline deleted successfully',
  })
  async deletePipeline(
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
    @Query('dropTable') dropTable?: boolean,
    @Query('orgId') orgId?: string,
  ) {
    try {
      const _finalOrgId = orgId || req?.user?.orgId;

      await this.pipelineService.deletePipeline(id, dropTable || false);

      return createDeleteResponse(id, 'Pipeline deleted successfully');
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Execute pipeline
   */
  @Post(':id/run')
  @ApiOperation({
    summary: 'Execute pipeline',
    description: 'Manually trigger pipeline execution.',
  })
  @ApiParam({
    name: 'id',
    description: 'Pipeline ID',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Pipeline execution started',
  })
  async executePipeline(@Param('id') id: string, @Request() _req: Request) {
    try {
      const result = await this.pipelineService.executePipeline(id);

      return createSuccessResponse(result, 'Pipeline execution completed', HttpStatus.OK, {
        runId: result.runId,
        status: result.status,
        rowsWritten: result.rowsWritten,
      });
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Dry run pipeline
   */
  @Post(':id/dry-run')
  @ApiOperation({
    summary: 'Dry run pipeline',
    description: 'Test pipeline without writing data to destination.',
  })
  @ApiParam({
    name: 'id',
    description: 'Pipeline ID',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Dry run completed',
  })
  async dryRunPipeline(@Param('id') id: string, @Request() _req: Request) {
    try {
      const result = await this.pipelineService.dryRunPipeline(id);

      return createSuccessResponse(result, 'Dry run completed successfully', HttpStatus.OK, {
        sourceRowCount: result.sourceRowCount,
        sampleRowCount: result.sampleRows.length,
      });
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Pause pipeline
   */
  @Post(':id/pause')
  @ApiOperation({
    summary: 'Pause pipeline',
    description: 'Pause a pipeline to prevent scheduled executions.',
  })
  @ApiParam({
    name: 'id',
    description: 'Pipeline ID',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Pipeline paused',
  })
  async pausePipeline(
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
    @Query('orgId') orgIdParam?: string,
  ) {
    try {
      const finalOrgId = orgIdParam || req?.user?.orgId;

      await this.pipelineService.togglePipeline(id, 'paused');

      // Fetch and return the updated pipeline to ensure frontend has latest state
      const updatedPipeline = await this.pipelineService.findPipelineById(id, finalOrgId);

      if (!updatedPipeline) {
        throw new NotFoundException(`Pipeline ${id} not found`);
      }

      return createSuccessResponse(updatedPipeline, 'Pipeline paused successfully');
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Resume pipeline
   */
  @Post(':id/resume')
  @ApiOperation({
    summary: 'Resume pipeline',
    description: 'Resume a paused pipeline.',
  })
  @ApiParam({
    name: 'id',
    description: 'Pipeline ID',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Pipeline resumed',
  })
  async resumePipeline(
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
    @Query('orgId') orgIdParam?: string,
  ) {
    try {
      const finalOrgId = orgIdParam || req?.user?.orgId;

      await this.pipelineService.togglePipeline(id, 'active');

      // Fetch and return the updated pipeline to ensure frontend has latest state
      const updatedPipeline = await this.pipelineService.findPipelineById(id, finalOrgId);

      if (!updatedPipeline) {
        throw new NotFoundException(`Pipeline ${id} not found`);
      }

      return createSuccessResponse(updatedPipeline, 'Pipeline resumed successfully');
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Validate pipeline
   */
  @Post(':id/validate')
  @ApiOperation({
    summary: 'Validate pipeline configuration',
    description: 'Validate pipeline configuration before execution.',
  })
  @ApiParam({
    name: 'id',
    description: 'Pipeline ID',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Validation result',
  })
  async validatePipeline(@Param('id') id: string, @Request() _req: Request) {
    try {
      const result = await this.pipelineService.validatePipeline(id);

      return createSuccessResponse(
        result,
        result.valid ? 'Pipeline configuration is valid' : 'Pipeline configuration has errors',
        HttpStatus.OK,
        {
          valid: result.valid,
          errorCount: result.errors.length,
          warningCount: result.warnings.length,
        },
      );
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Auto-map columns
   */
  @Post(':id/auto-map')
  @ApiOperation({
    summary: 'Auto-map source columns to destination',
    description: 'Automatically generate column mappings based on source schema.',
  })
  @ApiParam({
    name: 'id',
    description: 'Pipeline ID',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Auto-mapping result',
  })
  async autoMapColumns(@Param('id') id: string, @Request() _req: Request) {
    try {
      const pipeline = await this.pipelineService.findPipelineById(id);
      if (!pipeline) {
        throw new NotFoundException(`Pipeline ${id} not found`);
      }

      return createSuccessResponse(
        {
          id,
          message: 'Auto-mapping feature requires source schema discovery',
          note: 'Use schema discovery endpoints to get source columns first',
        },
        'Auto-mapping endpoint ready',
      );
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Get pipeline runs
   */
  @Get(':id/runs')
  @ApiOperation({
    summary: 'Get pipeline run history',
    description: 'Retrieve execution history for a pipeline.',
  })
  @ApiParam({
    name: 'id',
    description: 'Pipeline ID',
    type: String,
  })
  @ApiQuery({
    name: 'limit',
    required: false,
    description: 'Number of runs to return',
    type: Number,
  })
  @ApiQuery({
    name: 'offset',
    required: false,
    description: 'Offset for pagination',
    type: Number,
  })
  @ApiResponse({
    status: 200,
    description: 'Pipeline run history',
  })
  async getPipelineRuns(
    @Param('id') id: string,
    @Request() _req: ExpressRequestType,
    @Query('limit') limit?: number,
    @Query('offset') offset?: number,
  ) {
    try {
      const runs = await this.pipelineService.findPipelineRuns(id, limit || 20, offset || 0);

      return createListResponse(runs, `Found ${runs.length} pipeline run(s)`, {
        total: runs.length,
        limit: limit || 20,
        offset: offset || 0,
        hasMore: runs.length === (limit || 20),
      });
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Get pipeline run details
   */
  @Get(':id/runs/:runId')
  @ApiOperation({
    summary: 'Get pipeline run details',
    description: 'Retrieve detailed information about a specific pipeline run.',
  })
  @ApiParam({
    name: 'id',
    description: 'Pipeline ID',
    type: String,
  })
  @ApiParam({
    name: 'runId',
    description: 'Run ID',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Pipeline run details',
  })
  async getPipelineRun(
    @Param('id') id: string,
    @Param('runId') runId: string,
    @Request() _req: ExpressRequestType,
  ) {
    try {
      const run = await this.pipelineService.findPipelineRunById(runId);

      if (!run || run.pipelineId !== id) {
        throw new NotFoundException(`Pipeline run ${runId} not found`);
      }

      return createSuccessResponse(
        run,
        'Pipeline run details retrieved successfully',
        HttpStatus.OK,
        {
          runId: run.id,
          status: run.status,
          rowsWritten: run.rowsWritten,
        },
      );
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Get pipeline statistics
   */
  @Get(':id/stats')
  @ApiOperation({
    summary: 'Get pipeline statistics',
    description: 'Retrieve aggregated statistics for a pipeline.',
  })
  @ApiParam({
    name: 'id',
    description: 'Pipeline ID',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Pipeline statistics',
  })
  async getPipelineStats(@Param('id') id: string, @Request() _req: Request) {
    try {
      const stats = await this.pipelineService.getPipelineStats(id);

      return createSuccessResponse(stats, 'Pipeline statistics retrieved successfully');
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }
}
