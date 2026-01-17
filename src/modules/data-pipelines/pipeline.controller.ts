/**
 * Pipeline Controller
 * REST API endpoints for data pipeline management
 *
 * Supports all data source types (PostgreSQL, MySQL, MongoDB, S3, REST API, BigQuery, Snowflake)
 * Uses proper DTOs with validation
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
  Logger,
  NotFoundException,
  Param,
  Patch,
  Post,
  Query,
  Request,
  UseGuards,
  UsePipes,
  ValidationPipe,
} from '@nestjs/common';
import type { Request as ExpressRequest } from 'express';
import {
  ApiBearerAuth,
  ApiBody,
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
import { RequiredUUIDPipe } from '../activity-logs/pipes/required-uuid.pipe';
import { PipelineService } from './services/pipeline.service';
import {
  CreatePipelineDto,
  UpdatePipelineDto,
  RunPipelineDto,
  DryRunPipelineDto,
  PipelineResponseDto,
  PipelineRunResponseDto,
  PipelineStatsResponseDto,
  ValidationResultResponseDto,
  DryRunResponseDto,
} from './dto';

type ExpressRequestType = ExpressRequest;

@ApiTags('data-pipelines')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard)
@Controller('organizations/:organizationId/pipelines')
@UsePipes(new ValidationPipe({ transform: true, whitelist: true }))
export class PipelineController {
  private readonly logger = new Logger(PipelineController.name);

  constructor(private readonly pipelineService: PipelineService) {}

  // ============================================================================
  // PIPELINE CRUD OPERATIONS
  // ============================================================================

  /**
   * Create a new pipeline
   */
  @Post()
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: 'Create data pipeline',
    description: 'Create a new data pipeline to sync data from a source to destination.',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization UUID' })
  @ApiBody({ type: CreatePipelineDto })
  @ApiResponse({ status: 201, description: 'Pipeline created successfully', type: PipelineResponseDto })
  @ApiResponse({ status: 400, description: 'Invalid input' })
  @ApiResponse({ status: 404, description: 'Source or destination schema not found' })
  async createPipeline(
    @Param('organizationId', RequiredUUIDPipe) organizationId: string,
    @Body() dto: CreatePipelineDto,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);

      const pipeline = await this.pipelineService.createPipeline({
        ...dto,
        organizationId,
        userId,
      });

      return createSuccessResponse(pipeline, 'Pipeline created successfully', HttpStatus.CREATED);
    } catch (error) {
      this.handleError('create pipeline', error);
    }
  }

  /**
   * List all pipelines for organization
   */
  @Get()
  @ApiOperation({
    summary: 'List all pipelines',
    description: 'Get all data pipelines for the organization.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiResponse({ status: 200, description: 'List of pipelines' })
  async listPipelines(
    @Param('organizationId', RequiredUUIDPipe) organizationId: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      const pipelines = await this.pipelineService.findByOrganization(organizationId, userId);

      return createListResponse(pipelines, `Found ${pipelines.length} pipeline(s)`, {
        total: pipelines.length,
        limit: pipelines.length,
        offset: 0,
        hasMore: false,
      });
    } catch (error) {
      this.handleError('list pipelines', error);
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
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string', description: 'Pipeline UUID' })
  @ApiResponse({ status: 200, description: 'Pipeline details', type: PipelineResponseDto })
  @ApiResponse({ status: 404, description: 'Pipeline not found' })
  async getPipeline(
    @Param('organizationId', RequiredUUIDPipe) organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
  ) {
    try {
      const pipeline = await this.pipelineService.findById(id, organizationId);

      if (!pipeline) {
        throw new NotFoundException(`Pipeline ${id} not found`);
      }

      return createSuccessResponse(pipeline, 'Pipeline retrieved successfully');
    } catch (error) {
      this.handleError('get pipeline', error);
    }
  }

  /**
   * Get pipeline with schemas
   */
  @Get(':id/full')
  @ApiOperation({
    summary: 'Get pipeline with schemas',
    description: 'Retrieve pipeline with source and destination schema details.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Pipeline with schemas' })
  async getPipelineWithSchemas(
    @Param('organizationId', RequiredUUIDPipe) organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
  ) {
    try {
      const result = await this.pipelineService.findByIdWithSchemas(id, organizationId);

      if (!result) {
        throw new NotFoundException(`Pipeline ${id} not found`);
      }

      return createSuccessResponse(result, 'Pipeline with schemas retrieved successfully');
    } catch (error) {
      this.handleError('get pipeline with schemas', error);
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
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiBody({ type: UpdatePipelineDto })
  @ApiResponse({ status: 200, description: 'Pipeline updated successfully', type: PipelineResponseDto })
  async updatePipeline(
    @Param('organizationId', RequiredUUIDPipe) _organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Body() updates: UpdatePipelineDto,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      const updated = await this.pipelineService.updatePipeline(id, updates, userId);

      return createSuccessResponse(updated, 'Pipeline updated successfully');
    } catch (error) {
      this.handleError('update pipeline', error);
    }
  }

  /**
   * Delete pipeline
   */
  @Delete(':id')
  @ApiOperation({
    summary: 'Delete pipeline',
    description: 'Delete a pipeline (soft delete).',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Pipeline deleted successfully' })
  async deletePipeline(
    @Param('organizationId', RequiredUUIDPipe) _organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      await this.pipelineService.deletePipeline(id, userId);

      return createDeleteResponse(id, 'Pipeline deleted successfully');
    } catch (error) {
      this.handleError('delete pipeline', error);
    }
  }

  // ============================================================================
  // PIPELINE EXECUTION
  // ============================================================================

  /**
   * Run pipeline
   */
  @Post(':id/run')
  @ApiOperation({
    summary: 'Execute pipeline',
    description: 'Manually trigger pipeline execution.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiBody({ type: RunPipelineDto, required: false })
  @ApiResponse({ status: 200, description: 'Pipeline run started', type: PipelineRunResponseDto })
  async runPipeline(
    @Param('organizationId', RequiredUUIDPipe) _organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Body() dto: RunPipelineDto,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      const run = await this.pipelineService.runPipeline(
        id,
        userId,
        dto?.triggerType || 'manual',
        dto?.batchSize ? { batchSize: dto.batchSize } : undefined,
      );

      return createSuccessResponse(run, 'Pipeline run started successfully');
    } catch (error) {
      this.handleError('run pipeline', error);
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
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Pipeline paused', type: PipelineResponseDto })
  async pausePipeline(
    @Param('organizationId', RequiredUUIDPipe) _organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      const updated = await this.pipelineService.pausePipeline(id, userId);

      return createSuccessResponse(updated, 'Pipeline paused successfully');
    } catch (error) {
      this.handleError('pause pipeline', error);
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
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Pipeline resumed', type: PipelineResponseDto })
  async resumePipeline(
    @Param('organizationId', RequiredUUIDPipe) _organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      const updated = await this.pipelineService.resumePipeline(id, userId);

      return createSuccessResponse(updated, 'Pipeline resumed successfully');
    } catch (error) {
      this.handleError('resume pipeline', error);
    }
  }

  /**
   * Cancel running pipeline
   */
  @Post(':id/runs/:runId/cancel')
  @ApiOperation({
    summary: 'Cancel pipeline run',
    description: 'Cancel a running or pending pipeline run.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiParam({ name: 'runId', type: 'string' })
  @ApiResponse({ status: 200, description: 'Pipeline run cancelled', type: PipelineRunResponseDto })
  async cancelPipelineRun(
    @Param('organizationId', RequiredUUIDPipe) _organizationId: string,
    @Param('id', RequiredUUIDPipe) _id: string,
    @Param('runId', RequiredUUIDPipe) runId: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      const updated = await this.pipelineService.cancelPipelineRun(runId, userId);

      return createSuccessResponse(updated, 'Pipeline run cancelled successfully');
    } catch (error) {
      this.handleError('cancel pipeline run', error);
    }
  }

  // ============================================================================
  // PIPELINE VALIDATION
  // ============================================================================

  /**
   * Validate pipeline configuration
   */
  @Post(':id/validate')
  @ApiOperation({
    summary: 'Validate pipeline configuration',
    description: 'Validate pipeline configuration before execution.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Validation result', type: ValidationResultResponseDto })
  async validatePipeline(
    @Param('organizationId', RequiredUUIDPipe) _organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      const result = await this.pipelineService.validatePipeline(id, userId);

      return createSuccessResponse(
        result,
        result.valid ? 'Pipeline configuration is valid' : 'Pipeline configuration has errors',
      );
    } catch (error) {
      this.handleError('validate pipeline', error);
    }
  }

  /**
   * Dry run pipeline
   */
  @Post(':id/dry-run')
  @ApiOperation({
    summary: 'Dry run pipeline',
    description: 'Test pipeline execution without writing data.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiBody({ type: DryRunPipelineDto, required: false })
  @ApiResponse({ status: 200, description: 'Dry run completed', type: DryRunResponseDto })
  async dryRunPipeline(
    @Param('organizationId', RequiredUUIDPipe) _organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Body() dto: DryRunPipelineDto,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      const result = await this.pipelineService.dryRunPipeline(id, userId, dto?.sampleSize || 10);

      return createSuccessResponse(result, 'Dry run completed successfully');
    } catch (error) {
      this.handleError('dry run pipeline', error);
    }
  }

  // ============================================================================
  // PIPELINE RUNS
  // ============================================================================

  /**
   * Get pipeline runs
   */
  @Get(':id/runs')
  @ApiOperation({
    summary: 'Get pipeline run history',
    description: 'Retrieve execution history for a pipeline.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiQuery({ name: 'limit', required: false, type: Number, description: 'Max results (default: 20)' })
  @ApiQuery({ name: 'offset', required: false, type: Number, description: 'Offset for pagination' })
  @ApiResponse({ status: 200, description: 'Pipeline run history' })
  async getPipelineRuns(
    @Param('organizationId', RequiredUUIDPipe) _organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Query('limit') limit?: number,
    @Query('offset') offset?: number,
  ) {
    try {
      const effectiveLimit = Math.min(limit || 20, 100);
      const effectiveOffset = offset || 0;

      const runs = await this.pipelineService.getPipelineRuns(id, effectiveLimit, effectiveOffset);

      return createListResponse(runs, `Found ${runs.length} pipeline run(s)`, {
        total: runs.length,
        limit: effectiveLimit,
        offset: effectiveOffset,
        hasMore: runs.length === effectiveLimit,
      });
    } catch (error) {
      this.handleError('get pipeline runs', error);
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
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiParam({ name: 'runId', type: 'string' })
  @ApiResponse({ status: 200, description: 'Pipeline run details', type: PipelineRunResponseDto })
  async getPipelineRun(
    @Param('organizationId', RequiredUUIDPipe) _organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Param('runId', RequiredUUIDPipe) runId: string,
  ) {
    try {
      const run = await this.pipelineService.getPipelineRunById(runId);

      if (!run || run.pipelineId !== id) {
        throw new NotFoundException(`Pipeline run ${runId} not found`);
      }

      return createSuccessResponse(run, 'Pipeline run details retrieved successfully');
    } catch (error) {
      this.handleError('get pipeline run', error);
    }
  }

  // ============================================================================
  // PIPELINE STATISTICS
  // ============================================================================

  /**
   * Get pipeline statistics
   */
  @Get(':id/stats')
  @ApiOperation({
    summary: 'Get pipeline statistics',
    description: 'Retrieve aggregated statistics for a pipeline.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Pipeline statistics', type: PipelineStatsResponseDto })
  async getPipelineStats(
    @Param('organizationId', RequiredUUIDPipe) _organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
  ) {
    try {
      const stats = await this.pipelineService.getPipelineStats(id);

      return createSuccessResponse(stats, 'Pipeline statistics retrieved successfully');
    } catch (error) {
      this.handleError('get pipeline stats', error);
    }
  }

  // ============================================================================
  // HELPER METHODS
  // ============================================================================

  /**
   * Extract user ID from request
   */
  private extractUserId(req: ExpressRequestType): string {
    const userId = req.user?.id;
    if (!userId) {
      throw new BadRequestException('User ID is required');
    }
    return userId;
  }

  /**
   * Handle errors consistently
   */
  private handleError(operation: string, error: unknown): never {
    const message = error instanceof Error ? error.message : String(error);
    this.logger.error(`Failed to ${operation}: ${message}`);

    if (error instanceof HttpException) {
      throw error;
    }

    throw new HttpException(
      {
        success: false,
        error: message,
      },
      HttpStatus.INTERNAL_SERVER_ERROR,
    );
  }
}
