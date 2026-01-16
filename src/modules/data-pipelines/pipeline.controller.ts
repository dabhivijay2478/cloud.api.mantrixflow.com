/**
 * Pipeline Controller
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
  Logger,
  NotFoundException,
  Param,
  Patch,
  Post,
  Query,
  Request,
  UseGuards,
} from '@nestjs/common';
import type { Request as ExpressRequest } from 'express';
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
import { PipelineService } from './services/pipeline.service';
import { SourceSchemaService } from './services/source-schema.service';
import { DestinationSchemaService } from './services/destination-schema.service';
import type { CreatePipelineDto, UpdatePipelineDto } from './services/pipeline.service';
import type {
  CreateSourceSchemaDto,
  UpdateSourceSchemaDto,
} from './services/source-schema.service';
import type {
  CreateDestinationSchemaDto,
  UpdateDestinationSchemaDto,
} from './services/destination-schema.service';

type ExpressRequestType = ExpressRequest;

@ApiTags('data-pipelines')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard)
@Controller('organizations/:organizationId/pipelines')
export class PipelineController {
  private readonly logger = new Logger(PipelineController.name);

  constructor(
    private readonly pipelineService: PipelineService,
    private readonly sourceSchemaService: SourceSchemaService,
    private readonly destinationSchemaService: DestinationSchemaService,
  ) {}

  /**
   * Create pipeline
   */
  @Post()
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: 'Create data pipeline',
    description: 'Create a new data pipeline to sync data from a source to destination.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiResponse({ status: 201, description: 'Pipeline created successfully' })
  async createPipeline(
    @Param('organizationId') organizationId: string,
    @Body() dto: Omit<CreatePipelineDto, 'organizationId' | 'userId'>,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = req.user?.id;
      if (!userId) {
        throw new BadRequestException('User ID is required');
      }

      const pipeline = await this.pipelineService.createPipeline({
        ...dto,
        organizationId,
        userId,
      });

      return createSuccessResponse(pipeline, 'Pipeline created successfully', HttpStatus.CREATED);
    } catch (error) {
      this.logger.error(
        `Failed to create pipeline: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to create pipeline',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
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
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiResponse({ status: 200, description: 'List of pipelines' })
  async listPipelines(@Param('organizationId') organizationId: string) {
    try {
      const pipelines = await this.pipelineService.findByOrganization(organizationId);

      return createListResponse(pipelines, `Found ${pipelines.length} pipeline(s)`, {
        total: pipelines.length,
        limit: pipelines.length,
        offset: 0,
        hasMore: false,
      });
    } catch (error) {
      this.logger.error(
        `Failed to list pipelines: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw new HttpException(
        {
          success: false,
          error: error instanceof Error ? error.message : 'Failed to list pipelines',
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
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
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Pipeline details' })
  async getPipeline(@Param('organizationId') organizationId: string, @Param('id') id: string) {
    try {
      const pipeline = await this.pipelineService.findById(id, organizationId);

      if (!pipeline) {
        throw new NotFoundException(`Pipeline ${id} not found`);
      }

      return createSuccessResponse(pipeline, 'Pipeline retrieved successfully');
    } catch (error) {
      this.logger.error(
        `Failed to get pipeline: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to get pipeline',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
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
  @ApiResponse({ status: 200, description: 'Pipeline updated successfully' })
  async updatePipeline(
    @Param('organizationId') organizationId: string,
    @Param('id') id: string,
    @Body() updates: UpdatePipelineDto,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = req.user?.id;
      if (!userId) {
        throw new BadRequestException('User ID is required');
      }

      const updated = await this.pipelineService.updatePipeline(id, updates, userId);

      return createSuccessResponse(updated, 'Pipeline updated successfully');
    } catch (error) {
      this.logger.error(
        `Failed to update pipeline: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to update pipeline',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
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
    @Param('organizationId') organizationId: string,
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = req.user?.id;
      if (!userId) {
        throw new BadRequestException('User ID is required');
      }

      await this.pipelineService.deletePipeline(id, userId);

      return createDeleteResponse(id, 'Pipeline deleted successfully');
    } catch (error) {
      this.logger.error(
        `Failed to delete pipeline: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to delete pipeline',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
    }
  }

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
  @ApiResponse({ status: 200, description: 'Pipeline run started' })
  async runPipeline(
    @Param('organizationId') organizationId: string,
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = req.user?.id;
      if (!userId) {
        throw new BadRequestException('User ID is required');
      }

      const run = await this.pipelineService.runPipeline(id, userId, 'manual');

      return createSuccessResponse(run, 'Pipeline run started successfully');
    } catch (error) {
      this.logger.error(
        `Failed to run pipeline: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to run pipeline',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
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
  @ApiResponse({ status: 200, description: 'Pipeline paused' })
  async pausePipeline(
    @Param('organizationId') organizationId: string,
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = req.user?.id;
      if (!userId) {
        throw new BadRequestException('User ID is required');
      }

      const updated = await this.pipelineService.pausePipeline(id, userId);

      return createSuccessResponse(updated, 'Pipeline paused successfully');
    } catch (error) {
      this.logger.error(
        `Failed to pause pipeline: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to pause pipeline',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
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
  @ApiResponse({ status: 200, description: 'Pipeline resumed' })
  async resumePipeline(
    @Param('organizationId') organizationId: string,
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = req.user?.id;
      if (!userId) {
        throw new BadRequestException('User ID is required');
      }

      const updated = await this.pipelineService.resumePipeline(id, userId);

      return createSuccessResponse(updated, 'Pipeline resumed successfully');
    } catch (error) {
      this.logger.error(
        `Failed to resume pipeline: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to resume pipeline',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
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
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Validation result' })
  async validatePipeline(@Param('organizationId') organizationId: string, @Param('id') id: string) {
    try {
      const result = await this.pipelineService.validatePipeline(id);

      return createSuccessResponse(
        result,
        result.valid ? 'Pipeline configuration is valid' : 'Pipeline configuration has errors',
      );
    } catch (error) {
      this.logger.error(
        `Failed to validate pipeline: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to validate pipeline',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
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
  @ApiResponse({ status: 200, description: 'Dry run completed' })
  async dryRunPipeline(@Param('organizationId') organizationId: string, @Param('id') id: string) {
    try {
      const result = await this.pipelineService.dryRunPipeline(id);

      return createSuccessResponse(result, 'Dry run completed successfully');
    } catch (error) {
      this.logger.error(
        `Failed to dry run pipeline: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to dry run pipeline',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
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
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiQuery({ name: 'limit', required: false, type: Number })
  @ApiQuery({ name: 'offset', required: false, type: Number })
  @ApiResponse({ status: 200, description: 'Pipeline run history' })
  async getPipelineRuns(
    @Param('organizationId') organizationId: string,
    @Param('id') id: string,
    @Query('limit') limit?: number,
    @Query('offset') offset?: number,
  ) {
    try {
      const runs = await this.pipelineService.getPipelineRuns(id, limit || 20, offset || 0);

      return createListResponse(runs, `Found ${runs.length} pipeline run(s)`, {
        total: runs.length,
        limit: limit || 20,
        offset: offset || 0,
        hasMore: runs.length === (limit || 20),
      });
    } catch (error) {
      this.logger.error(
        `Failed to get pipeline runs: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to get pipeline runs',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
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
  @ApiResponse({ status: 200, description: 'Pipeline run details' })
  async getPipelineRun(
    @Param('organizationId') organizationId: string,
    @Param('id') id: string,
    @Param('runId') runId: string,
  ) {
    try {
      const run = await this.pipelineService.getPipelineRunById(runId);

      if (!run || run.pipelineId !== id) {
        throw new NotFoundException(`Pipeline run ${runId} not found`);
      }

      return createSuccessResponse(run, 'Pipeline run details retrieved successfully');
    } catch (error) {
      this.logger.error(
        `Failed to get pipeline run: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to get pipeline run',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
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
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Pipeline statistics' })
  async getPipelineStats(@Param('organizationId') organizationId: string, @Param('id') id: string) {
    try {
      const stats = await this.pipelineService.getPipelineStats(id);

      return createSuccessResponse(stats, 'Pipeline statistics retrieved successfully');
    } catch (error) {
      this.logger.error(
        `Failed to get pipeline stats: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to get pipeline stats',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
    }
  }
}
