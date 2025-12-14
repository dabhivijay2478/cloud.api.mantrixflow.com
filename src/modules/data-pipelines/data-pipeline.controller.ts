/**
 * Data Pipeline Controller
 * REST API endpoints for data pipeline management
 */

import {
  Controller,
  Post,
  Get,
  Patch,
  Delete,
  Body,
  Param,
  Query,
  HttpCode,
  HttpStatus,
  Request,
  BadRequestException,
  NotFoundException,
  HttpException,
} from '@nestjs/common';
import {
  ApiTags,
  ApiOperation,
  ApiResponse,
  ApiParam,
  ApiQuery,
  ApiBody,
  ApiBearerAuth,
} from '@nestjs/swagger';
import { PostgresPipelineService } from './postgres-pipeline.service';
import { CreatePipelineDto, UpdatePipelineDto } from './dto/create-pipeline.dto';
import { createErrorResponse } from '../data-sources/postgres/utils/error-mapper.util';
import {
  ApiSuccessResponse,
  ApiListResponse,
  ApiDeleteResponse,
  createSuccessResponse,
  createListResponse,
  createDeleteResponse,
} from '../../common/dto/api-response.dto';

// TODO: Create and use actual auth guards
// @UseGuards(JwtAuthGuard, OrgGuard)

interface AuthenticatedRequest extends Request {
  user?: {
    id?: string;
    orgId?: string;
  };
}

@ApiTags('data-pipelines')
@ApiBearerAuth('JWT-auth')
@Controller('api/data-pipelines')
export class DataPipelineController {
  constructor(
    private readonly pipelineService: PostgresPipelineService,
  ) { }

  /**
   * Create pipeline
   */
  @Post()
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: 'Create data pipeline',
    description:
      'Create a new data pipeline to sync data from a source to PostgreSQL destination.',
  })
  @ApiResponse({
    status: 201,
    description: 'Pipeline created successfully',
  })
  async createPipeline(
    @Body() dto: CreatePipelineDto,
    @Request() req: AuthenticatedRequest,
  ) {
    try {
      const orgId = req.user?.orgId || 'default-org-id';
      const userId = req.user?.id || 'default-user-id';

      // Create pipeline using repository directly (service method would be added later)
      const pipeline = await this.pipelineService['pipelineRepository'].create({
        orgId,
        userId,
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
      });

      return createSuccessResponse(
        pipeline,
        'Pipeline created successfully',
        HttpStatus.CREATED,
        {
          pipelineId: pipeline.id,
          pipelineName: pipeline.name,
        },
      );
    } catch (error) {
      const errorResponse = createErrorResponse(error);
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
  async listPipelines(
    @Query('orgId') orgId?: string,
    @Request() req?: AuthenticatedRequest,
  ) {
    try {
      const finalOrgId = orgId || req?.user?.orgId || 'default-org-id';

      const pipelines = await this.pipelineService['pipelineRepository'].findByOrg(finalOrgId);

      return createListResponse(
        pipelines,
        `Found ${pipelines.length} pipeline(s)`,
        {
          total: pipelines.length,
          limit: pipelines.length,
          offset: 0,
          hasMore: false,
        },
      );
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
    @Query('orgId') orgId?: string,
    @Request() req?: AuthenticatedRequest,
  ) {
    try {
      const finalOrgId = orgId || req?.user?.orgId;

      const pipeline = await this.pipelineService['pipelineRepository'].findById(id, finalOrgId);

      if (!pipeline) {
        throw new NotFoundException(`Pipeline ${id} not found`);
      }

      return createSuccessResponse(
        pipeline,
        'Pipeline retrieved successfully',
        HttpStatus.OK,
        {
          pipelineId: pipeline.id,
          pipelineName: pipeline.name,
          status: pipeline.status,
        },
      );
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
    @Query('orgId') orgId?: string,
    @Request() req?: AuthenticatedRequest,
  ) {
    try {
      const finalOrgId = orgId || req?.user?.orgId;

      const updated = await this.pipelineService['pipelineRepository'].update(id, updates);

      return createSuccessResponse(
        updated,
        'Pipeline updated successfully',
        HttpStatus.OK,
        {
          pipelineId: updated.id,
          updatedFields: Object.keys(updates),
        },
      );
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
    @Query('dropTable') dropTable?: boolean,
    @Query('orgId') orgId?: string,
    @Request() req?: AuthenticatedRequest,
  ) {
    try {
      const finalOrgId = orgId || req?.user?.orgId;

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
  async executePipeline(
    @Param('id') id: string,
    @Request() req?: AuthenticatedRequest,
  ) {
    try {
      const result = await this.pipelineService.executePipeline(id);

      return createSuccessResponse(
        result,
        'Pipeline execution completed',
        HttpStatus.OK,
        {
          runId: result.runId,
          status: result.status,
          rowsWritten: result.rowsWritten,
        },
      );
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
  async dryRunPipeline(
    @Param('id') id: string,
    @Request() req?: AuthenticatedRequest,
  ) {
    try {
      const result = await this.pipelineService.dryRunPipeline(id);

      return createSuccessResponse(
        result,
        'Dry run completed successfully',
        HttpStatus.OK,
        {
          sourceRowCount: result.sourceRowCount,
          sampleRowCount: result.sampleRows.length,
        },
      );
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
    @Request() req?: AuthenticatedRequest,
  ) {
    try {
      await this.pipelineService.togglePipeline(id, 'paused');

      return createSuccessResponse(
        { id, status: 'paused' },
        'Pipeline paused successfully',
      );
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
    @Request() req?: AuthenticatedRequest,
  ) {
    try {
      await this.pipelineService.togglePipeline(id, 'active');

      return createSuccessResponse(
        { id, status: 'active' },
        'Pipeline resumed successfully',
      );
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
  async validatePipeline(
    @Param('id') id: string,
    @Request() req?: AuthenticatedRequest,
  ) {
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
  async autoMapColumns(
    @Param('id') id: string,
    @Request() req?: AuthenticatedRequest,
  ) {
    try {
      const pipeline = await this.pipelineService['pipelineRepository'].findById(id);
      if (!pipeline) {
        throw new NotFoundException(`Pipeline ${id} not found`);
      }

      return createSuccessResponse(
        {
          id,
          message: 'Auto-mapping feature requires source schema discovery',
          note: 'Use schema discovery endpoints to get source columns first'
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
    @Query('limit') limit?: number,
    @Query('offset') offset?: number,
    @Request() req?: AuthenticatedRequest,
  ) {
    try {
      const runs = await this.pipelineService['pipelineRepository'].findRunsByPipeline(
        id,
        limit || 20,
        offset || 0,
      );

      return createListResponse(
        runs,
        `Found ${runs.length} pipeline run(s)`,
        {
          total: runs.length,
          limit: limit || 20,
          offset: offset || 0,
          hasMore: runs.length === (limit || 20),
        },
      );
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
    @Request() req?: AuthenticatedRequest,
  ) {
    try {
      const run = await this.pipelineService['pipelineRepository'].findRunById(runId);

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
  async getPipelineStats(
    @Param('id') id: string,
    @Request() req?: AuthenticatedRequest,
  ) {
    try {
      const stats = await this.pipelineService['pipelineRepository'].getStats(id);

      return createSuccessResponse(
        stats,
        'Pipeline statistics retrieved successfully',
      );
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }
}

