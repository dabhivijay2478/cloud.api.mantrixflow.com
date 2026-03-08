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
import {
  ApiBearerAuth,
  ApiBody,
  ApiOperation,
  ApiParam,
  ApiQuery,
  ApiResponse,
  ApiTags,
} from '@nestjs/swagger';
import type { Request as ExpressRequest } from 'express';
import { ERROR_CODES, type ErrorCode } from '../../common/constants';
import {
  createDeleteResponse,
  createListResponse,
  createSuccessResponse,
} from '../../common/dto/api-response.dto';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import { ActivityLoggerService } from '../../common/logger';
import {
  areSourceDbMutationsAllowed,
  SOURCE_DB_MUTATION_POLICY_MESSAGE,
} from '../../common/utils/source-db-mutation-policy';
import { RequiredUUIDPipe } from '../activity-logs/pipes/required-uuid.pipe';
import {
  CreatePipelineDto,
  DryRunPipelineDto,
  DryRunResponseDto,
  PipelineResponseDto,
  PipelineRunResponseDto,
  PipelineStatsResponseDto,
  RunPipelineDto,
  UpdatePipelineDto,
  ValidationResultResponseDto,
} from './dto';
import { PipelineService } from './services/pipeline.service';

type ExpressRequestType = ExpressRequest;

@ApiTags('data-pipelines')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard)
@Controller('organizations/:organizationId/pipelines')
@UsePipes(new ValidationPipe({ transform: true, whitelist: true }))
export class PipelineController {
  private readonly logger = new Logger(PipelineController.name);

  constructor(
    private readonly pipelineService: PipelineService,
    private readonly activity: ActivityLoggerService,
  ) {}

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
  @ApiResponse({
    status: 201,
    description: 'Pipeline created successfully',
    type: PipelineResponseDto,
  })
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

      this.activity.info('pipeline.created', `Pipeline created: ${pipeline.name}`, {
        pipelineId: pipeline.id,
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
    description: 'Get all data pipelines for the organization with pagination.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiQuery({
    name: 'limit',
    required: false,
    type: Number,
    description: 'Items per page (default: 20, max: 100)',
  })
  @ApiQuery({
    name: 'offset',
    required: false,
    type: Number,
    description: 'Items to skip (default: 0). Ignored when cursor is provided.',
  })
  @ApiQuery({
    name: 'cursor',
    required: false,
    type: String,
    description:
      'Cursor for cursor-based pagination (created_at ISO string). Use for large orgs (1M+ pipelines).',
  })
  @ApiResponse({ status: 200, description: 'List of pipelines' })
  async listPipelines(
    @Param('organizationId', RequiredUUIDPipe) organizationId: string,
    @Request() req: ExpressRequestType,
    @Query('limit') limit?: string,
    @Query('offset') offset?: string,
    @Query('cursor') cursor?: string,
  ) {
    try {
      const userId = this.extractUserId(req);
      const limitNum = Math.min(Math.max(parseInt(limit || '20', 10) || 20, 1), 100);
      const offsetNum = Math.max(parseInt(offset || '0', 10) || 0, 0);

      const result = await this.pipelineService.findByOrganizationPaginated(
        organizationId,
        userId,
        limitNum,
        offsetNum,
        cursor,
      );

      const meta =
        'nextCursor' in result
          ? {
              total: result.total >= 0 ? result.total : undefined,
              limit: limitNum,
              offset: 0,
              nextCursor: result.nextCursor as string | null,
              hasMore: !!result.nextCursor,
            }
          : {
              total: result.total,
              limit: limitNum,
              offset: offsetNum,
              hasMore: offsetNum + limitNum < result.total,
            };

      return createListResponse(result.data, `Found ${result.data.length} pipeline(s)`, meta);
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
      const pipelineWithSchemas = await this.pipelineService.findByIdWithSchemas(
        id,
        organizationId,
      );

      if (!pipelineWithSchemas) {
        throw new NotFoundException(`Pipeline ${id} not found`);
      }

      const { pipeline } = pipelineWithSchemas;

      // Transform script is now the source of truth for transformations
      // No need to extract mappings - the script handles all transformations

      // Return pipeline response
      const pipelineResponse = {
        ...pipeline,
        appliedMappings: [], // Empty since we use transform script now
      };

      return createSuccessResponse(pipelineResponse, 'Pipeline retrieved successfully');
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
  @ApiResponse({
    status: 200,
    description: 'Pipeline updated successfully',
    type: PipelineResponseDto,
  })
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

      this.activity.info('pipeline.started', `Pipeline run started: ${id}`, {
        pipelineId: id,
        runId: run.id,
        userId,
        metadata: { triggerType: dto?.triggerType || 'manual' },
      });

      return createSuccessResponse(run, 'Pipeline run started successfully');
    } catch (error) {
      this.handleError('run pipeline', error);
    }
  }

  /**
   * Get sync state (cursor/LSN) for incremental/CDC pipelines
   */
  @Get(':id/sync-state')
  @ApiOperation({
    summary: 'Get sync state',
    description: 'Get pipeline sync state (cursor, LSN). NestJS owns state.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Sync state' })
  async getSyncState(
    @Param('organizationId', RequiredUUIDPipe) organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      const result = await this.pipelineService.getSyncState(id, organizationId, userId);
      return createSuccessResponse(result, result.message);
    } catch (error) {
      this.handleError('get sync state', error);
    }
  }

  /**
   * Reset sync state — next run will do full sync
   */
  @Delete(':id/sync-state')
  @ApiOperation({
    summary: 'Reset sync state',
    description: 'Clear sync state. Next run will do a full sync.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Sync state reset' })
  async resetSyncState(
    @Param('organizationId', RequiredUUIDPipe) organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      const result = await this.pipelineService.resetSyncState(id, organizationId, userId);
      return createSuccessResponse(result, result.message);
    } catch (error) {
      this.handleError('reset sync state', error);
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
  @ApiQuery({
    name: 'limit',
    required: false,
    type: Number,
    description: 'Max results (default: 20)',
  })
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

  /**
   * Get pipeline schedule information
   */
  @Get(':id/schedule-info')
  @ApiOperation({
    summary: 'Get pipeline schedule information',
    description: 'Retrieve schedule status, next run time, and configuration for a pipeline.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Pipeline schedule information' })
  async getScheduleInfo(
    @Param('organizationId', RequiredUUIDPipe) organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
  ) {
    try {
      const pipeline = await this.pipelineService.findById(id, organizationId);

      if (!pipeline) {
        throw new NotFoundException(`Pipeline ${id} not found`);
      }

      const scheduleInfo = {
        isScheduled: pipeline.scheduleType && pipeline.scheduleType !== 'none',
        scheduleType: pipeline.scheduleType || 'none',
        scheduleValue: pipeline.scheduleValue,
        scheduleTimezone: pipeline.scheduleTimezone || 'UTC',
        nextScheduledRunAt: pipeline.nextScheduledRunAt,
        lastScheduledRunAt: pipeline.lastScheduledRunAt,
        lastRunAt: pipeline.lastRunAt,
        lastRunStatus: pipeline.lastRunStatus,
        status: pipeline.status,
        humanReadable: this.getHumanReadableSchedule(
          pipeline.scheduleType || 'none',
          pipeline.scheduleValue,
          pipeline.scheduleTimezone || 'UTC',
        ),
      };

      return createSuccessResponse(
        scheduleInfo,
        'Pipeline schedule information retrieved successfully',
      );
    } catch (error) {
      this.handleError('get pipeline schedule info', error);
    }
  }

  /**
   * Get CDC (Change Data Capture) status and configuration
   */
  @Get(':id/cdc-status')
  @ApiOperation({
    summary: 'Get CDC status and configuration',
    description:
      'Check if pipeline is properly configured for CDC/incremental sync and its current state.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'CDC status information' })
  async getCdcStatus(
    @Param('organizationId', RequiredUUIDPipe) organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
  ) {
    try {
      const pipeline = await this.pipelineService.findById(id, organizationId);

      if (!pipeline) {
        throw new NotFoundException(`Pipeline ${id} not found`);
      }

      // Check CDC configuration requirements
      const hasIncrementalMode = pipeline.syncMode === 'incremental';
      const hasIncrementalColumn = !!pipeline.incrementalColumn;
      const hasCompletedFullSync = pipeline.lastRunStatus === 'success' && pipeline.lastSyncAt;
      const isInListingStatus = pipeline.status === 'listing';

      // Get checkpoint info
      const checkpoint = pipeline.checkpoint as any;
      const hasCheckpoint = !!checkpoint?.lastSyncValue;

      const cdcReadiness = {
        // Configuration checks
        isConfiguredForCdc: hasIncrementalMode && hasIncrementalColumn,
        syncMode: pipeline.syncMode,
        incrementalColumn: pipeline.incrementalColumn,

        // State checks
        hasCompletedFullSync,
        isInListingStatus,
        hasCheckpoint,
        currentStatus: pipeline.status,

        // Checkpoint details
        checkpoint: checkpoint
          ? {
              lastSyncValue: checkpoint.lastSyncValue,
              watermarkField: checkpoint.watermarkField || pipeline.incrementalColumn,
              lastSyncAt: checkpoint.lastSyncAt,
              rowsProcessed: checkpoint.rowsProcessed,
            }
          : null,

        // CDC readiness summary
        cdcEnabled:
          hasIncrementalMode &&
          hasIncrementalColumn &&
          hasCompletedFullSync &&
          isInListingStatus &&
          hasCheckpoint,

        // Issues if not ready
        issues: [] as string[],

        // Recommendations
        recommendations: [] as string[],
      };

      // Add issues and recommendations
      if (!hasIncrementalMode) {
        cdcReadiness.issues.push('syncMode is not "incremental"');
        cdcReadiness.recommendations.push('Update pipeline syncMode to "incremental"');
      }
      if (!hasIncrementalColumn) {
        cdcReadiness.issues.push('incrementalColumn is not set');
        cdcReadiness.recommendations.push(
          'Set incrementalColumn to a timestamp or auto-incrementing column (e.g., updated_at, id)',
        );
      }
      if (!hasCompletedFullSync) {
        cdcReadiness.issues.push('No successful full sync completed');
        cdcReadiness.recommendations.push(
          'Run the pipeline at least once to complete initial full sync',
        );
      }
      if (!isInListingStatus && hasCompletedFullSync) {
        cdcReadiness.issues.push(`Pipeline status is "${pipeline.status}" instead of "listing"`);
        cdcReadiness.recommendations.push(
          'After a successful full sync with incremental mode, status should be "listing" for CDC to work',
        );
      }
      if (!hasCheckpoint) {
        cdcReadiness.issues.push('No checkpoint stored');
        cdcReadiness.recommendations.push('Complete a full sync to create the initial checkpoint');
      }
      if (!areSourceDbMutationsAllowed()) {
        cdcReadiness.issues.push(SOURCE_DB_MUTATION_POLICY_MESSAGE);
        cdcReadiness.recommendations.push(
          'Keep this pipeline on FULL sync unless source DB mutations are explicitly allowed by platform policy.',
        );
        cdcReadiness.cdcEnabled = false;
      }

      // Add pg_cron/PGMQ requirements note
      if (cdcReadiness.cdcEnabled) {
        cdcReadiness.recommendations.push(
          '✅ CDC is ready! Ensure pg_cron and PGMQ extensions are installed and the pipeline_polling_function is scheduled.',
        );
      } else {
        cdcReadiness.recommendations.push(
          '📌 For automatic CDC polling, ensure pg_cron and PGMQ extensions are installed in your database.',
        );
      }

      return createSuccessResponse(cdcReadiness, 'CDC status retrieved successfully');
    } catch (error) {
      this.handleError('get CDC status', error);
    }
  }

  /**
   * Get human-readable schedule description
   */
  private getHumanReadableSchedule(
    scheduleType: string,
    scheduleValue: string | null | undefined,
    timezone: string,
  ): string {
    switch (scheduleType) {
      case 'none':
        return 'Manual (no automatic schedule)';
      case 'minutes': {
        const minutes = parseInt(scheduleValue || '30', 10);
        return `Every ${minutes} minute${minutes === 1 ? '' : 's'}`;
      }
      case 'hourly': {
        const hours = parseInt(scheduleValue || '1', 10);
        return hours === 1 ? 'Every hour' : `Every ${hours} hours`;
      }
      case 'daily':
        return `Daily at ${scheduleValue || '00:00'} (${timezone})`;
      case 'weekly': {
        const parts = (scheduleValue || '1:00:00').split(':');
        const dayNames = [
          'Sunday',
          'Monday',
          'Tuesday',
          'Wednesday',
          'Thursday',
          'Friday',
          'Saturday',
        ];
        const day = parseInt(parts[0], 10);
        const time = parts.length > 1 ? `${parts[1]}:${parts[2] || '00'}` : '00:00';
        return `Every ${dayNames[day] || 'Monday'} at ${time} (${timezone})`;
      }
      case 'monthly': {
        const parts = (scheduleValue || '1:00:00').split(':');
        const dayOfMonth = parseInt(parts[0], 10);
        const time = parts.length > 1 ? `${parts[1]}:${parts[2] || '00'}` : '00:00';
        return `Monthly on day ${dayOfMonth} at ${time} (${timezone})`;
      }
      case 'custom_cron':
        return `Custom: ${scheduleValue || 'Not set'}`;
      default:
        return 'Unknown schedule';
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
   * Handle errors consistently. Rethrows HttpException for filter to format.
   * For other errors, throws HttpException with code/message for filter.
   */
  private handleError(operation: string, error: unknown): never {
    if (error instanceof HttpException) {
      throw error;
    }

    let message = error instanceof Error ? error.message : String(error);
    let statusCode = HttpStatus.INTERNAL_SERVER_ERROR;
    let code: ErrorCode = ERROR_CODES.INTERNAL_ERROR;

    if (error instanceof Error) {
      this.logger.error(`Failed to ${operation}: ${message}`, error.stack);

      if (message.includes('relation') && message.includes('does not exist')) {
        message =
          `Database table does not exist. Please run migrations:\n` +
          `  cd apps/api && bun run db:migrate\n` +
          `Error: ${message}`;
        statusCode = HttpStatus.SERVICE_UNAVAILABLE;
        code = ERROR_CODES.SERVICE_UNAVAILABLE;
      } else if (message.includes('column') && message.includes('does not exist')) {
        const columnMatch = message.match(/column "([^"]+)" does not exist/);
        const columnName = columnMatch ? columnMatch[1] : 'unknown';
        message =
          `Database column "${columnName}" does not exist. This usually means migrations haven't been run.\n` +
          `\nTo fix this, run:\n` +
          `  cd apps/api\n` +
          `  bun run db:migrate\n` +
          `\nOriginal error: ${message}`;
        statusCode = HttpStatus.SERVICE_UNAVAILABLE;
        code = ERROR_CODES.SERVICE_UNAVAILABLE;
      } else if (message.includes('syntax error')) {
        message = `Database query error: ${message}`;
      }
    } else {
      this.logger.error(`Failed to ${operation}: ${message}`);
    }

    throw new HttpException({ code, message }, statusCode);
  }
}
