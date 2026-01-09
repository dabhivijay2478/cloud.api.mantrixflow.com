/**
 * Activity Log Controller
 * REST API endpoints for activity log management
 */

import {
  Controller,
  Get,
  Query,
  Request,
  UseGuards,
  BadRequestException,
} from '@nestjs/common';
import type { Request as ExpressRequest } from 'express';
import {
  ApiBearerAuth,
  ApiOperation,
  ApiQuery,
  ApiResponse,
  ApiTags,
} from '@nestjs/swagger';
import { createListResponse } from '../../common/dto/api-response.dto';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import { ActivityLogService } from './activity-log.service';
import { RequiredUUIDPipe } from './pipes/required-uuid.pipe';
import { OptionalUUIDPipe } from './pipes/optional-uuid.pipe';

type ExpressRequestType = ExpressRequest;

@ApiTags('activity-logs')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard)
@Controller('api/activity-logs')
export class ActivityLogController {
  constructor(private readonly activityLogService: ActivityLogService) {}

  /**
   * Get activity logs
   * Returns activity logs for the specified organization with optional filtering and pagination
   */
  @Get()
  @ApiOperation({
    summary: 'Get activity logs',
    description:
      'Get activity logs for an organization with optional filtering and pagination',
  })
  @ApiQuery({
    name: 'organizationId',
    required: true,
    description: 'Organization ID (UUID)',
    type: String,
  })
  @ApiQuery({
    name: 'actionType',
    required: false,
    description: 'Filter by action type (e.g., ORG_CREATED, PIPELINE_RUN_STARTED)',
    type: String,
  })
  @ApiQuery({
    name: 'entityType',
    required: false,
    description: 'Filter by entity type (organization, pipeline, migration, etc.)',
    type: String,
  })
  @ApiQuery({
    name: 'entityId',
    required: false,
    description: 'Filter by entity ID (UUID)',
    type: String,
  })
  @ApiQuery({
    name: 'userId',
    required: false,
    description: 'Filter by user ID (UUID)',
    type: String,
  })
  @ApiQuery({
    name: 'limit',
    required: false,
    description: 'Maximum number of logs to return (default: 50, max: 100)',
    type: Number,
  })
  @ApiQuery({
    name: 'cursor',
    required: false,
    description: 'Cursor for pagination (ID of the last log from previous page)',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Activity logs retrieved successfully',
  })
  @ApiResponse({
    status: 400,
    description: 'Invalid request parameters',
  })
  async getActivityLogs(
    @Query('organizationId', RequiredUUIDPipe) organizationId: string,
    @Query('actionType') actionType?: string,
    @Query('entityType') entityType?: string,
    @Query('entityId', OptionalUUIDPipe) entityId?: string,
    @Query('userId', OptionalUUIDPipe) userId?: string,
    @Query('limit') limit?: string,
    @Query('cursor', OptionalUUIDPipe) cursor?: string,
    @Request() req?: ExpressRequestType,
  ) {
    // Validate limit
    const limitNum = limit ? parseInt(limit, 10) : 50;
    if (isNaN(limitNum) || limitNum < 1 || limitNum > 100) {
      throw new BadRequestException(
        'Limit must be a number between 1 and 100',
      );
    }

    const logs = await this.activityLogService.getActivityLogs(
      organizationId,
      {
        actionType,
        entityType,
        entityId,
        userId,
      },
      {
        limit: limitNum,
        cursor,
      },
    );

    return createListResponse(logs, 'Activity logs retrieved successfully');
  }
}
