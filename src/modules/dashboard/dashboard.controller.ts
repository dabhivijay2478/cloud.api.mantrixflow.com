/**
 * Dashboard Controller
 * REST API endpoints for dashboard data
 */

import { Controller, Get, Query, Request, UseGuards } from '@nestjs/common';
import type { Request as ExpressRequest } from 'express';
import {
  ApiBearerAuth,
  ApiOperation,
  ApiQuery,
  ApiResponse,
  ApiTags,
} from '@nestjs/swagger';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import { createSuccessResponse } from '../../common/dto/api-response.dto';
import { DashboardService } from './dashboard.service';
import { RequiredUUIDPipe } from '../activity-logs/pipes/required-uuid.pipe';

type ExpressRequestType = ExpressRequest;

@ApiTags('dashboard')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard)
@Controller('dashboard')
export class DashboardController {
  constructor(private readonly dashboardService: DashboardService) {}

  /**
   * Get dashboard overview
   */
  @Get('overview')
  @ApiOperation({
    summary: 'Get dashboard overview',
    description: 'Get aggregated dashboard data for an organization including pipelines, migrations, and activity',
  })
  @ApiQuery({
    name: 'organizationId',
    required: true,
    description: 'Organization ID',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Dashboard overview retrieved successfully',
  })
  async getDashboardOverview(
    @Query('organizationId', RequiredUUIDPipe) organizationId: string,
    @Request() _req: ExpressRequestType,
  ) {
    const overview = await this.dashboardService.getDashboardOverview(organizationId);
    return createSuccessResponse(overview, 'Dashboard overview retrieved successfully');
  }
}
