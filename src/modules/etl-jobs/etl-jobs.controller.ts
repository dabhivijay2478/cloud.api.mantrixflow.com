/**
 * ETL Jobs Controller
 * Internal endpoints for pg_cron + callback; pipeline runs via PipelineController
 */

import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  Post,
  Query,
  UseGuards,
} from '@nestjs/common';
import { ApiHeader, ApiOperation, ApiResponse, ApiTags } from '@nestjs/swagger';
import { InternalTokenGuard } from '../../common/guards/internal-token.guard';
import { EtlJobsService } from './etl-jobs.service';
import type { EtlCallbackDto } from './dto';
import { createSuccessResponse } from '../../common/dto/api-response.dto';

@ApiTags('internal')
@Controller('internal')
export class EtlJobsController {
  constructor(private readonly etlJobsService: EtlJobsService) {}

  /**
   * Process ETL queue - called by pg_cron every minute (or NestJS scheduler)
   */
  @Post('process-etl-jobs')
  @HttpCode(HttpStatus.OK)
  @UseGuards(InternalTokenGuard)
  @ApiHeader({ name: 'X-Internal-Token', required: true })
  @ApiOperation({ summary: 'Process ETL queue' })
  @ApiResponse({ status: 200, description: 'Queue processed' })
  @ApiResponse({ status: 401, description: 'Invalid X-Internal-Token' })
  async processEtlJobs(@Query('qty') qty?: string) {
    const processed = await this.etlJobsService.processQueue(
      qty ? parseInt(qty, 10) : 5,
    );
    return createSuccessResponse({ processed }, `Processed ${processed} job(s)`);
  }

  /**
   * Callback from FastAPI when meltano run completes
   */
  @Post('etl-callback')
  @HttpCode(HttpStatus.OK)
  @UseGuards(InternalTokenGuard)
  @ApiHeader({ name: 'X-Internal-Token', required: true })
  @ApiOperation({ summary: 'ETL callback' })
  @ApiResponse({ status: 200, description: 'Callback received' })
  @ApiResponse({ status: 401, description: 'Invalid X-Internal-Token' })
  async etlCallback(@Body() dto: EtlCallbackDto) {
    await this.etlJobsService.handleCallback(dto);
    return createSuccessResponse({ received: true }, 'Callback received');
  }
}
