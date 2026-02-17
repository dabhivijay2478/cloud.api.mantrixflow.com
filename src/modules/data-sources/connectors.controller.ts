/**
 * Connectors Controller
 * Metadata-driven connector configuration for dynamic UI forms.
 * GET /connectors/metadata returns field schemas per source type.
 */

import { Controller, Get, HttpCode, HttpStatus, UseGuards } from '@nestjs/common';
import { ApiBearerAuth, ApiOperation, ApiResponse, ApiTags } from '@nestjs/swagger';
import { createSuccessResponse } from '../../common/dto/api-response.dto';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import { CONNECTOR_METADATA } from './connector-metadata';

@ApiTags('connectors')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard)
@Controller('connectors')
export class ConnectorsController {
  /**
   * Get connector metadata for all supported source types.
   * Frontend uses this to build connection forms dynamically.
   * Adding a new connector = new entry in CONNECTOR_METADATA + plugin in meltano.yml.
   */
  @Get('metadata')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Get connector metadata',
    description:
      'Get required/optional fields and UI schema for each connector type. Used to build connection forms dynamically.',
  })
  @ApiResponse({ status: 200, description: 'Connector metadata retrieved successfully' })
  getMetadata() {
    return createSuccessResponse({ connectors: CONNECTOR_METADATA });
  }
}
