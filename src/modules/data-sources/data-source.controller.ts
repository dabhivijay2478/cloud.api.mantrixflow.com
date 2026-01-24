/**
 * Data Source Controller (READ-ONLY)
 * REST API endpoints for data source management - GET operations only
 * 
 * Architecture:
 * - NestJS: Provides read-only GET endpoints for listing and retrieving data source metadata
 * - Python FastAPI: Handles all data operations (create, update, delete, test connection, discover schema)
 * 
 * Available Endpoints (GET only):
 * - GET / - List all data sources
 * - GET /types - Get supported data source types
 * - GET /:id - Get data source by ID
 * - GET /:sourceId/connection - Get connection metadata (read-only)
 * 
 * Removed Endpoints (moved to Python FastAPI):
 * - POST / - Create data source
 * - PUT /:id - Update data source
 * - DELETE /:id - Delete data source
 * - POST /test-connection - Test connection config
 * - POST /:sourceId/connection - Create/update connection
 * - POST /:sourceId/test-connection - Test connection
 * - POST /:sourceId/discover-schema - Discover schema
 */

import {
  Controller,
  Delete,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  ParseUUIDPipe,
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
import { OrganizationRoleGuard } from '../../common/guards/organization-role.guard';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import { DataSourceService } from './data-source.service';
import { ConnectionService } from './connection.service';

type ExpressRequestType = ExpressRequest;

@ApiTags('data-sources')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard, OrganizationRoleGuard)
@Controller('organizations/:organizationId/data-sources')
export class DataSourceController {
  constructor(
    private readonly dataSourceService: DataSourceService,
    private readonly connectionService: ConnectionService,
  ) {}

  /**
   * List all data sources for organization
   */
  @Get()
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'List data sources',
    description: 'Get all data sources for an organization',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiQuery({ name: 'sourceType', required: false, description: 'Filter by source type' })
  @ApiQuery({ name: 'isActive', required: false, description: 'Filter by active status' })
  @ApiResponse({ status: 200, description: 'Data sources retrieved successfully' })
  async listDataSources(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Request() req: ExpressRequestType,
    @Query('sourceType') sourceType?: string,
    @Query('isActive') isActive?: string,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    const filters: { sourceType?: string; isActive?: boolean } = {};
    if (sourceType) filters.sourceType = sourceType;
    if (isActive !== undefined) filters.isActive = isActive === 'true';

    const dataSources = await this.dataSourceService.listDataSources(
      organizationId,
      userId,
      filters,
    );

    return createListResponse(dataSources);
  }

  /**
   * Get supported data source types
   */
  @Get('types')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Get supported data source types',
    description: 'Get list of supported data source types',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiResponse({ status: 200, description: 'Supported types retrieved successfully' })
  async getSupportedTypes(@Param('organizationId', ParseUUIDPipe) _organizationId: string) {
    const types = this.dataSourceService.getSupportedTypes();
    return createSuccessResponse({ types });
  }

  /**
   * Get data source by ID
   */
  @Get(':id')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Get data source',
    description: 'Get data source details by ID',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiParam({ name: 'id', type: 'string', description: 'Data source ID' })
  @ApiResponse({ status: 200, description: 'Data source retrieved successfully' })
  async getDataSource(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Param('id', ParseUUIDPipe) id: string,
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    const dataSource = await this.dataSourceService.getDataSourceById(organizationId, id, userId);

    return createSuccessResponse(dataSource);
  }

  /**
   * Get connection for data source
   * NOTE: All connection operations (create, update, test, discover) are handled by Python FastAPI service
   * This endpoint only retrieves existing connection metadata for display
   */
  @Get(':sourceId/connection')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Get connection',
    description: 'Get connection configuration for a data source (sensitive fields masked)',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiParam({ name: 'sourceId', type: 'string', description: 'Data source ID' })
  @ApiQuery({ name: 'includeSensitive', required: false, description: 'Include sensitive fields' })
  @ApiResponse({ status: 200, description: 'Connection retrieved successfully' })
  async getConnection(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Param('sourceId', ParseUUIDPipe) sourceId: string,
    @Request() req: ExpressRequestType,
    @Query('includeSensitive') includeSensitive?: string,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    const connection = await this.connectionService.getConnection(
      organizationId,
      sourceId,
      userId,
      includeSensitive === 'true',
    );

    if (!connection) {
      return createSuccessResponse(null, 'Connection not configured');
    }

    return createSuccessResponse(connection);
  }

  /**
   * Delete connection for data source
   * NOTE: This endpoint is kept for Python service to call back for actual database deletion
   * Frontend should call Python API directly: DELETE /connections/{connection_id}
   */
  @Delete(':sourceId/connection')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Delete connection',
    description: 'Delete connection configuration for a data source (called by Python service)',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiParam({ name: 'sourceId', type: 'string', description: 'Data source ID' })
  @ApiResponse({ status: 200, description: 'Connection deleted successfully' })
  async deleteConnection(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Param('sourceId', ParseUUIDPipe) sourceId: string,
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    await this.connectionService.deleteConnection(organizationId, sourceId, userId);

    return createSuccessResponse({ deletedId: sourceId }, 'Connection deleted successfully');
  }

}
