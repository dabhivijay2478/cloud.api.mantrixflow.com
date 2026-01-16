/**
 * Data Source Controller
 * REST API endpoints for data source management
 */

import {
  Body,
  Controller,
  Delete,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  ParseUUIDPipe,
  Post,
  Put,
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
import { OrganizationRoleGuard, RequireRole } from '../../common/guards/organization-role.guard';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import {
  DataSourceService,
  type CreateDataSourceDto,
  type UpdateDataSourceDto,
} from './data-source.service';
import { ConnectionService, type CreateConnectionDto } from './connection.service';

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
   * Create new data source
   */
  @Post()
  @RequireRole('OWNER', 'ADMIN', 'EDITOR')
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: 'Create data source',
    description: 'Create a new data source for the organization',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiResponse({ status: 201, description: 'Data source created successfully' })
  async createDataSource(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Body() dto: CreateDataSourceDto,
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    const dataSource = await this.dataSourceService.createDataSource(organizationId, userId, dto);

    return createSuccessResponse(dataSource, 'Data source created successfully');
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
   * Update data source
   */
  @Put(':id')
  @RequireRole('OWNER', 'ADMIN', 'EDITOR')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Update data source',
    description: 'Update data source details',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiParam({ name: 'id', type: 'string', description: 'Data source ID' })
  @ApiResponse({ status: 200, description: 'Data source updated successfully' })
  async updateDataSource(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Param('id', ParseUUIDPipe) id: string,
    @Body() dto: UpdateDataSourceDto,
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    const dataSource = await this.dataSourceService.updateDataSource(
      organizationId,
      id,
      userId,
      dto,
    );

    return createSuccessResponse(dataSource, 'Data source updated successfully');
  }

  /**
   * Delete data source
   */
  @Delete(':id')
  @RequireRole('OWNER', 'ADMIN', 'EDITOR')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Delete data source',
    description: 'Soft delete a data source',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiParam({ name: 'id', type: 'string', description: 'Data source ID' })
  @ApiResponse({ status: 200, description: 'Data source deleted successfully' })
  async deleteDataSource(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Param('id', ParseUUIDPipe) id: string,
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    await this.dataSourceService.deleteDataSource(organizationId, id, userId);

    return createDeleteResponse('Data source deleted successfully');
  }

  /**
   * Create or update connection for data source
   */
  @Post(':sourceId/connection')
  @RequireRole('OWNER', 'ADMIN', 'EDITOR')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Configure connection',
    description: 'Create or update connection configuration for a data source',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiParam({ name: 'sourceId', type: 'string', description: 'Data source ID' })
  @ApiResponse({ status: 200, description: 'Connection configured successfully' })
  async createOrUpdateConnection(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Param('sourceId', ParseUUIDPipe) sourceId: string,
    @Body() dto: CreateConnectionDto,
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    const connection = await this.connectionService.createOrUpdateConnection(
      organizationId,
      sourceId,
      userId,
      dto,
    );

    return createSuccessResponse(connection, 'Connection configured successfully');
  }

  /**
   * Get connection for data source
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
    @Query('includeSensitive') includeSensitive?: string,
    @Request() req: ExpressRequestType,
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
   * Test connection
   */
  @Post(':sourceId/test-connection')
  @RequireRole('OWNER', 'ADMIN', 'EDITOR')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Test connection',
    description: 'Test the connection configuration for a data source',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiParam({ name: 'sourceId', type: 'string', description: 'Data source ID' })
  @ApiResponse({ status: 200, description: 'Connection test completed' })
  async testConnection(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Param('sourceId', ParseUUIDPipe) sourceId: string,
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    const result = await this.connectionService.testConnection(organizationId, sourceId, userId);

    return createSuccessResponse(result);
  }

  /**
   * Discover schema
   */
  @Post(':sourceId/discover-schema')
  @RequireRole('OWNER', 'ADMIN', 'EDITOR')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Discover schema',
    description: 'Discover database schema for a data source',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiParam({ name: 'sourceId', type: 'string', description: 'Data source ID' })
  @ApiResponse({ status: 200, description: 'Schema discovered successfully' })
  async discoverSchema(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Param('sourceId', ParseUUIDPipe) sourceId: string,
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    const schema = await this.connectionService.discoverSchema(organizationId, sourceId, userId);

    return createSuccessResponse(schema, 'Schema discovered successfully');
  }
}
