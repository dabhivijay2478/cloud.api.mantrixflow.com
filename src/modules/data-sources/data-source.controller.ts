/**
 * Data Source Controller
 * REST API endpoints for data source management
 *
 * Architecture:
 * - NestJS: Handles create/list/get/delete of data sources
 * - Python FastAPI: Handles connection operations (create, update, test), collector/emitter/transformations
 *
 * Available Endpoints:
 * - POST / - Create data source (NestJS)
 * - GET / - List all data sources (NestJS)
 * - GET /types - Get supported data source types (NestJS)
 * - GET /:id - Get data source by ID (NestJS)
 * - DELETE /:id - Delete data source (NestJS)
 * - GET /:sourceId/connection - Get connection metadata (read-only, NestJS)
 *
 * Python FastAPI Endpoints:
 * - POST /connections - Create/update connection
 * - POST /test-connection - Test connection config
 * - Collector/Emitter/Transformations endpoints
 */

import {
  BadRequestException,
  Body,
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
import {
  ApiBearerAuth,
  ApiOperation,
  ApiParam,
  ApiQuery,
  ApiResponse,
  ApiTags,
} from '@nestjs/swagger';
import type { Request as ExpressRequest } from 'express';
import {
  createDeleteResponse,
  createListResponse,
  createSuccessResponse,
} from '../../common/dto/api-response.dto';
import { OrganizationRoleGuard } from '../../common/guards/organization-role.guard';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import { ConnectorMetadataService } from '../connectors/connector-metadata.service';
import { resolveSourceConnectorType } from '../connectors/utils/connector-resolver';
import { CdcVerifyService } from './cdc-verify.service';
import {
  ConnectionService,
  type CreateConnectionDto,
  type CreateDataSourceWithConnectionDto,
} from './connection.service';
import { type CreateDataSourceDto, DataSourceService } from './data-source.service';

type ExpressRequestType = ExpressRequest;

@ApiTags('data-sources')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard, OrganizationRoleGuard)
@Controller('organizations/:organizationId/data-sources')
export class DataSourceController {
  constructor(
    private readonly dataSourceService: DataSourceService,
    private readonly connectionService: ConnectionService,
    private readonly connectorMetadataService: ConnectorMetadataService,
    private readonly cdcVerifyService: CdcVerifyService,
  ) {}

  /**
   * Create a data source
   */
  @Post()
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: 'Create data source',
    description: 'Create a new data source for an organization',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiResponse({ status: 201, description: 'Data source created successfully' })
  async createDataSource(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Request() req: ExpressRequestType,
    @Body() body: Record<string, unknown>,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    const sourceType = (body.sourceType || body.source_type) as string | undefined;
    if (!sourceType || typeof sourceType !== 'string') {
      throw new BadRequestException('sourceType (or source_type) is required');
    }

    const name = body.name as string | undefined;
    if (!name || typeof name !== 'string') {
      throw new BadRequestException('name is required');
    }

    const connectorRole = (body.connectorRole || body.connector_role) as
      | 'source'
      | 'destination'
      | undefined;
    const dto: CreateDataSourceDto = {
      name,
      description: (body.description as string | undefined) || undefined,
      sourceType,
      connectorRole: connectorRole === 'destination' ? 'destination' : undefined,
      metadata:
        body.metadata && typeof body.metadata === 'object'
          ? (body.metadata as Record<string, unknown>)
          : undefined,
    };

    const dataSource = await this.dataSourceService.createDataSource(organizationId, userId, dto);
    return createSuccessResponse(dataSource, 'Data source created successfully');
  }

  /**
   * Create data source and connection atomically (prevents orphaned data sources)
   */
  @Post('with-connection')
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: 'Create data source with connection',
    description:
      'Create a data source and its connection in a single transaction. Use this when adding a new connector to avoid orphaned records on failure.',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiResponse({ status: 201, description: 'Data source and connection created successfully' })
  async createDataSourceWithConnection(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Request() req: ExpressRequestType,
    @Body() body: Record<string, unknown>,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    const name = body.name as string | undefined;
    if (!name || typeof name !== 'string') {
      throw new BadRequestException('name is required');
    }

    const connectionType = (body.connectionType || body.connection_type) as string | undefined;
    if (!connectionType || typeof connectionType !== 'string') {
      throw new BadRequestException('connectionType (or connection_type) is required');
    }

    const config = body.config as Record<string, unknown> | undefined;
    if (!config || typeof config !== 'object') {
      throw new BadRequestException('config is required');
    }

    const connectorRole = (body.connectorRole || body.connector_role) as
      | 'source'
      | 'destination'
      | undefined;

    const dto: CreateDataSourceWithConnectionDto = {
      name: name.trim(),
      connectionType,
      connectorRole: connectorRole === 'destination' ? 'destination' : undefined,
      config: config as Record<string, any>,
    };

    const result = await this.connectionService.createDataSourceWithConnection(
      organizationId,
      userId,
      dto,
    );

    return createSuccessResponse(
      {
        id: result.dataSource.id,
        name: result.dataSource.name,
        connection: result.connection,
      },
      'Data source and connection created successfully',
    );
  }

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
    @Query('limit') limit?: string,
    @Query('offset') offset?: string,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    const filters: { sourceType?: string; isActive?: boolean } = {};
    if (sourceType) filters.sourceType = sourceType;
    if (isActive !== undefined) filters.isActive = isActive === 'true';

    const limitNum = Math.min(Math.max(parseInt(limit || '20', 10) || 20, 1), 100);
    const offsetNum = Math.max(parseInt(offset || '0', 10) || 0, 0);

    const result = await this.dataSourceService.listDataSourcesPaginated(
      organizationId,
      userId,
      filters,
      limitNum,
      offsetNum,
    );

    return createListResponse(result.data, undefined, {
      total: result.total,
      limit: limitNum,
      offset: offsetNum,
      hasMore: offsetNum + limitNum < result.total,
    });
  }

  /**
   * Get available connectors (sources + destinations) from ETL registry
   */
  @Get('connectors')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'List connectors',
    description: 'Get available source and destination connectors from ETL registry',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiResponse({ status: 200, description: 'Connectors retrieved successfully' })
  async listConnectors(@Param('organizationId', ParseUUIDPipe) _organizationId: string) {
    const data = await this.connectorMetadataService.listConnectors();
    return createSuccessResponse(data);
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
   * Test connection config (pre-save, ad-hoc)
   * Uses NestJS in-memory connection test - no Python ETL required
   */
  @Post('test-connection')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Test connection config',
    description: 'Test connection configuration before saving (PostgreSQL only)',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiResponse({ status: 200, description: 'Connection test result' })
  async testConnectionConfig(
    @Param('organizationId', ParseUUIDPipe) _organizationId: string,
    @Body() body: {
      connectionType?: string;
      connection_type?: string;
      config?: Record<string, unknown>;
    },
  ) {
    const connectionType = (body.connectionType ?? body.connection_type) as string;
    const config = body.config as Record<string, unknown>;
    if (!connectionType || typeof connectionType !== 'string') {
      throw new BadRequestException('connectionType (or connection_type) is required');
    }
    if (!config || typeof config !== 'object') {
      throw new BadRequestException('config is required');
    }
    const result = await this.connectionService.testConnectionConfig(
      connectionType,
      config as Record<string, any>,
    );
    return createSuccessResponse(result);
  }

  /**
   * Create or update connection for data source
   * NestJS owns credential encryption before persistence.
   */
  @Post(':sourceId/connection')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Create or update connection',
    description: 'Create or update encrypted connection configuration for a data source',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiParam({ name: 'sourceId', type: 'string', description: 'Data source ID' })
  @ApiResponse({ status: 200, description: 'Connection saved successfully' })
  async createOrUpdateConnection(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Param('sourceId', ParseUUIDPipe) sourceId: string,
    @Request() req: ExpressRequestType,
    @Body() body: Record<string, unknown>,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    const connectionType = (body.connectionType || body.connection_type) as string | undefined;
    if (!connectionType || typeof connectionType !== 'string') {
      throw new BadRequestException('connectionType (or connection_type) is required');
    }

    const config = body.config as Record<string, unknown> | undefined;
    if (!config || typeof config !== 'object') {
      throw new BadRequestException('config is required');
    }

    const dto: CreateConnectionDto = {
      connectionType,
      config: config as Record<string, any>,
    };

    const connection = await this.connectionService.createOrUpdateConnection(
      organizationId,
      sourceId,
      userId,
      dto,
    );

    return createSuccessResponse(connection, 'Connection saved successfully');
  }

  /**
   * Get connection for data source
   * Returns connection metadata/config from NestJS storage.
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
   * Discover full schema (columns, primary_keys, streams) from data source
   * Used by Add Collector / pipeline flow. Proxies to ETL discover-schema.
   */
  @Post(':sourceId/discover-schema')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Discover schema',
    description: 'Discover tables, columns, primary keys from data source (full response)',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiParam({ name: 'sourceId', type: 'string', description: 'Data source ID' })
  @ApiResponse({ status: 200, description: 'Schema discovered successfully' })
  async discoverSchema(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Param('sourceId', ParseUUIDPipe) sourceId: string,
    @Request() req: ExpressRequestType,
    @Body() body: Record<string, unknown>,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    const dataSource = await this.dataSourceService.getDataSourceById(
      organizationId,
      sourceId,
      userId,
    );
    const sourceConfig = await this.connectionService.getDecryptedConnection(
      organizationId,
      sourceId,
      userId,
    );
    if (!sourceConfig || typeof sourceConfig !== 'object') {
      throw new BadRequestException(
        'Connection not configured for this data source. Please add connection credentials first.',
      );
    }

    const sourceType = resolveSourceConnectorType(dataSource.sourceType).registryType;
    const schemaName = (body.schema_name ?? body.schemaName ?? 'public') as string;
    const tableName = (body.table_name ?? body.tableName) as string | undefined;
    const query = body.query as string | undefined;

    const result = await this.connectorMetadataService.discoverFull({
      source_type: sourceType,
      source_config: sourceConfig,
      schema_name: schemaName,
      table_name: tableName,
      query,
    });

    return createSuccessResponse(result);
  }

  /**
   * Preview data from data source using ETL (Airbyte)
   * Discovers streams and returns sample rows for the selected stream
   */
  @Post(':sourceId/preview')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Preview data',
    description: 'Preview sample data from a data source using ETL/Airbyte',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiParam({ name: 'sourceId', type: 'string', description: 'Data source ID' })
  @ApiResponse({ status: 200, description: 'Preview data retrieved successfully' })
  async previewData(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Param('sourceId', ParseUUIDPipe) sourceId: string,
    @Request() req: ExpressRequestType,
    @Body() body: Record<string, unknown>,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    const dataSource = await this.dataSourceService.getDataSourceById(
      organizationId,
      sourceId,
      userId,
    );
    const sourceConfig = await this.connectionService.getDecryptedConnection(
      organizationId,
      sourceId,
      userId,
    );

    const sourceType = resolveSourceConnectorType(dataSource.sourceType).registryType;
    const sourceStream = body.source_stream as string | undefined;
    const limit = Math.min(Math.max(Number(body.limit) || 50, 1), 100);

    let streamToPreview = sourceStream;
    if (!streamToPreview) {
      const discoverResult = await this.connectorMetadataService.discover({
        source_type: sourceType,
        source_config: sourceConfig,
      });
      const streams = discoverResult?.streams ?? [];
      streamToPreview = streams[0]?.name;
      if (!streamToPreview) {
        throw new BadRequestException('No streams found. Ensure the source has data.');
      }
    }

    const previewResult = await this.connectorMetadataService.preview({
      source_type: sourceType,
      source_config: sourceConfig,
      source_stream: streamToPreview,
      limit,
    });

    return createSuccessResponse({
      stream: previewResult.stream ?? streamToPreview,
      records: previewResult.records ?? [],
      columns: previewResult.columns ?? [],
      total: previewResult.total ?? 0,
      ...(previewResult.warning && { warning: previewResult.warning }),
    });
  }

  /**
   * Test connection to data source before discover
   */
  @Post(':sourceId/test-connection')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Test connection',
    description: 'Test connectivity to data source before running discover',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiParam({ name: 'sourceId', type: 'string', description: 'Data source ID' })
  @ApiResponse({ status: 200, description: 'Connection test result' })
  async testConnection(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Param('sourceId', ParseUUIDPipe) sourceId: string,
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    const dataSource = await this.dataSourceService.getDataSourceById(
      organizationId,
      sourceId,
      userId,
    );
    const sourceConfig = await this.connectionService.getDecryptedConnection(
      organizationId,
      sourceId,
      userId,
    );
    const sourceType = resolveSourceConnectorType(dataSource.sourceType).registryType;

    const result = await this.connectorMetadataService.testConnection({
      source_type: sourceType,
      source_config: sourceConfig,
    });

    return createSuccessResponse({
      success: result.success,
      message: result.success ? 'Connection successful' : result.error,
    });
  }

  /**
   * Discover streams from data source using ETL (Airbyte)
   */
  @Post(':sourceId/discover')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Discover streams',
    description: 'Discover available streams/tables from a data source using ETL/Airbyte',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiParam({ name: 'sourceId', type: 'string', description: 'Data source ID' })
  @ApiResponse({ status: 200, description: 'Streams discovered successfully' })
  async discoverStreams(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Param('sourceId', ParseUUIDPipe) sourceId: string,
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    const dataSource = await this.dataSourceService.getDataSourceById(
      organizationId,
      sourceId,
      userId,
    );
    const sourceConfig = await this.connectionService.getDecryptedConnection(
      organizationId,
      sourceId,
      userId,
    );
    const sourceType = resolveSourceConnectorType(dataSource.sourceType).registryType;

    const discoverResult = await this.connectorMetadataService.discover({
      source_type: sourceType,
      source_config: sourceConfig,
    });

    return createSuccessResponse({
      streams: discoverResult.streams ?? [],
    });
  }

  /**
   * Get CDC status and available providers for a data source
   */
  @Get(':sourceId/cdc-status')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Get CDC status',
    description: 'Get CDC prerequisites status and available providers for log-based sync setup',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiParam({ name: 'sourceId', type: 'string', description: 'Data source ID' })
  @ApiResponse({ status: 200, description: 'CDC status retrieved successfully' })
  async getCdcStatus(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Param('sourceId', ParseUUIDPipe) sourceId: string,
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    const result = await this.cdcVerifyService.getCdcStatus(organizationId, sourceId, userId);

    return createSuccessResponse(result);
  }

  /**
   * Verify a single CDC prerequisite step
   */
  @Post(':sourceId/cdc-verify')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Verify CDC step',
    description:
      'Verify a single CDC prerequisite step (wal_level, wal2json, replication_role, replication_test)',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiParam({ name: 'sourceId', type: 'string', description: 'Data source ID' })
  @ApiResponse({ status: 200, description: 'CDC step verification result' })
  async cdcVerify(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Param('sourceId', ParseUUIDPipe) sourceId: string,
    @Request() req: ExpressRequestType,
    @Body() body: { step?: string; provider_selected?: string },
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    const step = body.step ?? 'wal_level';
    if (!['wal_level', 'wal2json', 'replication_role', 'replication_test'].includes(step)) {
      throw new BadRequestException(
        'step must be one of: wal_level, wal2json, replication_role, replication_test',
      );
    }

    const result = await this.cdcVerifyService.verifyStep(
      organizationId,
      sourceId,
      userId,
      step,
      body.provider_selected,
    );

    return createSuccessResponse(result);
  }

  /**
   * Verify all CDC prerequisite steps
   */
  @Post(':sourceId/cdc-verify-all')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Verify all CDC steps',
    description: 'Run all CDC prerequisite verification steps for the data source',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiParam({ name: 'sourceId', type: 'string', description: 'Data source ID' })
  @ApiResponse({ status: 200, description: 'CDC verification result' })
  async cdcVerifyAll(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Param('sourceId', ParseUUIDPipe) sourceId: string,
    @Request() req: ExpressRequestType,
    @Body() body: { provider_selected?: string },
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    const result = await this.cdcVerifyService.verifyAll(
      organizationId,
      sourceId,
      userId,
      body.provider_selected,
    );

    return createSuccessResponse(result);
  }

  /**
   * Delete connection for data source
   * Connections are managed by NestJS only. Frontend calls this endpoint.
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

  /**
   * Get data source by ID
   * IMPORTANT: This must come AFTER :sourceId/* routes to avoid route conflicts.
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
   * Delete data source (soft delete)
   * NestJS handles data source deletion
   * IMPORTANT: This must come AFTER :sourceId/* routes to avoid route conflicts.
   */
  @Delete(':id')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Delete data source',
    description: 'Soft delete a data source (NestJS handles deletion)',
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

    return createDeleteResponse(id, 'Data source deleted successfully');
  }
}
