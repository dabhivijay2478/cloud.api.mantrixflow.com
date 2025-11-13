/**
 * PostgreSQL Connector Controller
 * REST API endpoints for PostgreSQL connector
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
  UseGuards,
  Request,
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
import { PostgresService } from './postgres.service';
import { PostgresConnectionConfig, ExecuteQuerySchema } from './postgres.types';
import { createErrorResponse } from './utils/error-mapper.util';
import { TestConnectionDto, TestConnectionResponseDto } from './dto/test-connection.dto';
import { CreateConnectionDto, ConnectionResponseDto } from './dto/create-connection.dto';
import { ExecuteQueryDto, QueryExecutionResponseDto } from './dto/execute-query.dto';
import { CreateSyncJobDto, SyncJobResponseDto } from './dto/create-sync-job.dto';

// TODO: Create and use actual auth guards
// @UseGuards(JwtAuthGuard, OrgGuard)

@ApiTags('postgres')
@ApiBearerAuth('JWT-auth')
@Controller('api/connectors/postgres')
export class PostgresController {
  constructor(private readonly postgresService: PostgresService) {}

  /**
   * Test connection (without saving)
   */
  @Post('test-connection')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Test PostgreSQL connection',
    description: 'Test a PostgreSQL connection without saving it. Validates credentials and connectivity.',
  })
  @ApiBody({ type: TestConnectionDto })
  @ApiResponse({
    status: 200,
    description: 'Connection test successful',
    type: TestConnectionResponseDto,
  })
  @ApiResponse({ status: 400, description: 'Invalid connection parameters' })
  async testConnection(@Body() dto: TestConnectionDto) {
    try {
      // Convert DTO to PostgresConnectionConfig
      const config: PostgresConnectionConfig = {
        host: dto.host,
        port: dto.port || 5432,
        database: dto.database,
        username: dto.username,
        password: dto.password,
        ssl: dto.ssl?.enabled
          ? {
              enabled: dto.ssl.enabled,
              caCert: dto.ssl.caCert,
              rejectUnauthorized: dto.ssl.rejectUnauthorized,
            }
          : undefined,
        sshTunnel: dto.sshTunnel?.enabled
          ? {
              enabled: dto.sshTunnel.enabled,
              host: dto.sshTunnel.host,
              port: dto.sshTunnel.port,
              username: dto.sshTunnel.username,
              privateKey: dto.sshTunnel.privateKey,
            }
          : undefined,
        connectionTimeout: dto.connectionTimeout,
        queryTimeout: dto.queryTimeout,
        poolSize: dto.poolSize,
      };
      return await this.postgresService.testConnection(config);
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * Create connection
   */
  @Post('connections')
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: 'Create PostgreSQL connection',
    description: 'Create a new PostgreSQL connection with encrypted credentials.',
  })
  @ApiBody({ type: CreateConnectionDto })
  @ApiResponse({
    status: 201,
    description: 'Connection created successfully',
    type: ConnectionResponseDto,
  })
  @ApiResponse({ status: 400, description: 'Invalid connection data or connection test failed' })
  @ApiResponse({ status: 403, description: 'Maximum connections exceeded' })
  async createConnection(
    @Body() body: CreateConnectionDto,
    @Request() req: any, // TODO: Use proper request type with user/org
  ) {
    try {
      const orgId = req.user?.orgId || 'default-org-id'; // TODO: Get from auth
      const userId = req.user?.id || 'default-user-id'; // TODO: Get from auth

      // Convert DTO to PostgresConnectionConfig
      const config: PostgresConnectionConfig = {
        host: body.config.host,
        port: body.config.port || 5432,
        database: body.config.database,
        username: body.config.username,
        password: body.config.password,
        ssl: body.config.ssl?.enabled
          ? {
              enabled: body.config.ssl.enabled,
              caCert: body.config.ssl.caCert,
              rejectUnauthorized: body.config.ssl.rejectUnauthorized,
            }
          : undefined,
        sshTunnel: body.config.sshTunnel?.enabled
          ? {
              enabled: body.config.sshTunnel.enabled,
              host: body.config.sshTunnel.host,
              port: body.config.sshTunnel.port,
              username: body.config.sshTunnel.username,
              privateKey: body.config.sshTunnel.privateKey,
            }
          : undefined,
        connectionTimeout: body.config.connectionTimeout,
        queryTimeout: body.config.queryTimeout,
        poolSize: body.config.poolSize,
      };

      return await this.postgresService.createConnection(orgId, userId, body.name, config);
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * List connections
   */
  @Get('connections')
  @ApiOperation({
    summary: 'List all connections',
    description: 'Get all PostgreSQL connections for the current organization.',
  })
  @ApiResponse({
    status: 200,
    description: 'List of connections',
    type: [ConnectionResponseDto],
  })
  async listConnections(@Request() req: any) {
    try {
      const orgId = req.user?.orgId || 'default-org-id'; // TODO: Get from auth
      return await this.postgresService.listConnections(orgId);
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * Get connection by ID
   */
  @Get('connections/:id')
  @ApiOperation({
    summary: 'Get connection by ID',
    description: 'Retrieve a specific PostgreSQL connection by its ID.',
  })
  @ApiParam({ name: 'id', description: 'Connection ID', type: String })
  @ApiResponse({
    status: 200,
    description: 'Connection details',
    type: ConnectionResponseDto,
  })
  @ApiResponse({ status: 404, description: 'Connection not found' })
  async getConnection(@Param('id') id: string, @Request() req: any) {
    try {
      const orgId = req.user?.orgId || 'default-org-id'; // TODO: Get from auth
      return await this.postgresService.getConnection(id, orgId);
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * Update connection
   */
  @Patch('connections/:id')
  async updateConnection(
    @Param('id') id: string,
    @Body() updates: Partial<PostgresConnectionConfig>,
    @Request() req: any,
  ) {
    try {
      const orgId = req.user?.orgId || 'default-org-id'; // TODO: Get from auth
      return await this.postgresService.updateConnection(id, orgId, updates);
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * Delete connection
   */
  @Delete('connections/:id')
  @HttpCode(HttpStatus.NO_CONTENT)
  async deleteConnection(@Param('id') id: string, @Request() req: any) {
    try {
      const orgId = req.user?.orgId || 'default-org-id'; // TODO: Get from auth
      await this.postgresService.deleteConnection(id, orgId);
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * Discover databases
   */
  @Get('connections/:id/databases')
  @ApiOperation({
    summary: 'Discover databases',
    description: 'List all databases available in the PostgreSQL connection.',
  })
  @ApiParam({ name: 'id', description: 'Connection ID', type: String })
  @ApiResponse({ status: 200, description: 'List of databases' })
  async getDatabases(@Param('id') id: string, @Request() req: any) {
    try {
      const orgId = req.user?.orgId || 'default-org-id';
      const schema = await this.postgresService.discoverSchema(id, orgId);
      return schema.databases;
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * Discover schemas
   */
  @Get('connections/:id/schemas')
  @ApiOperation({
    summary: 'Discover schemas',
    description: 'List all schemas available in the PostgreSQL connection.',
  })
  @ApiParam({ name: 'id', description: 'Connection ID', type: String })
  @ApiResponse({ status: 200, description: 'List of schemas' })
  async getSchemas(@Param('id') id: string, @Request() req: any) {
    try {
      const orgId = req.user?.orgId || 'default-org-id';
      const schema = await this.postgresService.discoverSchema(id, orgId);
      return schema.schemas;
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * Discover tables
   */
  @Get('connections/:id/tables')
  @ApiOperation({
    summary: 'Discover tables',
    description: 'List all tables in a specific schema.',
  })
  @ApiParam({ name: 'id', description: 'Connection ID', type: String })
  @ApiQuery({ name: 'schema', description: 'Schema name', required: false, example: 'public' })
  @ApiResponse({ status: 200, description: 'List of tables' })
  async getTables(
    @Param('id') id: string,
    @Query('schema') schema: string = 'public',
    @Request() req: any,
  ) {
    try {
      const orgId = req.user?.orgId || 'default-org-id';
      const discovery = await this.postgresService.discoverSchema(id, orgId);
      return discovery.tables.filter((t) => !schema || t.schema === schema);
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * Get table schema
   */
  @Get('connections/:id/tables/:table/schema')
  async getTableSchema(
    @Param('id') id: string,
    @Param('table') table: string,
    @Query('schema') schema: string = 'public',
    @Request() req: any,
  ) {
    try {
      const orgId = req.user?.orgId || 'default-org-id';
      const discovery = await this.postgresService.discoverSchema(id, orgId);
      const tableInfo = discovery.tables.find(
        (t) => t.name === table && t.schema === schema,
      );
      if (!tableInfo) {
        throw new Error('Table not found');
      }
      return tableInfo;
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * Refresh schema cache
   */
  @Post('connections/:id/refresh-schema')
  async refreshSchema(@Param('id') id: string, @Request() req: any) {
    try {
      const orgId = req.user?.orgId || 'default-org-id';
      return await this.postgresService.discoverSchema(id, orgId, true);
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * Execute query
   */
  @Post('connections/:id/query')
  @ApiOperation({
    summary: 'Execute SQL query',
    description: 'Execute a SELECT query against the PostgreSQL database. Only read-only queries are allowed.',
  })
  @ApiParam({ name: 'id', description: 'Connection ID', type: String })
  @ApiBody({ type: ExecuteQueryDto })
  @ApiResponse({
    status: 200,
    description: 'Query executed successfully',
    type: QueryExecutionResponseDto,
  })
  @ApiResponse({ status: 400, description: 'Invalid query or query syntax error' })
  @ApiResponse({ status: 403, description: 'Query contains dangerous keywords or rate limit exceeded' })
  async executeQuery(
    @Param('id') id: string,
    @Body() body: ExecuteQueryDto,
    @Request() req: any,
  ) {
    try {
      const orgId = req.user?.orgId || 'default-org-id';
      const userId = req.user?.id || 'default-user-id';
      return await this.postgresService.executeQuery(
        id,
        orgId,
        userId,
        body.query,
        body.params,
        body.timeout,
      );
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * Explain query
   */
  @Post('connections/:id/query/explain')
  async explainQuery(
    @Param('id') id: string,
    @Body() body: { query: string; params?: any[] },
    @Request() req: any,
  ) {
    try {
      const orgId = req.user?.orgId || 'default-org-id';
      return await this.postgresService.explainQuery(
        id,
        orgId,
        body.query,
        body.params,
      );
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * Create sync job
   */
  @Post('connections/:id/sync')
  @ApiOperation({
    summary: 'Create sync job',
    description: 'Create a new data synchronization job to sync PostgreSQL table data to Supabase.',
  })
  @ApiParam({ name: 'id', description: 'Connection ID', type: String })
  @ApiBody({ type: CreateSyncJobDto })
  @ApiResponse({
    status: 201,
    description: 'Sync job created successfully',
    type: SyncJobResponseDto,
  })
  @ApiResponse({ status: 400, description: 'Invalid sync configuration' })
  async createSync(
    @Param('id') id: string,
    @Body() body: CreateSyncJobDto,
    @Request() req: any,
  ) {
    try {
      const orgId = req.user?.orgId || 'default-org-id';
      return await this.postgresService.createSyncJob(
        id,
        orgId,
        body.tableName,
        body.schema || 'public',
        body.syncMode,
        body.incrementalColumn,
        body.customWhereClause,
        body.syncFrequency || 'manual',
      );
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * Get sync jobs
   */
  @Get('connections/:id/sync-jobs')
  async getSyncJobs(@Param('id') id: string, @Request() req: any) {
    try {
      const orgId = req.user?.orgId || 'default-org-id';
      return await this.postgresService.getSyncJobs(id, orgId);
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * Get sync job by ID
   */
  @Get('connections/:id/sync-jobs/:jobId')
  async getSyncJob(
    @Param('id') id: string,
    @Param('jobId') jobId: string,
    @Request() req: any,
  ) {
    try {
      const orgId = req.user?.orgId || 'default-org-id';
      return await this.postgresService.getSyncJob(id, jobId, orgId);
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * Cancel sync job
   */
  @Post('connections/:id/sync-jobs/:jobId/cancel')
  async cancelSyncJob(
    @Param('id') id: string,
    @Param('jobId') jobId: string,
    @Request() req: any,
  ) {
    try {
      const orgId = req.user?.orgId || 'default-org-id';
      await this.postgresService.cancelSyncJob(id, jobId, orgId);
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * Update sync schedule
   */
  @Patch('connections/:id/sync-jobs/:jobId/schedule')
  async updateSyncSchedule(
    @Param('id') id: string,
    @Param('jobId') jobId: string,
    @Body() body: { syncFrequency: 'manual' | '15min' | '1hour' | '24hours' },
    @Request() req: any,
  ) {
    try {
      const orgId = req.user?.orgId || 'default-org-id';
      // TODO: Implement schedule update
      // This would update the sync job's frequency and nextSyncAt
      return { message: 'Schedule updated' };
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * Get connection health
   */
  @Get('connections/:id/health')
  async getHealth(@Param('id') id: string, @Request() req: any) {
    try {
      const orgId = req.user?.orgId || 'default-org-id';
      return await this.postgresService.getConnectionHealth(id, orgId);
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * Get query logs
   */
  @Get('connections/:id/query-logs')
  async getQueryLogs(
    @Param('id') id: string,
    @Query('limit') limit: string = '100',
    @Query('offset') offset: string = '0',
    @Request() req: any,
  ) {
    try {
      const orgId = req.user?.orgId || 'default-org-id';
      return await this.postgresService.getQueryLogs(
        id,
        orgId,
        parseInt(limit),
        parseInt(offset),
      );
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * Get connection metrics
   */
  @Get('connections/:id/metrics')
  async getMetrics(@Param('id') id: string, @Request() req: any) {
    try {
      const orgId = req.user?.orgId || 'default-org-id';
      return await this.postgresService.getConnectionMetrics(id, orgId);
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }
}
