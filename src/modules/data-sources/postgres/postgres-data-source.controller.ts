/**
 * PostgreSQL Connector Controller
 * REST API endpoints for PostgreSQL connector
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
  NotFoundException,
  Param,
  Patch,
  Post,
  Query,
  Request,
  UseGuards,
} from '@nestjs/common';
// Type declarations are imported via tsconfig
import type { Request as ExpressRequest } from 'express';

type ExpressRequestType = ExpressRequest;

import {
  ApiBearerAuth,
  ApiBody,
  ApiOperation,
  ApiParam,
  ApiQuery,
  ApiResponse,
  ApiTags,
} from '@nestjs/swagger';
import {
  ApiDeleteResponse,
  ApiListResponse,
  ApiSuccessResponse,
  createDeleteResponse,
  createListResponse,
  createSuccessResponse,
} from '../../../common/dto/api-response.dto';
import { SupabaseAuthGuard } from '../../../common/guards/supabase-auth.guard';
import { CreateConnectionDto } from './dto/create-connection.dto';
import { CreateSyncJobDto, SyncJobResponseDto } from './dto/create-sync-job.dto';
import { ExecuteQueryDto, QueryExecutionResponseDto } from './dto/execute-query.dto';
import { TestConnectionDto, TestConnectionResponseDto } from './dto/test-connection.dto';
import { UpdateConnectionDto } from './dto/update-connection.dto';
import type { PostgresConnectionConfig, TableInfo } from './postgres.types';
import type { PostgresDataSourceService } from './postgres-data-source.service';
import { parsePostgresConnectionString } from './utils/connection-string-parser.util';
import { createErrorResponse } from './utils/error-mapper.util';

@ApiTags('data-sources')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard)
@Controller('api/data-sources/postgres')
export class PostgresDataSourceController {
  constructor(private readonly postgresDataSourceService: PostgresDataSourceService) {}

  /**
   * Test connection (without saving)
   */
  @Post('test-connection')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Test PostgreSQL connection',
    description:
      'Test a PostgreSQL connection without saving it. Validates credentials and connectivity.',
  })
  @ApiBody({ type: TestConnectionDto })
  @ApiResponse({
    status: 200,
    description: 'Connection test successful',
    type: TestConnectionResponseDto,
  })
  @ApiResponse({
    status: 400,
    description: 'Invalid connection parameters',
  })
  async testConnection(@Body() dto: TestConnectionDto, @Request() _req: ExpressRequestType) {
    try {
      // Parse connection string if provided, otherwise use individual fields
      let baseConfig: Partial<PostgresConnectionConfig> = {};

      if (dto.connectionString) {
        try {
          const parsed = parsePostgresConnectionString(dto.connectionString);
          baseConfig = {
            host: parsed.host,
            port: parsed.port,
            database: parsed.database,
            username: parsed.username,
            password: parsed.password,
            ssl: parsed.ssl,
          };
        } catch (error) {
          throw new BadRequestException(
            `Invalid connection string: ${error instanceof Error ? error.message : 'Unknown error'}`,
          );
        }
      }

      // Individual fields override connection string values
      // Validate that we have required fields
      const host = dto.host || baseConfig.host;
      const database = dto.database || baseConfig.database;
      const username = dto.username || baseConfig.username;
      const password = dto.password || baseConfig.password;

      if (!host || !database || !username || !password) {
        throw new BadRequestException(
          'Either connectionString or all individual fields (host, database, username, password) must be provided',
        );
      }

      // Detect Neon and Supabase connections using databaseType or hostname
      const databaseType =
        dto.databaseType ||
        (host.includes('.neon.tech')
          ? 'neon'
          : host.includes('supabase.co') || host.includes('supabase.com')
            ? 'supabase'
            : 'other');
      const isNeon = databaseType === 'neon' || host.includes('.neon.tech');
      const isSupabase =
        databaseType === 'supabase' ||
        host.includes('supabase.co') ||
        host.includes('supabase.com');

      // Don't auto-enable SSL for localhost/127.0.0.1 (development)
      const isLocalhost = host === 'localhost' || host === '127.0.0.1' || host.startsWith('127.');

      // Auto-enable SSL for Neon and Supabase if not explicitly disabled and not localhost
      const shouldEnableSSL = !isLocalhost && (isNeon || isSupabase || dto.ssl?.enabled === true);

      // For localhost with SSL, don't reject unauthorized certificates
      const shouldRejectUnauthorized = !isLocalhost && dto.ssl?.rejectUnauthorized !== false;

      // For Neon databases, extract endpoint ID and add to options
      // Only add Neon options if it's actually a Neon host (not localhost)
      let neonOptions: string | undefined;
      if (isNeon && !isLocalhost && !baseConfig.options) {
        const endpointId = host.split('.')[0];
        neonOptions = `endpoint%3D${encodeURIComponent(endpointId)}`;
      }

      // Convert DTO to PostgresConnectionConfig
      const config: PostgresConnectionConfig = {
        host,
        port: dto.port || baseConfig.port || 5432,
        database,
        username,
        password,
        ssl: shouldEnableSSL
          ? {
              enabled: true,
              caCert: dto.ssl?.caCert,
              rejectUnauthorized: shouldRejectUnauthorized,
            }
          : dto.ssl?.enabled !== undefined
            ? {
                enabled: dto.ssl.enabled,
                caCert: dto.ssl.caCert,
                rejectUnauthorized: isLocalhost ? false : dto.ssl.rejectUnauthorized !== false,
              }
            : baseConfig.ssl,
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
        options: neonOptions || baseConfig.options, // Preserve options from connection string or add Neon endpoint ID
      };
      const testResult = await this.postgresDataSourceService.testConnection(config);
      return createSuccessResponse(
        testResult,
        testResult.success ? 'Connection test successful' : 'Connection test failed',
        testResult.success ? HttpStatus.OK : HttpStatus.BAD_REQUEST,
        {
          success: testResult.success,
          responseTimeMs: testResult.responseTimeMs,
        },
      );
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
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
    type: ApiSuccessResponse,
  })
  @ApiResponse({
    status: 400,
    description: 'Invalid connection data or connection test failed',
  })
  @ApiResponse({
    status: 403,
    description: 'Maximum connections exceeded',
  })
  @ApiQuery({
    name: 'orgId',
    description: 'Organization ID',
    required: false,
    type: String,
  })
  async createConnection(
    @Body() body: CreateConnectionDto,
    @Request() req: ExpressRequestType,
    @Query('orgId') orgIdParam?: string,
  ) {
    try {
      // Debug logging - check what we received
      console.log('[createConnection] Query orgId param:', orgIdParam);
      console.log('[createConnection] Query orgId param type:', typeof orgIdParam);
      console.log('[createConnection] User orgId from auth:', req.user?.orgId);
      console.log('[createConnection] Full request query:', JSON.stringify(req.query));
      console.log('[createConnection] Request URL:', req.url);

      // Extract orgId from query parameter first, then from auth - REQUIRED
      // Query parameter MUST take absolute precedence - this is the selected organization from UI
      let orgId: string | undefined;

      // STRICT: Query parameter takes absolute precedence
      if (orgIdParam) {
        const trimmed = orgIdParam.trim();
        if (trimmed.length > 0) {
          orgId = trimmed;
          console.log(
            '[createConnection] ✅ Using orgId from query parameter (UI selection):',
            orgId,
          );
        } else {
          console.warn('[createConnection] ⚠️ Query parameter orgId is empty string');
        }
      } else {
        console.warn('[createConnection] ⚠️ No orgId query parameter provided');
      }

      // Fallback to user auth orgId ONLY if query parameter was not provided
      if (!orgId && req.user?.orgId) {
        orgId = req.user.orgId;
        console.log('[createConnection] ⚠️ Falling back to orgId from user auth:', orgId);
      }

      if (!orgId) {
        console.error(
          '[createConnection] ❌ No orgId available - query param:',
          orgIdParam,
          'user orgId:',
          req.user?.orgId,
        );
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter (?orgId=...) or ensure you are authenticated with an organization.',
        );
      }

      const userId = req.user?.id;
      if (!userId) {
        throw new BadRequestException('User ID is required. Please ensure you are authenticated.');
      }

      // Debug logging - final values being used
      console.log('[createConnection] Final orgId being used:', orgId);
      console.log('[createConnection] Final userId being used:', userId);
      console.log('[createConnection] orgId source:', orgIdParam ? 'query parameter' : 'user auth');

      // Parse connection string if provided, otherwise use individual fields
      let baseConfig: Partial<PostgresConnectionConfig> = {};

      if (body.config.connectionString) {
        try {
          const parsed = parsePostgresConnectionString(body.config.connectionString);
          baseConfig = {
            host: parsed.host,
            port: parsed.port,
            database: parsed.database,
            username: parsed.username,
            password: parsed.password,
            ssl: parsed.ssl,
          };
        } catch (error) {
          throw new BadRequestException(
            `Invalid connection string: ${error instanceof Error ? error.message : 'Unknown error'}`,
          );
        }
      }

      // Individual fields override connection string values
      // Validate that we have required fields (either from connection string or individual fields)
      const host = body.config.host || baseConfig.host;
      const database = body.config.database || baseConfig.database;
      const username = body.config.username || baseConfig.username;
      const password = body.config.password || baseConfig.password;

      if (!host || !database || !username || !password) {
        throw new BadRequestException(
          'Either connectionString or all individual fields (host, database, username, password) must be provided',
        );
      }

      // Detect Neon and Supabase connections using databaseType or hostname
      const databaseType =
        body.config.databaseType ||
        (host.includes('.neon.tech')
          ? 'neon'
          : host.includes('supabase.co') || host.includes('supabase.com')
            ? 'supabase'
            : 'other');
      const isNeon = databaseType === 'neon' || host.includes('.neon.tech');
      const isSupabase =
        databaseType === 'supabase' ||
        host.includes('supabase.co') ||
        host.includes('supabase.com');

      // Don't auto-enable SSL for localhost/127.0.0.1 (development)
      const isLocalhost = host === 'localhost' || host === '127.0.0.1' || host.startsWith('127.');

      // Auto-enable SSL for Neon and Supabase if not explicitly disabled and not localhost
      const shouldEnableSSL =
        !isLocalhost && (isNeon || isSupabase || body.config.ssl?.enabled === true);

      // For localhost with SSL, don't reject unauthorized certificates
      const shouldRejectUnauthorized =
        !isLocalhost && body.config.ssl?.rejectUnauthorized !== false;

      // For Neon databases, extract endpoint ID and add to options
      // Only add Neon options if it's actually a Neon host (not localhost)
      let neonOptions: string | undefined;
      if (isNeon && !isLocalhost && !baseConfig.options) {
        const endpointId = host.split('.')[0];
        neonOptions = `endpoint%3D${encodeURIComponent(endpointId)}`;
      }

      // Convert DTO to PostgresConnectionConfig
      const config: PostgresConnectionConfig = {
        host,
        port: body.config.port || baseConfig.port || 5432,
        database,
        username,
        password,
        ssl: shouldEnableSSL
          ? {
              enabled: true,
              caCert: body.config.ssl?.caCert,
              rejectUnauthorized: shouldRejectUnauthorized,
            }
          : body.config.ssl?.enabled !== undefined
            ? {
                enabled: body.config.ssl.enabled,
                caCert: body.config.ssl.caCert,
                rejectUnauthorized: isLocalhost
                  ? false
                  : body.config.ssl.rejectUnauthorized !== false,
              }
            : baseConfig.ssl,
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
        options: neonOptions || baseConfig.options, // Preserve options from connection string or add Neon endpoint ID
      };

      const connection = await this.postgresDataSourceService.createConnection(
        orgId,
        userId,
        body.name,
        config,
      );
      return createSuccessResponse(
        connection,
        'Connection created successfully',
        HttpStatus.CREATED,
        {
          connectionId: connection.id,
          connectionName: connection.name,
        },
      );
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * List connections
   */
  @Get('connections')
  @ApiOperation({
    summary: 'List all connections',
    description:
      'Get all PostgreSQL connections for the specified organization. Organization ID can be passed as a query parameter or will be extracted from the authenticated user.',
  })
  @ApiQuery({
    name: 'orgId',
    required: false,
    description:
      "Organization ID (UUID v4). If not provided, will use the authenticated user's organization ID.",
    example: '123e4567-e89b-12d3-a456-426614174000',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'List of connections for the organization',
    type: ApiListResponse,
  })
  @ApiResponse({
    status: 400,
    description: 'Invalid organization ID format',
  })
  async listConnections(@Request() req: ExpressRequestType, @Query('orgId') orgId?: string) {
    try {
      // Use query parameter if provided, otherwise try to get from auth, otherwise throw error
      const finalOrgId = orgId || req?.user?.orgId;
      if (!finalOrgId) {
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter or ensure you are authenticated.',
        );
      }

      // Debug logging
      console.log('[listConnections] Query orgId:', orgId);
      console.log('[listConnections] User orgId:', req?.user?.orgId);
      console.log('[listConnections] Final orgId:', finalOrgId);

      const connections = await this.postgresDataSourceService.listConnections(finalOrgId);

      console.log('[listConnections] Found connections:', connections.length);

      return createListResponse(connections, `Found ${connections.length} connection(s)`, {
        total: connections.length,
        limit: connections.length,
        offset: 0,
        hasMore: false,
      });
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Get connection by ID
   */
  @Get('connections/:id')
  @ApiOperation({
    summary: 'Get connection by ID',
    description:
      'Retrieve a specific PostgreSQL connection by its ID. Organization ID can be passed as a query parameter for filtering.',
  })
  @ApiParam({
    name: 'id',
    description: 'Connection ID (UUID v4)',
    type: String,
    example: '123e4567-e89b-12d3-a456-426614174000',
  })
  @ApiQuery({
    name: 'orgId',
    required: false,
    description:
      'Organization ID (UUID v4). Optional filter to ensure the connection belongs to the specified organization.',
    example: '123e4567-e89b-12d3-a456-426614174000',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Connection details',
    type: ApiSuccessResponse,
  })
  @ApiResponse({
    status: 400,
    description: 'Invalid connection ID or organization ID format',
  })
  @ApiResponse({
    status: 404,
    description: 'Connection not found',
  })
  async getConnection(
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
    @Query('orgId') orgId?: string,
  ) {
    try {
      // Use query parameter if provided, otherwise try to get from auth
      const finalOrgId = orgId || req?.user?.orgId;
      const connection = await this.postgresDataSourceService.getConnection(id, finalOrgId);
      return createSuccessResponse(connection, 'Connection retrieved successfully', HttpStatus.OK, {
        connectionId: connection.id,
        connectionName: connection.name,
        connectionStatus: connection.status,
      });
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Update connection
   */
  @Patch('connections/:id')
  @ApiOperation({
    summary: 'Update connection',
    description:
      'Update an existing PostgreSQL connection. Only provided fields will be updated. Organization ID is required to verify ownership.',
  })
  @ApiParam({
    name: 'id',
    description: 'Connection ID (UUID v4)',
    type: String,
    example: '123e4567-e89b-12d3-a456-426614174000',
  })
  @ApiQuery({
    name: 'orgId',
    required: true,
    description:
      'Organization ID (UUID v4). Required to verify the connection belongs to your organization.',
    example: '123e4567-e89b-12d3-a456-426614174000',
    type: String,
  })
  @ApiBody({
    type: UpdateConnectionDto,
    description: 'Connection update data. Only include fields you want to update.',
  })
  @ApiResponse({
    status: 200,
    description: 'Connection updated successfully',
    type: ApiSuccessResponse,
  })
  @ApiResponse({
    status: 400,
    description: 'Invalid connection data, organization ID, or connection test failed',
  })
  @ApiResponse({
    status: 404,
    description: 'Connection not found',
  })
  async updateConnection(
    @Param('id') id: string,
    @Query('orgId') orgId: string,
    @Body() updates: UpdateConnectionDto,
    @Request() req: ExpressRequestType,
  ) {
    try {
      // Use query parameter if provided, otherwise try to get from auth
      const finalOrgId = orgId || req?.user?.orgId;
      if (!finalOrgId) {
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter.',
        );
      }
      const updated = await this.postgresDataSourceService.updateConnection(
        id,
        finalOrgId,
        updates as Partial<PostgresConnectionConfig> & Record<string, unknown>,
      );
      return createSuccessResponse(updated, 'Connection updated successfully', HttpStatus.OK, {
        connectionId: updated.id,
        connectionName: updated.name,
        updatedFields: Object.keys(updates),
      });
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Delete connection
   */
  @Delete('connections/:id')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Delete connection',
    description: 'Delete a PostgreSQL connection. Organization ID is required to verify ownership.',
  })
  @ApiParam({
    name: 'id',
    description: 'Connection ID (UUID v4)',
    type: String,
    example: '123e4567-e89b-12d3-a456-426614174000',
  })
  @ApiQuery({
    name: 'orgId',
    required: true,
    description:
      'Organization ID (UUID v4). Required to verify the connection belongs to your organization.',
    example: '123e4567-e89b-12d3-a456-426614174000',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Connection deleted successfully',
    type: ApiDeleteResponse,
  })
  @ApiResponse({
    status: 400,
    description: 'Invalid connection ID or organization ID format',
  })
  @ApiResponse({
    status: 404,
    description: 'Connection not found',
  })
  async deleteConnection(
    @Param('id') id: string,
    @Query('orgId') orgId: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      // Use query parameter if provided, otherwise try to get from auth
      const finalOrgId = orgId || req?.user?.orgId;
      if (!finalOrgId) {
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter.',
        );
      }
      await this.postgresDataSourceService.deleteConnection(id, finalOrgId);
      return createDeleteResponse(id, 'Connection deleted successfully');
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
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
  @ApiParam({
    name: 'id',
    description: 'Connection ID (UUID v4)',
    type: String,
  })
  @ApiQuery({
    name: 'orgId',
    required: false,
    description: 'Organization ID (UUID v4)',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'List of databases',
  })
  async getDatabases(
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
    @Query('orgId') orgId?: string,
  ) {
    try {
      const finalOrgId = orgId || req?.user?.orgId;
      if (!finalOrgId) {
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter or ensure you are authenticated with an organization.',
        );
      }
      const schema = await this.postgresDataSourceService.discoverSchema(id, finalOrgId);
      return createSuccessResponse(
        schema.databases,
        `Found ${schema.databases.length} database(s)`,
        HttpStatus.OK,
        {
          connectionId: id,
          totalDatabases: schema.databases.length,
        },
      );
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Discover schemas with tables
   */
  @Get('connections/:id/schemas')
  @ApiOperation({
    summary: 'Discover schemas with tables',
    description:
      'List all schemas available in the PostgreSQL connection, including tables for each schema.',
  })
  @ApiParam({
    name: 'id',
    description: 'Connection ID (UUID v4)',
    type: String,
  })
  @ApiQuery({
    name: 'orgId',
    required: false,
    description: 'Organization ID (UUID v4)',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'List of schemas with their tables',
  })
  async getSchemas(
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
    @Query('orgId') orgId?: string,
  ) {
    try {
      const finalOrgId = orgId || req?.user?.orgId;
      if (!finalOrgId) {
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter or ensure you are authenticated with an organization.',
        );
      }
      const schemasWithTables = await this.postgresDataSourceService.discoverSchemasWithTables(
        id,
        finalOrgId,
      );

      // Calculate total tables across all schemas
      const totalTables = schemasWithTables.reduce(
        (sum, schema) => sum + (schema.tables?.length || 0),
        0,
      );

      return createSuccessResponse(
        schemasWithTables,
        `Found ${schemasWithTables.length} schema(s) with ${totalTables} table(s)`,
        HttpStatus.OK,
        {
          connectionId: id,
          totalSchemas: schemasWithTables.length,
          totalTables,
        },
      );
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
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
  @ApiParam({
    name: 'id',
    description: 'Connection ID (UUID v4)',
    type: String,
  })
  @ApiQuery({
    name: 'schema',
    description: 'Schema name',
    required: false,
    example: 'public',
  })
  @ApiQuery({
    name: 'orgId',
    required: false,
    description: 'Organization ID (UUID v4)',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'List of tables',
  })
  @ApiResponse({
    status: 404,
    description: 'Schema not found',
  })
  async getTables(
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
    @Query('schema') schema?: string,
    @Query('orgId') orgId?: string,
  ) {
    try {
      // Extract orgId from query parameter first, then from auth - REQUIRED
      const finalOrgId = orgId || req?.user?.orgId;
      if (!finalOrgId) {
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter or ensure you are authenticated with an organization.',
        );
      }

      // If schema is not provided, return all tables from all schemas
      let tables: TableInfo[];
      if (!schema || schema.trim().length === 0) {
        // Get all schemas with their tables
        const schemasWithTables = await this.postgresDataSourceService.discoverSchemasWithTables(
          id,
          finalOrgId,
        );
        // Flatten all tables from all schemas
        tables = schemasWithTables.flatMap((s) => s.tables || []);
      } else {
        // Get tables from specific schema
        tables = await this.postgresDataSourceService.discoverTablesForSchema(
          id,
          finalOrgId,
          schema,
        );
      }

      return createListResponse(
        tables,
        `Found ${tables.length} table(s)${schema ? ` in schema "${schema}"` : ' across all schemas'}`,
        {
          total: tables.length,
          limit: tables.length,
          offset: 0,
          hasMore: false,
        },
      );
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Get table schema
   */
  @Get('connections/:id/tables/:table/schema')
  @ApiOperation({
    summary: 'Get table schema',
    description: 'Get detailed schema information for a specific table.',
  })
  @ApiParam({
    name: 'id',
    description: 'Connection ID (UUID v4)',
    type: String,
  })
  @ApiParam({
    name: 'table',
    description: 'Table name',
    type: String,
  })
  @ApiQuery({
    name: 'schema',
    description: 'Schema name',
    required: false,
    example: 'public',
  })
  @ApiQuery({
    name: 'orgId',
    required: false,
    description: 'Organization ID (UUID v4)',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Table schema details',
  })
  @ApiResponse({
    status: 404,
    description: 'Table not found',
  })
  async getTableSchema(
    @Param('id') id: string,
    @Param('table') table: string,
    @Request() req: ExpressRequestType,
    @Query('schema') schema: string = 'public',
    @Query('orgId') orgId?: string,
  ) {
    try {
      const finalOrgId = orgId || req?.user?.orgId;
      if (!finalOrgId) {
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter or ensure you are authenticated with an organization.',
        );
      }
      const discovery = await this.postgresDataSourceService.discoverSchema(id, finalOrgId);
      const tableInfo = discovery.tables.find((t) => t.name === table && t.schema === schema);
      if (!tableInfo) {
        throw new NotFoundException(`Table "${table}" not found in schema "${schema}"`);
      }
      return createSuccessResponse(
        tableInfo,
        `Table "${table}" schema retrieved successfully`,
        HttpStatus.OK,
        {
          connectionId: id,
          tableName: table,
          schemaName: schema,
        },
      );
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Refresh schema cache
   */
  @Post('connections/:id/refresh-schema')
  @ApiOperation({
    summary: 'Refresh schema cache',
    description:
      'Force refresh the schema cache for a connection. This will re-discover all databases, schemas, and tables.',
  })
  @ApiParam({
    name: 'id',
    description: 'Connection ID (UUID v4)',
    type: String,
  })
  @ApiQuery({
    name: 'orgId',
    required: false,
    description: 'Organization ID (UUID v4)',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Schema cache refreshed successfully',
  })
  async refreshSchema(
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
    @Query('orgId') orgId?: string,
  ) {
    try {
      const finalOrgId = orgId || req?.user?.orgId;
      if (!finalOrgId) {
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter or ensure you are authenticated with an organization.',
        );
      }
      const schema = await this.postgresDataSourceService.discoverSchema(id, finalOrgId, true);
      return createSuccessResponse(schema, 'Schema cache refreshed successfully', HttpStatus.OK, {
        connectionId: id,
        totalDatabases: schema.databases.length,
        totalSchemas: schema.schemas.length,
        totalTables: schema.tables.length,
      });
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Execute query
   */
  @Post('connections/:id/query')
  @ApiOperation({
    summary: 'Execute SQL query',
    description:
      'Execute a SELECT query against the PostgreSQL database. Only read-only queries are allowed.',
  })
  @ApiParam({
    name: 'id',
    description: 'Connection ID (UUID v4)',
    type: String,
  })
  @ApiQuery({
    name: 'orgId',
    required: false,
    description: 'Organization ID (UUID v4)',
    type: String,
  })
  @ApiBody({ type: ExecuteQueryDto })
  @ApiResponse({
    status: 200,
    description: 'Query executed successfully',
    type: QueryExecutionResponseDto,
  })
  @ApiResponse({
    status: 400,
    description: 'Invalid query or query syntax error',
  })
  @ApiResponse({
    status: 403,
    description: 'Query contains dangerous keywords or rate limit exceeded',
  })
  async executeQuery(
    @Param('id') id: string,
    @Body() body: ExecuteQueryDto,
    @Request() req: ExpressRequestType,
    @Query('orgId') orgId?: string,
  ) {
    try {
      const finalOrgId = orgId || req?.user?.orgId;
      if (!finalOrgId) {
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter or ensure you are authenticated with an organization.',
        );
      }
      const userId = req?.user?.id;
      if (!userId) {
        throw new BadRequestException('User ID is required. Please ensure you are authenticated.');
      }
      const result = await this.postgresDataSourceService.executeQuery(
        id,
        finalOrgId,
        userId,
        body.query,
        body.params,
        body.timeout,
      );
      return createSuccessResponse(result, 'Query executed successfully', HttpStatus.OK, {
        connectionId: id,
        rowsReturned: result.rowCount,
        executionTimeMs: result.executionTimeMs,
      });
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Explain query
   */
  @Post('connections/:id/query/explain')
  @ApiOperation({
    summary: 'Explain query execution plan',
    description:
      'Get the execution plan for a SQL query without executing it. Useful for query optimization.',
  })
  @ApiParam({
    name: 'id',
    description: 'Connection ID (UUID v4)',
    type: String,
  })
  @ApiQuery({
    name: 'orgId',
    required: false,
    description: 'Organization ID (UUID v4)',
    type: String,
  })
  @ApiBody({
    schema: {
      type: 'object',
      properties: {
        query: {
          type: 'string',
          description: 'SQL query to explain',
        },
        params: {
          type: 'array',
          items: { type: 'any' },
          description: 'Query parameters',
        },
      },
      required: ['query'],
    },
  })
  @ApiResponse({
    status: 200,
    description: 'Query execution plan',
  })
  async explainQuery(
    @Param('id') id: string,
    @Body() body: { query: string; params?: unknown[] },
    @Request() req: ExpressRequestType,
    @Query('orgId') orgId?: string,
  ) {
    try {
      const finalOrgId = orgId || req?.user?.orgId;
      if (!finalOrgId) {
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter or ensure you are authenticated with an organization.',
        );
      }
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      const plan = await this.postgresDataSourceService.explainQuery(
        id,
        finalOrgId,
        body.query,
        body.params,
      );
      return createSuccessResponse(
        plan,
        'Query execution plan generated successfully',
        HttpStatus.OK,
        {
          connectionId: id,
        },
      );
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
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
  @ApiParam({
    name: 'id',
    description: 'Connection ID (UUID v4)',
    type: String,
  })
  @ApiQuery({
    name: 'orgId',
    required: false,
    description: 'Organization ID (UUID v4)',
    type: String,
  })
  @ApiBody({ type: CreateSyncJobDto })
  @ApiResponse({
    status: 201,
    description: 'Sync job created successfully',
    type: SyncJobResponseDto,
  })
  @ApiResponse({
    status: 400,
    description: 'Invalid sync configuration',
  })
  async createSync(
    @Param('id') id: string,
    @Body() body: CreateSyncJobDto,
    @Request() req: ExpressRequestType,
    @Query('orgId') orgId?: string,
  ) {
    try {
      const finalOrgId = orgId || req?.user?.orgId;
      if (!finalOrgId) {
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter or ensure you are authenticated with an organization.',
        );
      }
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      const syncJob = await this.postgresDataSourceService.createSyncJob(
        id,
        finalOrgId,
        body.tableName,
        body.schema || 'public',
        body.syncMode,
        body.incrementalColumn,
        body.customWhereClause,
        body.syncFrequency || 'manual',
      );
      return createSuccessResponse(syncJob, 'Sync job created successfully', HttpStatus.CREATED, {
        connectionId: id,
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
        jobId: syncJob.id,
        tableName: body.tableName,
        syncMode: body.syncMode,
      });
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Get sync jobs
   */
  @Get('connections/:id/sync-jobs')
  @ApiOperation({
    summary: 'List sync jobs',
    description: 'Get all synchronization jobs for a connection.',
  })
  @ApiParam({
    name: 'id',
    description: 'Connection ID (UUID v4)',
    type: String,
  })
  @ApiQuery({
    name: 'orgId',
    required: false,
    description: 'Organization ID (UUID v4)',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'List of sync jobs',
  })
  async getSyncJobs(
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
    @Query('orgId') orgId?: string,
  ) {
    try {
      const finalOrgId = orgId || req?.user?.orgId;
      if (!finalOrgId) {
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter or ensure you are authenticated with an organization.',
        );
      }
      const syncJobs = await this.postgresDataSourceService.getSyncJobs(id, finalOrgId);
      return createListResponse(syncJobs, `Found ${syncJobs.length} sync job(s)`, {
        total: syncJobs.length,
        limit: syncJobs.length,
        offset: 0,
        hasMore: false,
      });
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Get sync job by ID
   */
  @Get('connections/:id/sync-jobs/:jobId')
  @ApiOperation({
    summary: 'Get sync job by ID',
    description: 'Retrieve details of a specific synchronization job.',
  })
  @ApiParam({
    name: 'id',
    description: 'Connection ID (UUID v4)',
    type: String,
  })
  @ApiParam({
    name: 'jobId',
    description: 'Sync job ID (UUID v4)',
    type: String,
  })
  @ApiQuery({
    name: 'orgId',
    required: false,
    description: 'Organization ID (UUID v4)',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Sync job details',
  })
  @ApiResponse({
    status: 404,
    description: 'Sync job not found',
  })
  async getSyncJob(
    @Param('id') id: string,
    @Param('jobId') jobId: string,
    @Request() req: ExpressRequestType,
    @Query('orgId') orgId?: string,
  ) {
    try {
      const finalOrgId = orgId || req?.user?.orgId;
      if (!finalOrgId) {
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter or ensure you are authenticated with an organization.',
        );
      }
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      const syncJob = await this.postgresDataSourceService.getSyncJob(id, jobId, finalOrgId);
      return createSuccessResponse(syncJob, 'Sync job retrieved successfully', HttpStatus.OK, {
        connectionId: id,
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
        jobId: syncJob.id,
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
        jobStatus: syncJob.status,
      });
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Cancel sync job
   */
  @Post('connections/:id/sync-jobs/:jobId/cancel')
  @ApiOperation({
    summary: 'Cancel sync job',
    description: 'Cancel a running or pending synchronization job.',
  })
  @ApiParam({
    name: 'id',
    description: 'Connection ID (UUID v4)',
    type: String,
  })
  @ApiParam({
    name: 'jobId',
    description: 'Sync job ID (UUID v4)',
    type: String,
  })
  @ApiQuery({
    name: 'orgId',
    required: false,
    description: 'Organization ID (UUID v4)',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Sync job cancelled successfully',
  })
  @ApiResponse({
    status: 404,
    description: 'Sync job not found',
  })
  async cancelSyncJob(
    @Param('id') id: string,
    @Param('jobId') jobId: string,
    @Request() req: ExpressRequestType,
    @Query('orgId') orgId?: string,
  ) {
    try {
      const finalOrgId = orgId || req?.user?.orgId;
      if (!finalOrgId) {
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter or ensure you are authenticated with an organization.',
        );
      }
      await this.postgresDataSourceService.cancelSyncJob(id, jobId, finalOrgId);
      return createSuccessResponse(
        { jobId, connectionId: id },
        'Sync job cancelled successfully',
        HttpStatus.OK,
        {
          connectionId: id,
          jobId,
        },
      );
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Update sync schedule
   */
  @Patch('connections/:id/sync-jobs/:jobId/schedule')
  @ApiOperation({
    summary: 'Update sync schedule',
    description:
      "Update the synchronization frequency for a sync job. This will update the sync job's frequency and nextSyncAt.",
  })
  @ApiParam({
    name: 'id',
    description: 'Connection ID (UUID v4)',
    type: String,
  })
  @ApiParam({
    name: 'jobId',
    description: 'Sync job ID (UUID v4)',
    type: String,
  })
  @ApiQuery({
    name: 'orgId',
    required: false,
    description: 'Organization ID (UUID v4)',
    type: String,
  })
  @ApiBody({
    schema: {
      type: 'object',
      properties: {
        syncFrequency: {
          type: 'string',
          enum: ['manual', '15min', '1hour', '24hours'],
          description: 'Sync frequency',
        },
      },
      required: ['syncFrequency'],
    },
  })
  @ApiResponse({
    status: 200,
    description: 'Schedule updated successfully',
  })
  updateSyncSchedule(
    @Param('id') id: string,
    @Param('jobId') jobId: string,
    @Body() body: { syncFrequency: 'manual' | '15min' | '1hour' | '24hours' },
  ) {
    try {
      // TODO: Implement schedule update
      // This would update the sync job's frequency and nextSyncAt
      return createSuccessResponse(
        {
          connectionId: id,
          jobId,
          syncFrequency: body.syncFrequency,
        },
        'Schedule updated successfully',
        HttpStatus.OK,
        {
          connectionId: id,
          jobId,
        },
      );
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Get connection health
   */
  @Get('connections/:id/health')
  @ApiOperation({
    summary: 'Get connection health',
    description:
      'Check the health status of a PostgreSQL connection, including connectivity and pool statistics.',
  })
  @ApiParam({
    name: 'id',
    description: 'Connection ID (UUID v4)',
    type: String,
  })
  @ApiQuery({
    name: 'orgId',
    required: false,
    description: 'Organization ID (UUID v4)',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Connection health status',
  })
  async getHealth(
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
    @Query('orgId') orgId?: string,
  ) {
    try {
      const finalOrgId = orgId || req?.user?.orgId;
      if (!finalOrgId) {
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter or ensure you are authenticated with an organization.',
        );
      }
      const health = await this.postgresDataSourceService.getConnectionHealth(id, finalOrgId);
      return createSuccessResponse(
        health,
        'Connection health retrieved successfully',
        HttpStatus.OK,
        {
          connectionId: id,
          status: health.status,
        },
      );
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Get query logs
   */
  @Get('connections/:id/query-logs')
  @ApiOperation({
    summary: 'Get query logs',
    description: 'Retrieve query execution logs for a connection. Supports pagination.',
  })
  @ApiParam({
    name: 'id',
    description: 'Connection ID (UUID v4)',
    type: String,
  })
  @ApiQuery({
    name: 'limit',
    description: 'Maximum number of logs to return',
    required: false,
    example: '100',
    type: String,
  })
  @ApiQuery({
    name: 'offset',
    description: 'Number of logs to skip',
    required: false,
    example: '0',
    type: String,
  })
  @ApiQuery({
    name: 'orgId',
    required: false,
    description: 'Organization ID (UUID v4)',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'List of query logs',
  })
  async getQueryLogs(
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
    @Query('limit') limit: string = '100',
    @Query('offset') offset: string = '0',
    @Query('orgId') orgId?: string,
  ) {
    try {
      const finalOrgId = orgId || req?.user?.orgId;
      if (!finalOrgId) {
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter or ensure you are authenticated with an organization.',
        );
      }
      const logs = await this.postgresDataSourceService.getQueryLogs(
        id,
        finalOrgId,
        parseInt(limit, 10),
        parseInt(offset, 10),
      );
      return createListResponse(logs, `Found ${logs.length} query log(s)`, {
        total: logs.length,
        limit: parseInt(limit, 10),
        offset: parseInt(offset, 10),
        hasMore: logs.length === parseInt(limit, 10),
      });
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }

  /**
   * Get connection metrics
   */
  @Get('connections/:id/metrics')
  @ApiOperation({
    summary: 'Get connection metrics',
    description:
      'Get performance metrics and statistics for a connection, including query counts, execution times, and pool utilization.',
  })
  @ApiParam({
    name: 'id',
    description: 'Connection ID (UUID v4)',
    type: String,
  })
  @ApiQuery({
    name: 'orgId',
    required: false,
    description: 'Organization ID (UUID v4)',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Connection metrics',
  })
  async getMetrics(
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
    @Query('orgId') orgId?: string,
  ) {
    try {
      const finalOrgId = orgId || req?.user?.orgId;
      if (!finalOrgId) {
        throw new BadRequestException(
          'Organization ID is required. Please provide orgId as a query parameter or ensure you are authenticated with an organization.',
        );
      }
      const metrics = await this.postgresDataSourceService.getConnectionMetrics(id, finalOrgId);
      return createSuccessResponse(
        metrics,
        'Connection metrics retrieved successfully',
        HttpStatus.OK,
        {
          connectionId: id,
          totalQueries: metrics.totalQueries,
        },
      );
    } catch (error) {
      // If it's already a NestJS exception, re-throw it
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof HttpException
      ) {
        throw error;
      }
      const errorResponse = createErrorResponse(error);
      throw new HttpException(errorResponse.error, errorResponse.statusCode);
    }
  }
}
