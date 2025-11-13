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
import { PostgresService } from './postgres.service';
import { PostgresConnectionConfig, ExecuteQuerySchema } from './postgres.types';
import { createErrorResponse } from './utils/error-mapper.util';

// TODO: Create and use actual auth guards
// @UseGuards(JwtAuthGuard, OrgGuard)

@Controller('api/connectors/postgres')
export class PostgresController {
  constructor(private readonly postgresService: PostgresService) {}

  /**
   * Test connection (without saving)
   */
  @Post('test-connection')
  @HttpCode(HttpStatus.OK)
  async testConnection(@Body() config: PostgresConnectionConfig) {
    try {
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
  async createConnection(
    @Body() body: { name: string; config: PostgresConnectionConfig },
    @Request() req: any, // TODO: Use proper request type with user/org
  ) {
    try {
      const orgId = req.user?.orgId || 'default-org-id'; // TODO: Get from auth
      const userId = req.user?.id || 'default-user-id'; // TODO: Get from auth

      return await this.postgresService.createConnection(
        orgId,
        userId,
        body.name,
        body.config,
      );
    } catch (error) {
      const errorResponse = createErrorResponse(error);
      throw errorResponse;
    }
  }

  /**
   * List connections
   */
  @Get('connections')
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
  async executeQuery(
    @Param('id') id: string,
    @Body() body: { query: string; params?: any[]; timeout?: number },
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
  async createSync(
    @Param('id') id: string,
    @Body()
    body: {
      tableName: string;
      schema?: string;
      syncMode: 'full' | 'incremental';
      incrementalColumn?: string;
      customWhereClause?: string;
      syncFrequency?: 'manual' | '15min' | '1hour' | '24hours';
    },
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
