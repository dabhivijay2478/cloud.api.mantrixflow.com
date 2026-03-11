/**
 * Explorer Controller
 * Streams table data for SQLRooms in-browser DuckDB loading.
 * Also supports executing arbitrary SQL against the remote database (JOINs, etc.).
 */

import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Param,
  ParseUUIDPipe,
  Post,
  Query,
  Request,
  Res,
  UseGuards,
} from '@nestjs/common';
import { ApiBearerAuth, ApiOperation, ApiParam, ApiQuery, ApiResponse, ApiTags } from '@nestjs/swagger';
import type { Request as ExpressRequest, Response } from 'express';
import { OrganizationRoleGuard, RequireRole } from '../../../common/guards/organization-role.guard';
import { SupabaseAuthGuard } from '../../../common/guards/supabase-auth.guard';
import { ExplorerDataService } from './explorer-data.service';

type ExpressRequestType = ExpressRequest;

@ApiTags('data-sources')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard, OrganizationRoleGuard)
@RequireRole('OWNER', 'ADMIN', 'EDITOR')
@Controller('organizations/:organizationId/data-sources')
export class ExplorerController {
  constructor(private readonly explorerDataService: ExplorerDataService) {}

  /**
   * Stream table rows as JSONL for SQLRooms Explorer
   */
  @Get(':sourceId/explorer/data')
  @ApiOperation({
    summary: 'Stream explorer data',
    description: 'Stream table rows as newline-delimited JSON for in-browser DuckDB loading',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiParam({ name: 'sourceId', type: 'string', description: 'Data source ID' })
  @ApiQuery({ name: 'schema', required: false, description: 'Schema name (default: public)' })
  @ApiQuery({ name: 'table', required: true, description: 'Table name' })
  @ApiQuery({ name: 'limit', required: false, description: 'Row limit (default 10000, max 100000)' })
  @ApiResponse({ status: 200, description: 'JSONL stream of rows' })
  async streamExplorerData(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Param('sourceId', ParseUUIDPipe) sourceId: string,
    @Query('schema') schema: string,
    @Query('table') table: string,
    @Query('limit') limit: string,
    @Request() req: ExpressRequestType,
    @Res() res: Response,
  ): Promise<void> {
    const userId = req.user?.id;
    if (!userId) {
      res.status(401).json({ error: 'User not authenticated' });
      return;
    }

    if (!table?.trim()) {
      res.status(400).json({ error: 'table is required' });
      return;
    }

    try {
      const limitNum = limit ? parseInt(limit, 10) : 10_000;
      const { adapter, params } = await this.explorerDataService.prepareStreamParams(
        organizationId,
        sourceId,
        schema || 'public',
        table,
        limitNum,
        userId,
      );

      res.setHeader('Content-Type', 'application/x-ndjson');
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('X-Accel-Buffering', 'no');
      if (typeof res.flushHeaders === 'function') res.flushHeaders();

      const stream = adapter.streamRows(params);
      for await (const row of stream) {
        res.write(JSON.stringify(row) + '\n');
      }
    } catch (err) {
      if (!res.headersSent) {
        const message = err instanceof Error ? err.message : String(err);
        res.status(400).json({ error: message });
        return;
      }
      const message = err instanceof Error ? err.message : String(err);
      res.write(JSON.stringify({ error: message }) + '\n');
    } finally {
      if (!res.writableEnded) {
        res.end();
      }
    }
  }

  /**
   * Execute arbitrary SQL against the remote database.
   * Supports JOINs, subqueries, etc. - like Snowflake/Redshift SQL editors.
   */
  @Post(':sourceId/explorer/execute-query')
  @ApiOperation({
    summary: 'Execute SQL query',
    description:
      'Execute arbitrary SQL against the remote database. Supports JOINs, subqueries, and full SQL. Runs on the server.',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization ID' })
  @ApiParam({ name: 'sourceId', type: 'string', description: 'Data source ID' })
  @ApiResponse({ status: 200, description: 'Query results' })
  async executeQuery(
    @Param('organizationId', ParseUUIDPipe) organizationId: string,
    @Param('sourceId', ParseUUIDPipe) sourceId: string,
    @Request() req: ExpressRequestType,
    @Body() body: { query: string; maxRows?: number; timeoutMs?: number },
  ) {
    const userId = req.user?.id;
    if (!userId) {
      return { error: 'User not authenticated' };
    }

    const query = body?.query;
    if (!query || typeof query !== 'string') {
      return { error: 'query is required' };
    }

    const maxRows = Math.min(
      Math.max(Number(body?.maxRows) || 10000, 1),
      100000,
    );
    const timeoutMs = Math.min(
      Math.max(Number(body?.timeoutMs) || 60000, 5000),
      300000,
    );

    try {
      const result = await this.explorerDataService.executeQuery(
        organizationId,
        sourceId,
        query,
        userId,
        maxRows,
        timeoutMs,
      );
      return result;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      throw new BadRequestException(message);
    }
  }
}
