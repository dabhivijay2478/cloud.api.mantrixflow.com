/**
 * Connectors Controller — Serves connector metadata at /api/connectors/metadata
 * Calls Python ETL (apps/new-etl) for connector list, health, CDC setup.
 * Also provides test-connection (NestJS in-memory, no ETL required).
 */

import {
  BadRequestException,
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  Post,
  UseGuards,
} from '@nestjs/common';
import { Public } from '../../common/decorators/public.decorator';
import { createSuccessResponse } from '../../common/dto/api-response.dto';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import { ConnectionService } from '../data-sources/connection.service';
import { ConnectorMetadataService } from './connector-metadata.service';

@Controller('connectors')
@UseGuards(SupabaseAuthGuard)
export class ConnectorsController {
  constructor(
    private readonly connectorMetadataService: ConnectorMetadataService,
    private readonly connectionService: ConnectionService,
  ) {}

  @Post('test-connection')
  @HttpCode(HttpStatus.OK)
  async testConnection(
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

  @Get('metadata')
  async getMetadata() {
    const data = await this.connectorMetadataService.listConnectors();
    return createSuccessResponse(data, 'Connectors retrieved successfully');
  }

  @Get('health')
  @Public()
  async health() {
    const data = await this.connectorMetadataService.health();
    return createSuccessResponse(data, 'Health check completed');
  }

  @Get(':sourceType/cdc-setup')
  @Public()
  async getCdcSetup(@Param('sourceType') sourceType: string) {
    const data = await this.connectorMetadataService.getCdcSetup(sourceType);
    return createSuccessResponse(data, 'CDC setup retrieved successfully');
  }
}
