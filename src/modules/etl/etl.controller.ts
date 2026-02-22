/**
 * ETL Controller — Proxies ETL operations to the new PyAirbyte ETL Server
 *
 * Connections CRUD and sync state are handled by NestJS (not proxied).
 */

import { Controller, Post, Get, Body, Param, UseGuards } from '@nestjs/common';
import { EtlService } from './etl.service';
import { Public } from '../../common/decorators/public.decorator';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';

@Controller('etl')
@UseGuards(SupabaseAuthGuard)
export class EtlController {
  constructor(private readonly etlService: EtlService) {}

  @Get('health')
  @Public()
  health() {
    return this.etlService.health();
  }

  @Get('connectors')
  @Public()
  listConnectors() {
    return this.etlService.listConnectors();
  }

  @Get('connectors/:sourceType/cdc-setup')
  @Public()
  getCdcSetup(@Param('sourceType') sourceType: string) {
    return this.etlService.getCdcSetup(sourceType);
  }

  @Post('test-connection')
  testConnection(@Body() body: object) {
    return this.etlService.testConnection(body);
  }

  @Post('discover')
  discover(@Body() body: object) {
    return this.etlService.discover(body);
  }

  @Post('preview')
  preview(@Body() body: object) {
    return this.etlService.preview(body);
  }

  @Post('collect')
  collect(@Body() body: object) {
    return this.etlService.collect(body);
  }

  @Post('emit')
  emit(@Body() body: object) {
    return this.etlService.emit(body);
  }

  @Post('transform')
  transform(@Body() body: object) {
    return this.etlService.transform(body);
  }
}
