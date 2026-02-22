/**
 * Connectors Controller — Serves connector metadata at /api/connectors/metadata
 *
 * Used by TanStack Devtools and other clients that expect this path.
 * Proxies to ETL connector list.
 */

import { Controller, Get, UseGuards } from '@nestjs/common';
import { EtlService } from '../etl/etl.service';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';

@Controller('connectors')
@UseGuards(SupabaseAuthGuard)
export class ConnectorsController {
  constructor(private readonly etlService: EtlService) {}

  @Get('metadata')
  getMetadata() {
    return this.etlService.listConnectors();
  }
}
