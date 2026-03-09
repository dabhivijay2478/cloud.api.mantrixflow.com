/**
 * Explorer Data Service
 * Orchestrates streaming of table rows for the SQLRooms in-browser Explorer.
 * Uses ExplorerAdapterRegistry for multi-RDBMS support.
 */

import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { resolveSourceConnectorType } from '../../connectors/utils/connector-resolver';
import { ConnectionService } from '../connection.service';
import { DataSourceService } from '../data-source.service';
import { ExplorerAdapterRegistry } from './explorer-adapter.registry';
import type {
  ExecuteQueryResult,
  IExplorerDbAdapter,
} from './adapters/explorer-db.adapter.interface';

const DEFAULT_LIMIT = 10_000;
const MAX_LIMIT = 100_000;

export interface ExplorerStreamParams {
  config: Record<string, unknown>;
  schema: string;
  table: string;
  limit: number;
}

@Injectable()
export class ExplorerDataService {
  private readonly logger = new Logger(ExplorerDataService.name);

  constructor(
    private readonly dataSourceService: DataSourceService,
    private readonly connectionService: ConnectionService,
    private readonly adapterRegistry: ExplorerAdapterRegistry,
  ) {}

  /**
   * Prepare stream params (validate and resolve config).
   * Call this before streaming to fail fast with proper HTTP errors.
   */
  async prepareStreamParams(
    organizationId: string,
    sourceId: string,
    schema: string,
    table: string,
    limit: number,
    userId: string,
  ): Promise<{ adapter: IExplorerDbAdapter; params: ExplorerStreamParams }> {
    if (!table?.trim()) {
      throw new BadRequestException('table is required');
    }

    const effectiveLimit = Math.min(
      Math.max(Number(limit) || DEFAULT_LIMIT, 1),
      MAX_LIMIT,
    );
    const effectiveSchema = (schema?.trim() || 'public').replace(/"/g, '');

    const dataSource = await this.dataSourceService.getDataSourceById(
      organizationId,
      sourceId,
      userId,
    );

    const connectionType = resolveSourceConnectorType(dataSource.sourceType)
      .registryType;

    const adapter = this.adapterRegistry.getAdapter(connectionType);

    const config = await this.connectionService.getDecryptedConnection(
      organizationId,
      sourceId,
      userId,
    );

    this.logger.log(
      `Streaming explorer data: ${effectiveSchema}.${table} (limit ${effectiveLimit})`,
    );

    return {
      adapter,
      params: {
        config,
        schema: effectiveSchema,
        table: table.trim(),
        limit: effectiveLimit,
      },
    };
  }

  /**
   * Execute arbitrary SQL against the remote database (JOINs, subqueries, etc.).
   * Like Snowflake/Redshift SQL editors - runs on the server.
   */
  async executeQuery(
    organizationId: string,
    sourceId: string,
    query: string,
    userId: string,
    maxRows = 10000,
    timeoutMs = 60000,
  ): Promise<ExecuteQueryResult> {
    if (!query?.trim()) {
      throw new BadRequestException('query is required');
    }

    const dataSource = await this.dataSourceService.getDataSourceById(
      organizationId,
      sourceId,
      userId,
    );

    const connectionType = resolveSourceConnectorType(dataSource.sourceType)
      .registryType;

    const adapter = this.adapterRegistry.getAdapter(connectionType);

    if (!adapter.executeQuery) {
      throw new BadRequestException(
        `Remote SQL execution is not supported for ${connectionType}. ` +
          'Use single-table load for this data source.',
      );
    }

    const config = await this.connectionService.getDecryptedConnection(
      organizationId,
      sourceId,
      userId,
    );

    this.logger.log(`Executing remote query (maxRows=${maxRows})`);

    return adapter.executeQuery({
      config,
      query: query.trim(),
      maxRows,
      timeoutMs,
    });
  }

  /**
   * Stream rows from a data source table as JSONL.
   * Used by the data source Explorer tab.
   */
  async *streamDataSourceTable(
    organizationId: string,
    sourceId: string,
    schema: string,
    table: string,
    limit: number,
    userId: string,
  ): AsyncIterable<Record<string, unknown>> {
    const { adapter, params } = await this.prepareStreamParams(
      organizationId,
      sourceId,
      schema,
      table,
      limit,
      userId,
    );
    yield* adapter.streamRows(params);
  }
}
