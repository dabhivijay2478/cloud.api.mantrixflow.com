/**
 * Connector Search Handler
 * Searches PostgreSQL connections (connectors)
 * This is an alias for data-source search to provide better UX
 */

import { Inject, Injectable } from '@nestjs/common';
import { and, eq, ilike, isNull, or } from 'drizzle-orm';
import type { DrizzleDatabase } from '../../../database/drizzle/database';
import { dataSources, dataSourceConnections } from '../../../database/schemas/data-sources';
import type { SearchHandler } from '../interfaces/search-handler.interface';
import type { SearchResultDto } from '../dto/search-response.dto';

@Injectable()
export class ConnectorSearchHandler implements SearchHandler {
  entityType = 'connector';

  constructor(@Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase) {}

  async search(
    organizationId: string,
    query: string,
    limit: number,
  ): Promise<SearchResultDto[]> {
    const searchPattern = `%${query}%`;

    const results = await this.db
      .select({
        id: dataSources.id,
        name: dataSources.name,
        sourceType: dataSources.sourceType,
        config: dataSourceConnections.config,
      })
      .from(dataSources)
      .leftJoin(dataSourceConnections, eq(dataSources.id, dataSourceConnections.dataSourceId))
      .where(
        and(
          eq(dataSources.organizationId, organizationId),
          isNull(dataSources.deletedAt),
          or(
            ilike(dataSources.name, searchPattern),
            ilike(dataSources.sourceType, searchPattern),
          ),
        ),
      )
      .limit(limit);

    return results.map((dataSource) => {
      // Extract connection details from config JSONB for display
      const config = dataSource.config as any;
      const subtitle = config?.host && config?.database 
        ? `${config.host}/${config.database}`
        : dataSource.sourceType;

      return {
        type: this.entityType,
        id: dataSource.id,
        title: dataSource.name || 'Unnamed Connector',
        subtitle,
        redirect: '/workspace/data-sources',
        filterKey: 'name',
        filterValue: query,
      };
    });
  }
}
