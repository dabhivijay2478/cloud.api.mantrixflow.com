/**
 * Connector Search Handler
 * Searches PostgreSQL connections (connectors)
 * This is an alias for data-source search to provide better UX
 */

import { Inject, Injectable } from '@nestjs/common';
import { and, eq, ilike, or } from 'drizzle-orm';
import type { DrizzleDatabase } from '../../../database/drizzle/database';
import { postgresConnections } from '../../../database/schemas/data-sources';
import type { SearchHandler } from '../interfaces/search-handler.interface';
import type { SearchResultDto } from '../dto/search-response.dto';

@Injectable()
export class ConnectorSearchHandler implements SearchHandler {
  entityType = 'connector';

  constructor(@Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase) {}

  async search(organizationId: string, query: string, limit: number): Promise<SearchResultDto[]> {
    const searchPattern = `%${query}%`;

    const results = await this.db
      .select({
        id: postgresConnections.id,
        name: postgresConnections.name,
        host: postgresConnections.host,
        database: postgresConnections.database,
      })
      .from(postgresConnections)
      .where(
        and(
          eq(postgresConnections.orgId, organizationId),
          or(
            ilike(postgresConnections.name, searchPattern),
            ilike(postgresConnections.host, searchPattern),
            ilike(postgresConnections.database, searchPattern),
          ),
        ),
      )
      .limit(limit);

    return results.map((connection) => ({
      type: this.entityType,
      id: connection.id,
      title: connection.name || 'Unnamed Connector',
      subtitle: `${connection.host}/${connection.database}`,
      redirect: '/workspace/data-sources',
      filterKey: 'name',
      filterValue: query,
    }));
  }
}
