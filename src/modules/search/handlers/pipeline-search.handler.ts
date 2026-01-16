/**
 * Pipeline Search Handler
 * Searches data pipelines
 */

import { Inject, Injectable } from '@nestjs/common';
import { and, eq, ilike, or } from 'drizzle-orm';
import type { DrizzleDatabase } from '../../../database/drizzle/database';
import { pipelines } from '../../../database/schemas/data-pipelines';
import type { SearchHandler } from '../interfaces/search-handler.interface';
import type { SearchResultDto } from '../dto/search-response.dto';

@Injectable()
export class PipelineSearchHandler implements SearchHandler {
  entityType = 'pipeline';

  constructor(@Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase) {}

  async search(
    organizationId: string,
    query: string,
    limit: number,
  ): Promise<SearchResultDto[]> {
    const searchPattern = `%${query}%`;

    const results = await this.db
      .select({
        id: pipelines.id,
        name: pipelines.name,
        description: pipelines.description,
      })
      .from(pipelines)
      .where(
        and(
          eq(pipelines.organizationId, organizationId),
          or(
            ilike(pipelines.name, searchPattern),
            pipelines.description
              ? ilike(pipelines.description, searchPattern)
              : undefined,
          ),
        ),
      )
      .limit(limit);

    return results.map((pipeline) => ({
      type: this.entityType,
      id: pipeline.id,
      title: pipeline.name || 'Unnamed Pipeline',
      subtitle: pipeline.description || undefined,
      redirect: '/workspace/data-pipelines',
      filterKey: 'name',
      filterValue: query,
    }));
  }
}
