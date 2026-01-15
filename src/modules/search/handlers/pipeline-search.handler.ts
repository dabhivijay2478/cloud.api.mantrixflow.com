/**
 * Pipeline Search Handler
 * Searches data pipelines
 */

import { Inject, Injectable } from '@nestjs/common';
import { and, eq, ilike, or } from 'drizzle-orm';
import type { DrizzleDatabase } from '../../../database/drizzle/database';
import { postgresPipelines } from '../../../database/schemas/data-pipelines';
import type { SearchHandler } from '../interfaces/search-handler.interface';
import type { SearchResultDto } from '../dto/search-response.dto';

@Injectable()
export class PipelineSearchHandler implements SearchHandler {
  entityType = 'pipeline';

  constructor(@Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase) {}

  async search(organizationId: string, query: string, limit: number): Promise<SearchResultDto[]> {
    const searchPattern = `%${query}%`;

    const results = await this.db
      .select({
        id: postgresPipelines.id,
        name: postgresPipelines.name,
        description: postgresPipelines.description,
      })
      .from(postgresPipelines)
      .where(
        and(
          eq(postgresPipelines.orgId, organizationId),
          or(
            ilike(postgresPipelines.name, searchPattern),
            postgresPipelines.description
              ? ilike(postgresPipelines.description, searchPattern)
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
