/**
 * User Search Handler
 * Searches organization members (team members)
 */

import { Inject, Injectable } from '@nestjs/common';
import { and, eq, ilike } from 'drizzle-orm';
import type { DrizzleDatabase } from '../../../database/drizzle/database';
import { organizationMembers } from '../../../database/schemas/organizations';
import type { SearchHandler } from '../interfaces/search-handler.interface';
import type { SearchResultDto } from '../dto/search-response.dto';

@Injectable()
export class UserSearchHandler implements SearchHandler {
  entityType = 'user';

  constructor(@Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase) {}

  async search(organizationId: string, query: string, limit: number): Promise<SearchResultDto[]> {
    const searchPattern = `%${query}%`;

    const results = await this.db
      .select({
        id: organizationMembers.id,
        email: organizationMembers.email,
        userId: organizationMembers.userId,
      })
      .from(organizationMembers)
      .where(
        and(
          eq(organizationMembers.organizationId, organizationId),
          ilike(organizationMembers.email, searchPattern),
          // Note: We're searching by email since that's what we have
          // If users table has name fields, we'd join and search those too
        ),
      )
      .limit(limit);

    return results.map((member) => ({
      type: this.entityType,
      id: member.userId || member.id, // Use userId if available, otherwise member id
      title: member.email.split('@')[0], // Use email prefix as display name
      subtitle: member.email,
      redirect: '/workspace/team',
      filterKey: 'name', // Column to filter on the team page
      filterValue: query,
    }));
  }
}
