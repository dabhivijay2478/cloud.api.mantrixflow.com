/**
 * Search Handler Interface
 * Pluggable interface for entity-specific search implementations
 */

import type { SearchResultDto } from '../dto/search-response.dto';

export interface SearchHandler {
  /**
   * Entity type identifier (e.g., 'user', 'pipeline', 'data-source')
   */
  entityType: string;

  /**
   * Search entities matching the query
   * @param organizationId - Organization ID to scope search
   * @param query - Search query string
   * @param limit - Maximum number of results
   * @returns Array of search results
   */
  search(
    organizationId: string,
    query: string,
    limit: number,
  ): Promise<SearchResultDto[]>;
}
