/**
 * Global Search Service
 * Aggregates results from multiple entity search handlers
 */

import { Injectable } from '@nestjs/common';
import type { SearchHandler } from './interfaces/search-handler.interface';
import type { SearchRequestDto } from './dto/search-request.dto';
import type { SearchResponseDto } from './dto/search-response.dto';

@Injectable()
export class SearchService {
  private handlers: Map<string, SearchHandler> = new Map();

  /**
   * Register a search handler for an entity type
   */
  registerHandler(handler: SearchHandler): void {
    this.handlers.set(handler.entityType, handler);
  }

  /**
   * Search across all registered entity types
   */
  async search(dto: SearchRequestDto): Promise<SearchResponseDto> {
    const { organizationId, query, limit = 5 } = dto;

    // Search all entity types in parallel
    const searchPromises = Array.from(this.handlers.values()).map((handler) =>
      handler.search(organizationId, query, limit).catch((error) => {
        console.error(`Error searching ${handler.entityType}:`, error);
        return []; // Return empty array on error to not break other searches
      }),
    );

    const resultsArrays = await Promise.all(searchPromises);
    const results = resultsArrays.flat();

    return {
      query,
      results,
    };
  }

  /**
   * Get all registered entity types
   */
  getEntityTypes(): string[] {
    return Array.from(this.handlers.keys());
  }
}
