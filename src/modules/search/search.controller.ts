/**
 * Global Search Controller
 * REST API endpoint for global search across multiple entities
 */

import { Controller, Get, Query, UseGuards } from '@nestjs/common';
import {
  ApiBearerAuth,
  ApiOperation,
  ApiQuery,
  ApiResponse,
  ApiTags,
} from '@nestjs/swagger';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import { SearchRequestDto } from './dto/search-request.dto';
import { SearchResponseDto } from './dto/search-response.dto';
import { SearchService } from './search.service';

@ApiTags('search')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard)
@Controller('search')
export class SearchController {
  constructor(private readonly searchService: SearchService) {}

  @Get()
  @ApiOperation({
    summary: 'Global search',
    description:
      'Search across multiple entity types (users, pipelines, data sources) within an organization.',
  })
  @ApiQuery({
    name: 'organizationId',
    description: 'Organization ID to search within',
    type: String,
    required: true,
  })
  @ApiQuery({
    name: 'query',
    description: 'Search query string',
    type: String,
    required: true,
  })
  @ApiQuery({
    name: 'limit',
    description: 'Maximum number of results per entity type',
    type: Number,
    required: false,
  })
  @ApiResponse({
    status: 200,
    description: 'Search results',
    type: SearchResponseDto,
  })
  async search(@Query() dto: SearchRequestDto): Promise<SearchResponseDto> {
    return this.searchService.search(dto);
  }
}
