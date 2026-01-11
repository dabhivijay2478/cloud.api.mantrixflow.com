/**
 * Global Search Response DTOs
 */

import { ApiProperty } from '@nestjs/swagger';

export class SearchResultDto {
  @ApiProperty({
    description: 'Entity type (user, pipeline, data-source)',
    example: 'user',
  })
  type: string;

  @ApiProperty({
    description: 'Entity ID',
    example: '123e4567-e89b-12d3-a456-426614174000',
  })
  id: string;

  @ApiProperty({
    description: 'Display title',
    example: 'John Doe',
  })
  title: string;

  @ApiProperty({
    description: 'Display subtitle',
    example: 'john@example.com',
    required: false,
  })
  subtitle?: string;

  @ApiProperty({
    description: 'Redirect URL path',
    example: '/workspace/team',
  })
  redirect: string;

  @ApiProperty({
    description: 'Filter key to apply on destination page',
    example: 'name',
  })
  filterKey: string;

  @ApiProperty({
    description: 'Filter value to apply',
    example: 'john',
  })
  filterValue: string;
}

export class SearchResponseDto {
  @ApiProperty({
    description: 'Original search query',
    example: 'john',
  })
  query: string;

  @ApiProperty({
    description: 'Search results',
    type: [SearchResultDto],
  })
  results: SearchResultDto[];
}
