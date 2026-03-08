/**
 * Create Source Schema DTO
 * Validation and documentation for source schema creation
 */

import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { IsObject, IsOptional, IsString, IsUUID, MaxLength } from 'class-validator';

export class SourceConfigDto {
  @ApiPropertyOptional({ description: 'API key for external sources' })
  @IsOptional()
  @IsString()
  apiKey?: string;

  @ApiPropertyOptional({ description: 'API endpoint' })
  @IsOptional()
  @IsString()
  endpoint?: string;

  @ApiPropertyOptional({ description: 'Access token' })
  @IsOptional()
  @IsString()
  accessToken?: string;

  @ApiPropertyOptional({ description: 'Headers for API requests' })
  @IsOptional()
  @IsObject()
  headers?: Record<string, string>;

  @ApiPropertyOptional({ description: 'Rate limit (requests per second)' })
  @IsOptional()
  rateLimit?: number;
}

export class CreateSourceSchemaDto {
  @ApiProperty({
    description: 'Source type',
    example: 'postgres',
  })
  @IsString()
  @MaxLength(100)
  sourceType: string;

  @ApiPropertyOptional({
    description: 'Data source ID for database connections',
    example: '550e8400-e29b-41d4-a716-446655440000',
  })
  @IsOptional()
  @IsUUID()
  dataSourceId?: string;

  @ApiPropertyOptional({
    description: 'Source configuration for external sources (API, S3, etc.)',
    type: SourceConfigDto,
  })
  @IsOptional()
  @IsObject()
  sourceConfig?: SourceConfigDto;

  @ApiPropertyOptional({
    description: 'Source database schema name',
    example: 'public',
    maxLength: 255,
  })
  @IsOptional()
  @IsString()
  @MaxLength(255)
  sourceSchema?: string;

  @ApiPropertyOptional({
    description: 'Source table name',
    example: 'users',
    maxLength: 255,
  })
  @IsOptional()
  @IsString()
  @MaxLength(255)
  sourceTable?: string;

  @ApiPropertyOptional({
    description: 'Custom SQL query for source',
    example: 'SELECT * FROM users WHERE active = true',
  })
  @IsOptional()
  @IsString()
  sourceQuery?: string;

  @ApiPropertyOptional({
    description: 'Name/description for this source schema',
    example: 'Active Users Source',
    maxLength: 255,
  })
  @IsOptional()
  @IsString()
  @MaxLength(255)
  name?: string;
}
