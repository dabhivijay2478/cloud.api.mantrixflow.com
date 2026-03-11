/**
 * Update Source Schema DTO
 * Validation and documentation for source schema updates
 */

import { ApiPropertyOptional } from '@nestjs/swagger';
import {
  IsString,
  IsOptional,
  MaxLength,
  IsObject,
  IsBoolean,
  IsArray,
  IsNumber,
} from 'class-validator';
import { SourceConfigDto } from './create-source-schema.dto';

export class DiscoveredColumnDto {
  @ApiPropertyOptional({ description: 'Column name' })
  @IsOptional()
  @IsString()
  name?: string;

  @ApiPropertyOptional({ description: 'Column data type' })
  @IsOptional()
  @IsString()
  type?: string;

  @ApiPropertyOptional({ description: 'Whether column is nullable' })
  @IsOptional()
  nullable?: boolean;
}

export class UpdateSourceSchemaDto {
  @ApiPropertyOptional({
    description: 'Name/description for this source schema',
    example: 'Updated Source Name',
    maxLength: 255,
  })
  @IsOptional()
  @IsString()
  @MaxLength(255)
  name?: string;

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
    description: 'Source configuration for external sources',
    type: SourceConfigDto,
  })
  @IsOptional()
  @IsObject()
  sourceConfig?: SourceConfigDto;

  @ApiPropertyOptional({
    description: 'Whether this schema is active',
    example: true,
  })
  @IsOptional()
  @IsBoolean()
  isActive?: boolean;

  @ApiPropertyOptional({
    description: 'Discovered columns from schema discovery',
    type: [DiscoveredColumnDto],
  })
  @IsOptional()
  @IsArray()
  discoveredColumns?: DiscoveredColumnDto[];

  @ApiPropertyOptional({
    description: 'Primary key column names',
    type: [String],
    example: ['id'],
  })
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  primaryKeys?: string[];

  @ApiPropertyOptional({
    description: 'Estimated row count from discovery',
    example: 1000,
  })
  @IsOptional()
  @IsNumber()
  estimatedRowCount?: number;
}
