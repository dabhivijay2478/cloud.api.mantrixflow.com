/**
 * Update Source Schema DTO
 * Validation and documentation for source schema updates
 */

import { ApiPropertyOptional } from '@nestjs/swagger';
import { IsString, IsOptional, MaxLength, IsObject, IsBoolean } from 'class-validator';
import { SourceConfigDto } from './create-source-schema.dto';

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
}
