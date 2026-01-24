/**
 * Update Destination Schema DTO
 * Validation and documentation for destination schema updates
 */

import { ApiPropertyOptional } from '@nestjs/swagger';
import {
  IsString,
  IsOptional,
  IsArray,
  IsEnum,
  MaxLength,
  IsBoolean,
} from 'class-validator';
import { WriteMode } from './create-destination-schema.dto';

export class UpdateDestinationSchemaDto {
  @ApiPropertyOptional({
    description: 'Name/description for this destination schema',
    example: 'Updated Destination Name',
    maxLength: 255,
  })
  @IsOptional()
  @IsString()
  @MaxLength(255)
  name?: string;

  @ApiPropertyOptional({
    description: 'Destination database schema name',
    example: 'public',
    maxLength: 255,
  })
  @IsOptional()
  @IsString()
  @MaxLength(255)
  destinationSchema?: string;

  @ApiPropertyOptional({
    description: 'Destination table name',
    example: 'synced_users',
    maxLength: 255,
  })
  @IsOptional()
  @IsString()
  @MaxLength(255)
  destinationTable?: string;

  @ApiPropertyOptional({
    description: 'Custom Python transform script (defines transform(record) function)',
    example: 'def transform(record):\n    return {"id": record.get("id"), "name": record.get("name")}',
  })
  @IsOptional()
  @IsString()
  transformScript?: string;

  @ApiPropertyOptional({
    description: 'Write mode for destination',
    enum: WriteMode,
  })
  @IsOptional()
  @IsEnum(WriteMode)
  writeMode?: WriteMode;

  @ApiPropertyOptional({
    description: 'Columns for upsert key (if writeMode is upsert)',
    example: ['id', 'email'],
  })
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  upsertKey?: string[];

  @ApiPropertyOptional({
    description: 'Whether this schema is active',
    example: true,
  })
  @IsOptional()
  @IsBoolean()
  isActive?: boolean;
}
