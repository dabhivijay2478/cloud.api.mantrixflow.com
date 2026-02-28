/**
 * Create Destination Schema DTO
 * Validation and documentation for destination schema creation
 */

import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import {
  IsString,
  IsUUID,
  IsOptional,
  IsArray,
  IsEnum,
  MaxLength,
  MinLength,
  IsBoolean,
} from 'class-validator';

export enum WriteMode {
  APPEND = 'append',
  UPSERT = 'upsert',
  REPLACE = 'replace',
}

export class CreateDestinationSchemaDto {
  @ApiProperty({
    description: 'Data source ID for destination connection',
    example: '550e8400-e29b-41d4-a716-446655440000',
  })
  @IsUUID()
  dataSourceId: string;

  @ApiPropertyOptional({
    description: 'Destination database schema name',
    example: 'public',
    default: 'public',
    maxLength: 255,
  })
  @IsOptional()
  @IsString()
  @MaxLength(255)
  destinationSchema?: string;

  @ApiProperty({
    description: 'Destination table name',
    example: 'synced_users',
    maxLength: 255,
  })
  @IsString()
  @MinLength(1)
  @MaxLength(255)
  destinationTable: string;

  @ApiPropertyOptional({
    description: 'Whether destination table already exists',
    example: false,
  })
  @IsOptional()
  @IsBoolean()
  destinationTableExists?: boolean;

  @ApiPropertyOptional({
    description: 'Transform type: dbt',
    example: 'dbt',
    default: 'dbt',
  })
  @IsOptional()
  @IsString()
  transformType?: string;

  @ApiPropertyOptional({
    description: 'dbt model name when transformType is dbt',
    example: 'stg_company_role_combined',
  })
  @IsOptional()
  @IsString()
  @MaxLength(255)
  dbtModel?: string;

  @ApiPropertyOptional({
    description: 'Custom SQL - runs through dbt against raw_input (e.g. select id, company_name as name from raw_input)',
  })
  @IsOptional()
  @IsString()
  customSql?: string;

  @ApiPropertyOptional({
    description: 'Write mode for destination',
    enum: WriteMode,
    default: WriteMode.APPEND,
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
    description: 'Name/description for this destination schema',
    example: 'Synced Users Destination',
    maxLength: 255,
  })
  @IsOptional()
  @IsString()
  @MaxLength(255)
  name?: string;
}
