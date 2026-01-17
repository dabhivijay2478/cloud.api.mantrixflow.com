/**
 * Create Pipeline DTO
 * Validation and documentation for pipeline creation
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
  ValidateNested,
} from 'class-validator';
import { Type } from 'class-transformer';

export enum SyncMode {
  FULL = 'full',
  INCREMENTAL = 'incremental',
}

export enum SyncFrequency {
  MANUAL = 'manual',
  HOURLY = 'hourly',
  DAILY = 'daily',
  WEEKLY = 'weekly',
}

export class TransformationDto {
  @ApiProperty({ description: 'Source column name' })
  @IsString()
  sourceColumn: string;

  @ApiProperty({
    description: 'Transformation type',
    enum: ['rename', 'cast', 'concat', 'split', 'custom', 'filter', 'mask', 'hash'],
  })
  @IsString()
  transformType: 'rename' | 'cast' | 'concat' | 'split' | 'custom' | 'filter' | 'mask' | 'hash';

  @ApiProperty({ description: 'Transformation configuration', default: {} })
  transformConfig: any = {};

  @ApiProperty({ description: 'Destination column name' })
  @IsString()
  destinationColumn: string;
}

export class CreatePipelineDto {
  @ApiProperty({
    description: 'Pipeline name',
    example: 'Sales Data Pipeline',
    maxLength: 255,
  })
  @IsString()
  @MinLength(1)
  @MaxLength(255)
  name: string;

  @ApiPropertyOptional({
    description: 'Pipeline description',
    example: 'Syncs sales data from PostgreSQL to BigQuery',
  })
  @IsOptional()
  @IsString()
  description?: string;

  @ApiProperty({
    description: 'Source schema ID',
    example: '550e8400-e29b-41d4-a716-446655440000',
  })
  @IsUUID()
  sourceSchemaId: string;

  @ApiProperty({
    description: 'Destination schema ID',
    example: '550e8400-e29b-41d4-a716-446655440001',
  })
  @IsUUID()
  destinationSchemaId: string;

  @ApiPropertyOptional({
    description: 'Data transformations to apply',
    type: [TransformationDto],
  })
  @IsOptional()
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => TransformationDto)
  transformations?: TransformationDto[];

  @ApiPropertyOptional({
    description: 'Sync mode',
    enum: SyncMode,
    default: SyncMode.FULL,
  })
  @IsOptional()
  @IsEnum(SyncMode)
  syncMode?: SyncMode;

  @ApiPropertyOptional({
    description: 'Column to use for incremental syncs',
    example: 'updated_at',
  })
  @IsOptional()
  @IsString()
  incrementalColumn?: string;

  @ApiPropertyOptional({
    description: 'Sync frequency',
    enum: SyncFrequency,
    default: SyncFrequency.MANUAL,
  })
  @IsOptional()
  @IsEnum(SyncFrequency)
  syncFrequency?: SyncFrequency;
}
