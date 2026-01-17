/**
 * Update Pipeline DTO
 * Validation and documentation for pipeline updates
 */

import { ApiPropertyOptional } from '@nestjs/swagger';
import {
  IsString,
  IsOptional,
  IsArray,
  IsEnum,
  MaxLength,
  MinLength,
  ValidateNested,
} from 'class-validator';
import { Type } from 'class-transformer';
import { TransformationDto, SyncMode, SyncFrequency } from './create-pipeline.dto';
import { PipelineStatus } from '../types/pipeline-lifecycle.types';

// Re-export for backward compatibility
export { PipelineStatus };

export class UpdatePipelineDto {
  @ApiPropertyOptional({
    description: 'Pipeline name',
    example: 'Updated Sales Pipeline',
    maxLength: 255,
  })
  @IsOptional()
  @IsString()
  @MinLength(1)
  @MaxLength(255)
  name?: string;

  @ApiPropertyOptional({
    description: 'Pipeline description',
    example: 'Updated description for the pipeline',
  })
  @IsOptional()
  @IsString()
  description?: string;

  @ApiPropertyOptional({
    description: 'Pipeline status',
    enum: PipelineStatus,
  })
  @IsOptional()
  @IsEnum(PipelineStatus)
  status?: PipelineStatus;

  @ApiPropertyOptional({
    description: 'Sync mode',
    enum: SyncMode,
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
  })
  @IsOptional()
  @IsEnum(SyncFrequency)
  syncFrequency?: SyncFrequency;

  @ApiPropertyOptional({
    description: 'Data transformations to apply',
    type: [TransformationDto],
  })
  @IsOptional()
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => TransformationDto)
  transformations?: TransformationDto[];
}
