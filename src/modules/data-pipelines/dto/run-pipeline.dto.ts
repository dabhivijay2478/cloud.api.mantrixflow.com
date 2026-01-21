/**
 * Run Pipeline DTO
 * Validation and documentation for pipeline execution
 */

import { ApiPropertyOptional } from '@nestjs/swagger';
import { IsOptional, IsEnum, IsNumber, Min, Max } from 'class-validator';

export enum TriggerType {
  MANUAL = 'manual',
  SCHEDULED = 'scheduled',
  API = 'api',
  POLLING = 'polling',
}

export class RunPipelineDto {
  @ApiPropertyOptional({
    description: 'Trigger type for this run',
    enum: TriggerType,
    default: TriggerType.MANUAL,
  })
  @IsOptional()
  @IsEnum(TriggerType)
  triggerType?: TriggerType;

  @ApiPropertyOptional({
    description: 'Batch size for processing',
    example: 1000,
    default: 1000,
  })
  @IsOptional()
  @IsNumber()
  @Min(1)
  @Max(10000)
  batchSize?: number;

  @ApiPropertyOptional({
    description: 'Force full sync even for incremental pipelines',
    example: false,
  })
  @IsOptional()
  forceFullSync?: boolean;
}

export class DryRunPipelineDto {
  @ApiPropertyOptional({
    description: 'Number of sample rows to process',
    example: 10,
    default: 10,
  })
  @IsOptional()
  @IsNumber()
  @Min(1)
  @Max(100)
  sampleSize?: number;
}
