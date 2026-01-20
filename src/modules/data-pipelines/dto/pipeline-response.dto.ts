/**
 * Pipeline Response DTOs
 * Response types for pipeline API endpoints
 */

import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class PipelineResponseDto {
  @ApiProperty({ description: 'Pipeline ID' })
  id: string;

  @ApiProperty({ description: 'Organization ID' })
  organizationId: string;

  @ApiProperty({ description: 'Created by user ID' })
  createdBy: string;

  @ApiProperty({ description: 'Pipeline name' })
  name: string;

  @ApiPropertyOptional({ description: 'Pipeline description' })
  description?: string;

  @ApiProperty({ description: 'Source schema ID' })
  sourceSchemaId: string;

  @ApiProperty({ description: 'Destination schema ID' })
  destinationSchemaId: string;

  @ApiPropertyOptional({ description: 'Data transformations' })
  transformations?: any[];

  @ApiProperty({ description: 'Sync mode' })
  syncMode: string;

  @ApiPropertyOptional({ description: 'Incremental column' })
  incrementalColumn?: string;

  @ApiProperty({ description: 'Sync frequency' })
  syncFrequency: string;

  @ApiProperty({ description: 'Pipeline status' })
  status: string;

  @ApiPropertyOptional({ description: 'Last run timestamp' })
  lastRunAt?: Date;

  @ApiPropertyOptional({ description: 'Last run status' })
  lastRunStatus?: string;

  @ApiPropertyOptional({ description: 'Last error message' })
  lastError?: string;

  @ApiProperty({ description: 'Total rows processed' })
  totalRowsProcessed: number;

  @ApiProperty({ description: 'Total successful runs' })
  totalRunsSuccessful: number;

  @ApiProperty({ description: 'Total failed runs' })
  totalRunsFailed: number;

  @ApiProperty({ description: 'Created timestamp' })
  createdAt: Date;

  @ApiProperty({ description: 'Updated timestamp' })
  updatedAt: Date;

  @ApiPropertyOptional({ 
    description: 'Applied column mappings',
    type: [Object],
    example: [{ sourcePath: 'email', destPath: 'user_email' }]
  })
  appliedMappings?: Array<{ sourcePath: string; destPath: string }>;
}

export class PipelineRunResponseDto {
  @ApiProperty({ description: 'Run ID' })
  id: string;

  @ApiProperty({ description: 'Pipeline ID' })
  pipelineId: string;

  @ApiProperty({ description: 'Organization ID' })
  organizationId: string;

  @ApiProperty({ description: 'Run status' })
  status: string;

  @ApiProperty({ description: 'Job state' })
  jobState: string;

  @ApiProperty({ description: 'Trigger type' })
  triggerType: string;

  @ApiPropertyOptional({ description: 'Triggered by user ID' })
  triggeredBy?: string;

  @ApiProperty({ description: 'Rows read' })
  rowsRead: number;

  @ApiProperty({ description: 'Rows written' })
  rowsWritten: number;

  @ApiProperty({ description: 'Rows skipped' })
  rowsSkipped: number;

  @ApiProperty({ description: 'Rows failed' })
  rowsFailed: number;

  @ApiPropertyOptional({ description: 'Started timestamp' })
  startedAt?: Date;

  @ApiPropertyOptional({ description: 'Completed timestamp' })
  completedAt?: Date;

  @ApiPropertyOptional({ description: 'Duration in seconds' })
  durationSeconds?: number;

  @ApiPropertyOptional({ description: 'Error message' })
  errorMessage?: string;

  @ApiProperty({ description: 'Created timestamp' })
  createdAt: Date;
}

export class PipelineStatsResponseDto {
  @ApiProperty({ description: 'Total rows processed' })
  totalRowsProcessed: number;

  @ApiProperty({ description: 'Total successful runs' })
  totalRunsSuccessful: number;

  @ApiProperty({ description: 'Total failed runs' })
  totalRunsFailed: number;

  @ApiPropertyOptional({ description: 'Last successful run timestamp' })
  lastSuccessfulRun?: Date;

  @ApiProperty({ description: 'Average run duration in seconds' })
  averageDuration: number;
}

export class DryRunResponseDto {
  @ApiProperty({ description: 'Number of rows that would be written' })
  wouldWrite: number;

  @ApiPropertyOptional({ description: 'Total source row count' })
  sourceRowCount?: number;

  @ApiProperty({ description: 'Sample transformed rows' })
  sampleRows: any[];

  @ApiProperty({ description: 'Any errors encountered' })
  errors: any[];

  @ApiPropertyOptional({ description: 'Sample transformed data' })
  transformedSample?: any[];

  @ApiPropertyOptional({ 
    description: 'Applied column mappings',
    type: [Object],
    example: [{ sourcePath: 'email', destPath: 'user_email' }]
  })
  appliedMappings?: Array<{ sourcePath: string; destPath: string }>;
}

export class ValidationResultResponseDto {
  @ApiProperty({ description: 'Whether validation passed' })
  valid: boolean;

  @ApiProperty({ description: 'Validation errors' })
  errors: string[];

  @ApiPropertyOptional({ description: 'Validation warnings' })
  warnings?: string[];
}
