import { ApiProperty } from '@nestjs/swagger';
import { IsString, IsEnum, IsOptional } from 'class-validator';

export class CreateSyncJobDto {
  @ApiProperty({ description: 'Table name to sync', example: 'users' })
  @IsString()
  tableName: string;

  @ApiProperty({ description: 'Schema name', example: 'public', default: 'public', required: false })
  @IsString()
  @IsOptional()
  schema?: string;

  @ApiProperty({
    description: 'Sync mode',
    enum: ['full', 'incremental'],
    example: 'full',
  })
  @IsEnum(['full', 'incremental'])
  syncMode: 'full' | 'incremental';

  @ApiProperty({
    description: 'Incremental column (required for incremental sync)',
    example: 'updated_at',
    required: false,
  })
  @IsString()
  @IsOptional()
  incrementalColumn?: string;

  @ApiProperty({
    description: 'Custom WHERE clause for filtering',
    example: "status = 'active'",
    required: false,
  })
  @IsString()
  @IsOptional()
  customWhereClause?: string;

  @ApiProperty({
    description: 'Sync frequency',
    enum: ['manual', '15min', '1hour', '24hours'],
    example: 'manual',
    default: 'manual',
    required: false,
  })
  @IsEnum(['manual', '15min', '1hour', '24hours'])
  @IsOptional()
  syncFrequency?: 'manual' | '15min' | '1hour' | '24hours';
}

export class SyncJobResponseDto {
  @ApiProperty({ description: 'Sync job ID', example: '123e4567-e89b-12d3-a456-426614174000' })
  id: string;

  @ApiProperty({ description: 'Connection ID', example: '123e4567-e89b-12d3-a456-426614174000' })
  connectionId: string;

  @ApiProperty({ description: 'Table name', example: 'users' })
  tableName: string;

  @ApiProperty({ description: 'Sync mode', enum: ['full', 'incremental'] })
  syncMode: string;

  @ApiProperty({ description: 'Job status', enum: ['pending', 'running', 'success', 'failed'] })
  status: string;

  @ApiProperty({ description: 'Rows synced', example: 1000 })
  rowsSynced: number;

  @ApiProperty({ description: 'Sync frequency', enum: ['manual', '15min', '1hour', '24hours'] })
  syncFrequency: string;

  @ApiProperty({ description: 'Started at', required: false })
  startedAt?: Date;

  @ApiProperty({ description: 'Completed at', required: false })
  completedAt?: Date;

  @ApiProperty({ description: 'Error message', required: false })
  errorMessage?: string;
}

