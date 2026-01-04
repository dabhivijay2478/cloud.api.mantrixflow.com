import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { IsObject, IsOptional, IsString } from 'class-validator';

/**
 * Destination Configuration DTO
 */
export class DestinationConfigDto {
  @ApiProperty({ description: 'Destination connection ID' })
  @IsString()
  connectionId: string;

  @ApiPropertyOptional({ description: 'Destination schema', default: 'public' })
  @IsString()
  @IsOptional()
  schema?: string;

  @ApiProperty({ description: 'Destination table name' })
  @IsString()
  table: string;

  @ApiPropertyOptional({
    description: 'Write mode',
    enum: ['append', 'upsert', 'replace'],
    default: 'append',
  })
  @IsString()
  @IsOptional()
  writeMode?: 'append' | 'upsert' | 'replace';

  @ApiPropertyOptional({ description: 'Upsert key columns' })
  @IsOptional()
  upsertKey?: string[];

  @ApiPropertyOptional({ description: 'Additional destination configuration' })
  @IsObject()
  @IsOptional()
  config?: Record<string, any>;
}
