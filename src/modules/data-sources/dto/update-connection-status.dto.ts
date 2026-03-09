/**
 * Update Connection Status DTO
 * Validation for PATCH .../connection (disconnect/reconnect)
 */

import { ApiProperty } from '@nestjs/swagger';
import { IsIn } from 'class-validator';

export class UpdateConnectionStatusDto {
  @ApiProperty({ enum: ['active', 'inactive'] })
  @IsIn(['active', 'inactive'])
  status!: 'active' | 'inactive';
}
