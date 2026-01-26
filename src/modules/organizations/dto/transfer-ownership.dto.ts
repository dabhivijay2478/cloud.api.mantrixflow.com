import { ApiProperty } from '@nestjs/swagger';
import { IsNotEmpty, IsUUID } from 'class-validator';

/**
 * Transfer Ownership DTO
 */
export class TransferOwnershipDto {
  @ApiProperty({
    description: 'User ID of the new owner (must be a member of the organization)',
    example: 'uuid',
  })
  @IsUUID()
  @IsNotEmpty()
  newOwnerId: string;
}
