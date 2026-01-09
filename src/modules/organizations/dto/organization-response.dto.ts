import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

/**
 * Organization Response DTO
 * Includes organization data plus user's role/ownership information
 */
export class OrganizationResponseDto {
  @ApiProperty({ description: 'Organization ID', example: 'uuid' })
  id: string;

  @ApiProperty({ description: 'Organization name', example: 'Acme Corporation' })
  name: string;

  @ApiProperty({ description: 'Organization slug', example: 'acme-corporation' })
  slug: string;

  @ApiPropertyOptional({ description: 'Organization description' })
  description?: string;

  @ApiProperty({ description: 'Whether user is an owner of this organization' })
  isOwner: boolean;

  @ApiPropertyOptional({ description: 'User\'s role in the organization (if member)', enum: ['owner', 'admin', 'member', 'viewer', 'guest'] })
  role?: 'owner' | 'admin' | 'member' | 'viewer' | 'guest';

  @ApiProperty({ description: 'Created at timestamp' })
  createdAt: Date;

  @ApiProperty({ description: 'Updated at timestamp' })
  updatedAt: Date;
}
