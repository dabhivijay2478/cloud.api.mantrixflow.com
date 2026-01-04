/**
 * DTOs for Organization Member Invites
 */

import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { IsArray, IsBoolean, IsEmail, IsEnum, IsOptional, IsString } from 'class-validator';

export class InviteMemberDto {
  @ApiProperty({
    description: 'Email address of the user to invite',
    example: 'user@example.com',
  })
  @IsEmail({}, { message: 'Please provide a valid email address' })
  @IsString()
  email: string;

  @ApiProperty({
    description: 'Role to assign to the invited member',
    enum: ['owner', 'admin', 'member', 'viewer', 'guest'],
    example: 'member',
  })
  @IsEnum(['owner', 'admin', 'member', 'viewer', 'guest'], {
    message: 'Role must be one of: owner, admin, member, viewer, guest',
  })
  role: 'owner' | 'admin' | 'member' | 'viewer' | 'guest';

  @ApiPropertyOptional({
    description: 'Whether the member should have agent panel access',
    default: false,
  })
  @IsBoolean()
  @IsOptional()
  agentPanelAccess?: boolean;

  @ApiPropertyOptional({
    description: 'List of allowed AI models for agent panel access',
    type: [String],
    example: ['gpt-4o', 'claude-opus-4-20250514'],
  })
  @IsArray()
  @IsString({ each: true })
  @IsOptional()
  allowedModels?: string[];
}

export class UpdateMemberDto {
  @ApiPropertyOptional({
    description: 'Role to assign to the member',
    enum: ['owner', 'admin', 'member', 'viewer', 'guest'],
  })
  @IsEnum(['owner', 'admin', 'member', 'viewer', 'guest'])
  @IsOptional()
  role?: 'owner' | 'admin' | 'member' | 'viewer' | 'guest';

  @ApiPropertyOptional({
    description: 'Whether the member should have agent panel access',
  })
  @IsBoolean()
  @IsOptional()
  agentPanelAccess?: boolean;

  @ApiPropertyOptional({
    description: 'List of allowed AI models for agent panel access',
    type: [String],
  })
  @IsArray()
  @IsString({ each: true })
  @IsOptional()
  allowedModels?: string[];

  @ApiPropertyOptional({
    description: 'Member status',
    enum: ['invited', 'accepted', 'active', 'inactive'],
  })
  @IsEnum(['invited', 'accepted', 'active', 'inactive'])
  @IsOptional()
  status?: 'invited' | 'accepted' | 'active' | 'inactive';
}
