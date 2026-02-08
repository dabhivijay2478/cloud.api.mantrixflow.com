/**
 * Organization Member Controller
 * REST API endpoints for organization member invites and management
 */

import {
  BadRequestException,
  Body,
  ConflictException,
  Controller,
  Delete,
  Get,
  HttpCode,
  HttpStatus,
  NotFoundException,
  Param,
  Patch,
  Post,
  Query,
  Request,
} from '@nestjs/common';
import type { Request as ExpressRequest } from 'express';

type ExpressRequestType = ExpressRequest;

import { UseGuards } from '@nestjs/common';
import {
  ApiBearerAuth,
  ApiBody,
  ApiOperation,
  ApiParam,
  ApiResponse,
  ApiTags,
} from '@nestjs/swagger';
import {
  createDeleteResponse,
  createListResponse,
  createSuccessResponse,
} from '../../common/dto/api-response.dto';
import { OrganizationRoleGuard, RequireRole } from '../../common/guards/organization-role.guard';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import { InviteMemberDto, UpdateMemberDto } from './dto/invite-member.dto';
import { OrganizationMemberService } from './organization-member.service';

@ApiTags('organizations')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard)
@Controller('organizations')
export class OrganizationMemberController {
  constructor(private readonly memberService: OrganizationMemberService) {}

  /**
   * Invite a member to an organization
   * AUTHORIZATION: Only OWNER and ADMIN can invite members
   */
  @Post(':organizationId/members/invite')
  @UseGuards(OrganizationRoleGuard)
  @RequireRole('OWNER', 'ADMIN')
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: 'Invite member to organization',
    description: 'Invite a user to join the organization by email (OWNER/ADMIN only)',
  })
  @ApiParam({
    name: 'organizationId',
    description: 'Organization ID',
  })
  @ApiBody({ type: InviteMemberDto })
  @ApiResponse({
    status: 201,
    description: 'Member invited successfully',
  })
  @ApiResponse({
    status: 403,
    description: 'Forbidden - Only OWNER and ADMIN can invite members',
  })
  @ApiResponse({
    status: 409,
    description: 'User has already been invited',
  })
  async inviteMember(
    @Param('organizationId') organizationId: string,
    @Body() dto: InviteMemberDto,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = req.user?.id;
      if (!userId) {
        throw new BadRequestException('User ID is required');
      }

      console.log('[INVITE API] Inviting member:', {
        organizationId,
        email: dto.email,
        role: dto.role,
        userId,
      });

      const member = await this.memberService.inviteMember(organizationId, userId, dto);

      console.log('[INVITE API] Invite successful:', member.id);

      return createSuccessResponse(member, 'Member invited successfully', 201);
    } catch (error) {
      console.error('[INVITE API] Error in inviteMember:', error);
      console.error('[INVITE API] Error type:', error?.constructor?.name);
      console.error(
        '[INVITE API] Error details:',
        JSON.stringify(error, Object.getOwnPropertyNames(error)),
      );

      // Re-throw NestJS exceptions as-is
      if (
        error instanceof BadRequestException ||
        error instanceof NotFoundException ||
        error instanceof ConflictException
      ) {
        throw error;
      }

      // Wrap unexpected errors
      throw new BadRequestException(
        error instanceof Error ? error.message : 'Failed to invite member',
      );
    }
  }

  /**
   * List all members of an organization
   */
  @Get(':organizationId/members')
  @ApiOperation({
    summary: 'List organization members',
    description: 'Get all members (active and invited) for an organization',
  })
  @ApiParam({
    name: 'organizationId',
    description: 'Organization ID',
  })
  @ApiResponse({
    status: 200,
    description: 'Members retrieved successfully',
  })
  async listMembers(
    @Param('organizationId') organizationId: string,
    @Query('limit') limit?: string,
    @Query('offset') offset?: string,
  ) {
    const limitNum = Math.min(Math.max(parseInt(limit || '20', 10) || 20, 1), 100);
    const offsetNum = Math.max(parseInt(offset || '0', 10) || 0, 0);

    const result = await this.memberService.listMembersPaginated(organizationId, limitNum, offsetNum);
    return createListResponse(result.data, 'Members retrieved successfully', {
      total: result.total,
      limit: limitNum,
      offset: offsetNum,
      hasMore: offsetNum + limitNum < result.total,
    });
  }

  /**
   * Get member by ID
   */
  @Get(':organizationId/members/:memberId')
  @ApiOperation({
    summary: 'Get organization member',
    description: 'Get member details by ID',
  })
  @ApiParam({
    name: 'organizationId',
    description: 'Organization ID',
  })
  @ApiParam({
    name: 'memberId',
    description: 'Member ID',
  })
  @ApiResponse({
    status: 200,
    description: 'Member retrieved successfully',
  })
  async getMember(@Param('memberId') memberId: string) {
    const member = await this.memberService.getMember(memberId);
    return createSuccessResponse(member, 'Member retrieved successfully');
  }

  /**
   * Update member
   * AUTHORIZATION: Only OWNER can change roles. OWNER and ADMIN can update permissions.
   */
  @Patch(':organizationId/members/:memberId')
  @UseGuards(OrganizationRoleGuard)
  @RequireRole('OWNER', 'ADMIN')
  @ApiOperation({
    summary: 'Update organization member',
    description: 'Update member role (OWNER only), permissions, or status (OWNER/ADMIN)',
  })
  @ApiParam({
    name: 'organizationId',
    description: 'Organization ID',
  })
  @ApiParam({
    name: 'memberId',
    description: 'Member ID',
  })
  @ApiBody({ type: UpdateMemberDto })
  @ApiResponse({
    status: 200,
    description: 'Member updated successfully',
  })
  async updateMember(
    @Param('memberId') memberId: string,
    @Body() dto: UpdateMemberDto,
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    const member = await this.memberService.updateMember(memberId, dto, userId);
    return createSuccessResponse(member, 'Member updated successfully');
  }

  /**
   * Remove member from organization
   * AUTHORIZATION: Only OWNER and ADMIN can remove members
   */
  @Delete(':organizationId/members/:memberId')
  @UseGuards(OrganizationRoleGuard)
  @RequireRole('OWNER', 'ADMIN')
  @ApiOperation({
    summary: 'Remove organization member',
    description: 'Remove a member from the organization (OWNER/ADMIN only)',
  })
  @ApiParam({
    name: 'organizationId',
    description: 'Organization ID',
  })
  @ApiParam({
    name: 'memberId',
    description: 'Member ID',
  })
  @ApiResponse({
    status: 200,
    description: 'Member removed successfully',
  })
  async removeMember(@Param('memberId') memberId: string, @Request() req: ExpressRequestType) {
    const userId = req.user?.id;
    await this.memberService.removeMember(memberId, userId);
    return createDeleteResponse(memberId, 'Member removed successfully');
  }
}
