/**
 * Organization Member Controller
 * REST API endpoints for organization member invites and management
 */

import {
  Controller,
  Post,
  Get,
  Patch,
  Delete,
  Body,
  Param,
  HttpCode,
  HttpStatus,
  Request,
  BadRequestException,
} from '@nestjs/common';
import type { Request as ExpressRequest } from 'express';
type Request = ExpressRequest;
import {
  ApiTags,
  ApiOperation,
  ApiResponse,
  ApiParam,
  ApiBody,
  ApiBearerAuth,
} from '@nestjs/swagger';
import { UseGuards } from '@nestjs/common';
import { OrganizationMemberService } from './organization-member.service';
import { InviteMemberDto, UpdateMemberDto } from './dto/invite-member.dto';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import {
  ApiSuccessResponse,
  ApiListResponse,
  ApiDeleteResponse,
  createSuccessResponse,
  createListResponse,
  createDeleteResponse,
} from '../../common/dto/api-response.dto';

@ApiTags('organizations')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard)
@Controller('api/organizations')
export class OrganizationMemberController {
  constructor(
    private readonly memberService: OrganizationMemberService,
  ) {}

  /**
   * Invite a member to an organization
   */
  @Post(':organizationId/members/invite')
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: 'Invite member to organization',
    description: 'Invite a user to join the organization by email',
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
    status: 409,
    description: 'User has already been invited',
  })
  async inviteMember(
    @Param('organizationId') organizationId: string,
    @Body() dto: InviteMemberDto,
    @Request() req: Request,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new BadRequestException('User ID is required');
    }

    // Validate agent panel access
    if (dto.agentPanelAccess && (!dto.allowedModels || dto.allowedModels.length === 0)) {
      throw new BadRequestException(
        'At least one model must be selected if agent panel access is enabled',
      );
    }

    const member = await this.memberService.inviteMember(
      organizationId,
      userId,
      dto,
    );
    return createSuccessResponse(
      member,
      'Member invited successfully',
      201,
    );
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
  async listMembers(@Param('organizationId') organizationId: string) {
    const members = await this.memberService.listMembers(organizationId);
    return createListResponse(members, 'Members retrieved successfully');
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
   */
  @Patch(':organizationId/members/:memberId')
  @ApiOperation({
    summary: 'Update organization member',
    description: 'Update member role, permissions, or status',
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
  ) {
    const member = await this.memberService.updateMember(memberId, dto);
    return createSuccessResponse(member, 'Member updated successfully');
  }

  /**
   * Remove member from organization
   */
  @Delete(':organizationId/members/:memberId')
  @ApiOperation({
    summary: 'Remove organization member',
    description: 'Remove a member from the organization',
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
  async removeMember(@Param('memberId') memberId: string) {
    await this.memberService.removeMember(memberId);
    return createDeleteResponse(memberId, 'Member removed successfully');
  }
}

