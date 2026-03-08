/**
 * Organization Member Service
 * Business logic for organization member invites and management
 */

import {
  BadRequestException,
  ConflictException,
  ForbiddenException,
  Injectable,
  Logger,
  NotFoundException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createClient } from '@supabase/supabase-js';
import type { OrganizationMember } from '../../database/schemas/organizations';
import { ActivityLogService } from '../activity-logs/activity-log.service';
import { EmailService } from '../email/email.service';
import { UserRepository } from '../users/repositories/user.repository';
import { ENTITY_TYPES, USER_ACTIONS } from '../activity-logs/constants/activity-log-types';
import type { InviteMemberDto, UpdateMemberDto } from './dto/invite-member.dto';
import { OrganizationRepository } from './repositories/organization.repository';
import { OrganizationMemberRepository } from './repositories/organization-member.repository';
import { OrganizationRoleService } from './services/organization-role.service';

@Injectable()
export class OrganizationMemberService {
  private readonly logger = new Logger(OrganizationMemberService.name);
  private supabaseAdmin: ReturnType<typeof createClient> | null = null;

  constructor(
    private readonly memberRepository: OrganizationMemberRepository,
    private readonly organizationRepository: OrganizationRepository,
    private readonly configService: ConfigService,
    private readonly activityLogService: ActivityLogService,
    private readonly roleService: OrganizationRoleService,
    private readonly emailService: EmailService,
    private readonly userRepository: UserRepository,
  ) {
    // Initialize Supabase admin client for sending invite emails
    const supabaseUrl = this.configService.get<string>('SUPABASE_URL');
    const supabaseServiceRoleKey = this.configService.get<string>('SUPABASE_SERVICE_ROLE_KEY');

    if (supabaseUrl && supabaseServiceRoleKey) {
      this.supabaseAdmin = createClient(supabaseUrl, supabaseServiceRoleKey, {
        auth: {
          autoRefreshToken: false,
          persistSession: false,
        },
      });
    }
  }

  /**
   * Invite a user to an organization
   * Creates an invite record and sends an email via Supabase
   *
   * AUTHORIZATION: Only OWNER and ADMIN can invite members
   */
  async inviteMember(
    organizationId: string,
    invitedByUserId: string,
    dto: InviteMemberDto,
  ): Promise<OrganizationMember> {
    // AUTHORIZATION CHECK: Only OWNER and ADMIN can invite
    const canInvite = await this.roleService.canInviteMembers(invitedByUserId, organizationId);
    if (!canInvite) {
      throw new ForbiddenException('Only OWNER and ADMIN can invite members to the organization');
    }

    // Verify organization exists
    const organization = await this.organizationRepository.findById(organizationId);
    if (!organization) {
      throw new NotFoundException(`Organization with ID "${organizationId}" not found`);
    }

    // AUTHORIZATION: Cannot invite users as OWNER (only one owner per org)
    if (dto.role === 'OWNER') {
      throw new BadRequestException(
        'Cannot invite users as OWNER. Each organization can have only one OWNER.',
      );
    }

    // Normalize email
    const email = dto.email.toLowerCase().trim();

    // Check for existing active invite
    const existingInvite = await this.memberRepository.findActiveInviteByEmail(
      organizationId,
      email,
    );
    if (existingInvite) {
      throw new ConflictException(
        `User with email "${email}" has already been invited to this organization`,
      );
    }

    // Create invite record
    const member = await this.memberRepository.create({
      organizationId,
      email,
      role: dto.role,
      status: 'invited',
      invitedBy: invitedByUserId,
      agentPanelAccess: dto.agentPanelAccess || false,
      allowedModels: dto.allowedModels || [],
    });

    // Send invite email via Supabase
    // Note: This requires Supabase to be configured with email templates
    if (this.supabaseAdmin) {
      try {
        const frontendUrl = this.configService.get<string>('FRONTEND_URL');
        if (!frontendUrl) {
          this.logger.warn(
            'FRONTEND_URL not set in environment; set it in apps/api/.env for invite email redirects',
          );
        }
        // redirectTo must match allowed redirect URLs in Supabase dashboard
        const redirectTo = frontendUrl ? `${frontendUrl}/auth/accept-invite` : undefined;
        const { error } = await this.supabaseAdmin.auth.admin.inviteUserByEmail(email, {
          ...(redirectTo && { redirectTo }),
          data: {
            organizationId,
            organizationName: organization.name,
            role: dto.role,
          },
        });

        if (error) {
          this.logger.error(
            'Failed to send invite email via Supabase',
            error instanceof Error ? error.stack : String(error),
          );
          // Don't fail the invite creation if email fails - invite record is still created
          // In production, you might want to queue this for retry
        }
      } catch (error) {
        this.logger.error(
          'Error sending invite email',
          error instanceof Error ? error.stack : String(error),
        );
        // Continue even if email fails
      }
    }

    // Log activity
    try {
      await this.activityLogService.logActivity({
        organizationId,
        userId: invitedByUserId,
        actionType: USER_ACTIONS.INVITED,
        entityType: ENTITY_TYPES.USER,
        entityId: null, // User doesn't exist yet
        message: `User "${email}" invited to organization with role "${dto.role}"`,
        metadata: {
          email,
          role: dto.role,
          invitedBy: invitedByUserId,
        },
      });
    } catch (error) {
      // Don't fail invite if logging fails
      this.logger.error(
        'Failed to log user invite activity',
        error instanceof Error ? error.stack : String(error),
      );
    }

    return member;
  }

  /**
   * List all members for an organization
   */
  async listMembers(organizationId: string): Promise<OrganizationMember[]> {
    // Verify organization exists
    const organization = await this.organizationRepository.findById(organizationId);
    if (!organization) {
      throw new NotFoundException(`Organization with ID "${organizationId}" not found`);
    }

    return this.memberRepository.findByOrganizationId(organizationId);
  }

  /**
   * List members with pagination
   */
  async listMembersPaginated(organizationId: string, limit: number = 20, offset: number = 0) {
    const organization = await this.organizationRepository.findById(organizationId);
    if (!organization) {
      throw new NotFoundException(`Organization with ID "${organizationId}" not found`);
    }

    return this.memberRepository.findByOrganizationIdPaginated(organizationId, limit, offset);
  }

  /**
   * Get member by ID
   */
  async getMember(id: string): Promise<OrganizationMember> {
    const member = await this.memberRepository.findById(id);
    if (!member) {
      throw new NotFoundException(`Member with ID "${id}" not found`);
    }
    return member;
  }

  /**
   * Update member
   *
   * AUTHORIZATION:
   * - Only OWNER can change roles
   * - Only OWNER and ADMIN can update member permissions
   */
  async updateMember(
    id: string,
    dto: UpdateMemberDto,
    updatedByUserId?: string,
  ): Promise<OrganizationMember> {
    const member = await this.getMember(id);

    if (updatedByUserId) {
      // AUTHORIZATION: Check if user can update this member
      const userRole = await this.roleService.getUserRole(updatedByUserId, member.organizationId);

      if (!userRole) {
        throw new ForbiddenException('You are not a member of this organization');
      }

      // AUTHORIZATION: Only OWNER can change roles
      if (dto.role && dto.role !== member.role) {
        if (userRole !== 'OWNER') {
          throw new ForbiddenException('Only OWNER can change member roles');
        }

        // AUTHORIZATION: Cannot change role to OWNER (only one owner per org)
        if (dto.role === 'OWNER') {
          throw new BadRequestException(
            'Cannot change member role to OWNER. Each organization can have only one OWNER.',
          );
        }
      }

      // AUTHORIZATION: Only OWNER and ADMIN can update member permissions
      if (dto.agentPanelAccess !== undefined || dto.allowedModels !== undefined) {
        if (userRole !== 'OWNER' && userRole !== 'ADMIN') {
          throw new ForbiddenException('Only OWNER and ADMIN can update member permissions');
        }
      }
    }

    const updated = await this.memberRepository.update(id, dto);

    // Log activity if role changed
    if (dto.role && dto.role !== member.role) {
      try {
        await this.activityLogService.logActivity({
          organizationId: member.organizationId,
          userId: updatedByUserId || null,
          actionType: USER_ACTIONS.ROLE_CHANGED,
          entityType: ENTITY_TYPES.USER,
          entityId: member.userId || null,
          message: `User role changed from "${member.role}" to "${dto.role}"`,
          metadata: {
            email: member.email,
            oldRole: member.role,
            newRole: dto.role,
            memberId: id,
          },
        });
      } catch (error) {
        // Don't fail update if logging fails
        this.logger.error(
          'Failed to log role change activity',
          error instanceof Error ? error.stack : String(error),
        );
      }
    }

    return updated;
  }

  /**
   * Remove member from organization
   *
   * AUTHORIZATION: Only OWNER and ADMIN can remove members
   */
  async removeMember(id: string, removedByUserId?: string): Promise<void> {
    const member = await this.getMember(id); // Verify exists

    if (removedByUserId) {
      // AUTHORIZATION: Only OWNER and ADMIN can remove members
      const canRemove = await this.roleService.canRemoveMembers(
        removedByUserId,
        member.organizationId,
      );
      if (!canRemove) {
        throw new ForbiddenException(
          'Only OWNER and ADMIN can remove members from the organization',
        );
      }

      // AUTHORIZATION: Cannot remove OWNER
      if (member.role === 'OWNER') {
        throw new BadRequestException(
          'Cannot remove OWNER from organization. Transfer ownership first.',
        );
      }
    }

    // Log activity before deletion
    try {
      await this.activityLogService.logActivity({
        organizationId: member.organizationId,
        userId: removedByUserId || null,
        actionType: USER_ACTIONS.REMOVED,
        entityType: ENTITY_TYPES.USER,
        entityId: member.userId || null,
        message: `User "${member.email}" removed from organization`,
        metadata: {
          email: member.email,
          role: member.role,
          memberId: id,
        },
      });
    } catch (error) {
      // Don't fail removal if logging fails
      this.logger.error(
        'Failed to log user removal activity',
        error instanceof Error ? error.stack : String(error),
      );
    }

    // Send member_removed email to the removed user
    try {
      const organization = await this.organizationRepository.findById(member.organizationId);
      const frontendUrl = this.configService.get<string>('FRONTEND_URL') ?? '';
      const dashboardUrl = `${frontendUrl}/workspace`;
      const removedUser = member.userId
        ? await this.userRepository.findById(member.userId)
        : null;
      await this.emailService.sendMemberRemoved({
        recipientEmail: member.email,
        firstName: removedUser?.firstName ?? null,
        orgName: organization?.name ?? 'the organization',
        dashboardUrl,
        userId: member.userId ?? '',
      });
    } catch (error) {
      this.logger.error(
        'Failed to send member removed email',
        error instanceof Error ? error.stack : String(error),
      );
    }

    await this.memberRepository.delete(id);
  }

  /**
   * Link a Supabase user to an invite when they sign up
   * This is called from the user service when a new user signs up
   */
  async linkUserToInvite(email: string, userId: string): Promise<OrganizationMember[]> {
    // Find all pending invites for this email
    const invites = await this.memberRepository.findAllInvitesByEmail(email);

    if (invites.length === 0) {
      return []; // No invites found
    }

    // Link user to all invites and update status
    const updatedMembers: OrganizationMember[] = [];
    for (const invite of invites) {
      const updated = await this.memberRepository.linkUserToInvite(invite.id, userId);

      // Log activity for invite acceptance
      try {
        await this.activityLogService.logActivity({
          organizationId: invite.organizationId,
          userId,
          actionType: USER_ACTIONS.INVITE_ACCEPTED,
          entityType: ENTITY_TYPES.USER,
          entityId: userId,
          message: `User accepted invite to organization`,
          metadata: {
            email: invite.email,
            role: invite.role,
            memberId: updated.id,
          },
        });
      } catch (error) {
        // Don't fail invite acceptance if logging fails
        this.logger.error(
          'Failed to log invite acceptance activity',
          error instanceof Error ? error.stack : String(error),
        );
      }

      updatedMembers.push(updated);
    }

    return updatedMembers;
  }

  /**
   * Find all invites by email (across all organizations)
   * Used when user signs up to check if they were invited
   */
  async findAllInvitesByEmail(email: string): Promise<OrganizationMember[]> {
    return this.memberRepository.findAllInvitesByEmail(email);
  }

  /**
   * Activate a member (change status from accepted to active)
   * This is typically called after user completes onboarding
   */
  async activateMember(id: string): Promise<OrganizationMember> {
    const member = await this.getMember(id);

    if (member.status === 'active') {
      return member; // Already active
    }

    if (member.status !== 'accepted') {
      throw new BadRequestException(
        `Cannot activate member with status "${member.status}". Member must be accepted first.`,
      );
    }

    return this.memberRepository.updateStatus(id, 'active');
  }
}
