/**
 * Organization Member Service
 * Business logic for organization member invites and management
 */

import {
  BadRequestException,
  ConflictException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createClient } from '@supabase/supabase-js';
import type { OrganizationMember } from '../../database/schemas/organizations';
import { ActivityLogService } from '../activity-logs/activity-log.service';
import {
  ENTITY_TYPES,
  USER_ACTIONS,
} from '../activity-logs/constants/activity-log-types';
import type { InviteMemberDto, UpdateMemberDto } from './dto/invite-member.dto';
import { OrganizationRepository } from './repositories/organization.repository';
import { OrganizationMemberRepository } from './repositories/organization-member.repository';

@Injectable()
export class OrganizationMemberService {
  private supabaseAdmin: ReturnType<typeof createClient> | null = null;

  constructor(
    private readonly memberRepository: OrganizationMemberRepository,
    private readonly organizationRepository: OrganizationRepository,
    private readonly configService: ConfigService,
    private readonly activityLogService: ActivityLogService,
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
   */
  async inviteMember(
    organizationId: string,
    invitedByUserId: string,
    dto: InviteMemberDto,
  ): Promise<OrganizationMember> {
    // Verify organization exists
    const organization = await this.organizationRepository.findById(organizationId);
    if (!organization) {
      throw new NotFoundException(`Organization with ID "${organizationId}" not found`);
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
        // Get frontend URL from config
        const frontendUrl = this.configService.get<string>('FRONTEND_URL', 'http://localhost:3000');

        // Send invite email via Supabase Auth
        // This uses Supabase's built-in invite functionality
        // redirectTo: When user clicks invite link, Supabase verify endpoint will redirect here
        // Supabase redirects with tokens in URL hash (#access_token=...), which must be handled client-side
        // IMPORTANT: redirectTo must match one of the allowed redirect URLs in Supabase dashboard
        const redirectTo = `${frontendUrl}/auth/accept-invite`;
        const { error } = await this.supabaseAdmin.auth.admin.inviteUserByEmail(email, {
          redirectTo,
          data: {
            organizationId,
            organizationName: organization.name,
            role: dto.role,
          },
        });

        if (error) {
          console.error('Failed to send invite email via Supabase:', error);
          // Don't fail the invite creation if email fails - invite record is still created
          // In production, you might want to queue this for retry
        }
      } catch (error) {
        console.error('Error sending invite email:', error);
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
      console.error('Failed to log user invite activity:', error);
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
   */
  async updateMember(
    id: string,
    dto: UpdateMemberDto,
    updatedByUserId?: string,
  ): Promise<OrganizationMember> {
    const member = await this.getMember(id);
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
        console.error('Failed to log role change activity:', error);
      }
    }

    return updated;
  }

  /**
   * Remove member from organization
   */
  async removeMember(id: string, removedByUserId?: string): Promise<void> {
    const member = await this.getMember(id); // Verify exists

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
      console.error('Failed to log user removal activity:', error);
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
        console.error('Failed to log invite acceptance activity:', error);
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
