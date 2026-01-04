/**
 * Organization Member Service
 * Business logic for organization member invites and management
 */

import {
  Injectable,
  NotFoundException,
  BadRequestException,
  ConflictException,
} from '@nestjs/common';
import { OrganizationMemberRepository } from './repositories/organization-member.repository';
import { OrganizationRepository } from './repositories/organization.repository';
import { InviteMemberDto, UpdateMemberDto } from './dto/invite-member.dto';
import type { OrganizationMember } from '../../database/schemas/organizations';
import { createClient } from '@supabase/supabase-js';

@Injectable()
export class OrganizationMemberService {
  private supabaseAdmin: ReturnType<typeof createClient> | null = null;

  constructor(
    private readonly memberRepository: OrganizationMemberRepository,
    private readonly organizationRepository: OrganizationRepository,
  ) {
    // Initialize Supabase admin client for sending invite emails
    const supabaseUrl = process.env.SUPABASE_URL;
    const supabaseServiceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

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
      throw new NotFoundException(
        `Organization with ID "${organizationId}" not found`,
      );
    }

    // Normalize email
    const email = dto.email.toLowerCase().trim();

    // Check for existing active invite
    const existingInvite =
      await this.memberRepository.findActiveInviteByEmail(organizationId, email);
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
        // Generate invite link - Supabase will redirect to accept-invite page
        const inviteUrl = `${process.env.FRONTEND_URL || 'http://localhost:3000'}/auth/accept-invite`;

        // Send invite email via Supabase Auth
        // This uses Supabase's built-in invite functionality
        // redirectTo: When user clicks invite link, Supabase verify endpoint will redirect here
        // The callback route will handle token/code verification and redirect to accept-invite
        const redirectTo = `${process.env.FRONTEND_URL || 'http://localhost:3000'}/auth/callback?type=invite`;
        const { error } = await this.supabaseAdmin.auth.admin.inviteUserByEmail(
          email,
          {
            redirectTo,
            data: {
              organizationId,
              organizationName: organization.name,
              role: dto.role,
            },
          },
        );

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

    return member;
  }

  /**
   * List all members for an organization
   */
  async listMembers(organizationId: string): Promise<OrganizationMember[]> {
    // Verify organization exists
    const organization = await this.organizationRepository.findById(organizationId);
    if (!organization) {
      throw new NotFoundException(
        `Organization with ID "${organizationId}" not found`,
      );
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
  ): Promise<OrganizationMember> {
    const member = await this.getMember(id);

    return this.memberRepository.update(id, dto);
  }

  /**
   * Remove member from organization
   */
  async removeMember(id: string): Promise<void> {
    await this.getMember(id); // Verify exists
    await this.memberRepository.delete(id);
  }

  /**
   * Link a Supabase user to an invite when they sign up
   * This is called from the user service when a new user signs up
   */
  async linkUserToInvite(
    email: string,
    userId: string,
  ): Promise<OrganizationMember[]> {
    // Find all pending invites for this email
    const invites = await this.memberRepository.findAllInvitesByEmail(email);

    if (invites.length === 0) {
      return []; // No invites found
    }

    // Link user to all invites and update status
    const updatedMembers: OrganizationMember[] = [];
    for (const invite of invites) {
      const updated = await this.memberRepository.linkUserToInvite(
        invite.id,
        userId,
      );
      updatedMembers.push(updated);
    }

    return updatedMembers;
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

