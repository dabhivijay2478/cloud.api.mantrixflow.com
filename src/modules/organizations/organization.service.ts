/**
 * Organization Service
 * Business logic for organization management
 */

import {
  BadRequestException,
  ForbiddenException,
  forwardRef,
  Inject,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import type { Organization } from '../../database/schemas/organizations';
import { ActivityLogService } from '../activity-logs/activity-log.service';
import { ENTITY_TYPES, ORG_ACTIONS } from '../activity-logs/constants/activity-log-types';
import type { CreateOrganizationDto } from './dto/create-organization.dto';
import type { UpdateOrganizationDto } from './dto/update-organization.dto';
import { OrganizationMemberRepository } from './repositories/organization-member.repository';
import { OrganizationRepository } from './repositories/organization.repository';
import { OrganizationRoleService } from './services/organization-role.service';
import { UserRepository } from '../users/repositories/user.repository';

@Injectable()
export class OrganizationService {
  constructor(
    private readonly organizationRepository: OrganizationRepository,
    private readonly memberRepository: OrganizationMemberRepository,
    @Inject(forwardRef(() => UserRepository))
    private readonly userRepository: UserRepository,
    private readonly activityLogService: ActivityLogService,
    private readonly roleService: OrganizationRoleService,
  ) {}

  /**
   * Generate slug from name
   */
  private generateSlug(name: string): string {
    return name
      .toLowerCase()
      .trim()
      .replace(/[^\w\s-]/g, '')
      .replace(/[\s_-]+/g, '-')
      .replace(/^-+|-+$/g, '');
  }

  /**
   * Check if a user is invited-only (i.e., all their organization memberships
   * are from invites, not from creating organizations)
   *
   * An invited-only user is one who:
   * - Has at least one organization membership
   * - Does NOT own any organizations (checked via owner_user_id in organizations table)
   * - ALL of their memberships have invitedBy set (they were invited, not owners)
   *
   * @param userId The user ID to check
   * @returns true if user is invited-only, false otherwise
   */
  async isInvitedOnlyUser(userId: string): Promise<boolean> {
    // Check if user owns any organizations (using owner_user_id in organizations table)
    const ownedOrgs = await this.organizationRepository.findByOwnerUserId(userId);
    if (ownedOrgs.length > 0) {
      // User owns at least one organization, so they're not invited-only
      return false;
    }

    // Get all organization memberships for this user
    const memberships = await this.memberRepository.findByUserId(userId);

    // If user has no memberships, they're not invited-only (they're a new user)
    if (memberships.length === 0) {
      return false;
    }

    // Check if ALL memberships have invitedBy set (meaning user was invited to all)
    // If any membership has no invitedBy, it means they created that organization
    const allInvited = memberships.every((membership) => membership.invitedBy !== null);

    return allInvited;
  }

  /**
   * Create organization
   *
   * Authorization Rules:
   * - Only users who are NOT invited-only can create organizations
   * - Invited users (members invited to existing organizations) cannot create new organizations
   * - The creating user becomes the OWNER of the organization
   */
  async createOrganization(userId: string, dto: CreateOrganizationDto): Promise<Organization> {
    // AUTHORIZATION CHECK: Block invited-only users from creating organizations
    const isInvitedOnly = await this.isInvitedOnlyUser(userId);
    if (isInvitedOnly) {
      throw new ForbiddenException(
        'Invited users are not allowed to create organizations. ' +
          'Only organization owners can create new organizations.',
      );
    }

    const slug = dto.slug || this.generateSlug(dto.name);

    // Check if slug already exists
    const existingOrg = await this.organizationRepository.findBySlug(slug);
    if (existingOrg) {
      throw new BadRequestException(`Organization with slug "${slug}" already exists`);
    }

    // Create organization with owner_user_id set to the creator
    const organization = await this.organizationRepository.create({
      name: dto.name,
      slug,
      description: dto.description,
      ownerUserId: userId, // Set the creator as the owner
      isActive: true,
    });

    // Add the user as member with OWNER role
    try {
      const user = await this.userRepository.findById(userId);
      if (user) {
        // Create member record with OWNER role and active status
        // Note: This membership does NOT have invitedBy set, indicating the user created the org
        await this.memberRepository.create({
          organizationId: organization.id,
          userId: userId,
          email: user.email.toLowerCase(),
          role: 'OWNER',
          status: 'active',
          acceptedAt: new Date(),
          // invitedBy is intentionally NOT set - this indicates the user created the organization
        });

        // Set this organization as the user's current organization
        await this.userRepository.setCurrentOrganization(userId, organization.id);
      }
    } catch (error) {
      // Log error but don't fail organization creation
      this.activityLogService.logger.error(
        'Failed to add user as member or set current organization',
        error instanceof Error ? error.stack : String(error),
      );
    }

    // Log activity
    try {
      await this.activityLogService.logActivity({
        organizationId: organization.id,
        userId,
        actionType: ORG_ACTIONS.CREATED,
        entityType: ENTITY_TYPES.ORGANIZATION,
        entityId: organization.id,
        message: `Organization "${organization.name}" created`,
        metadata: {
          name: organization.name,
          slug: organization.slug,
        },
      });
    } catch (error) {
      // Don't fail organization creation if logging fails
      this.activityLogService.logger.error(
        'Failed to log organization creation activity',
        error instanceof Error ? error.stack : String(error),
      );
    }

    return organization;
  }

  /**
   * List all organizations for a user
   * Returns organizations where the user is:
   * 1. An owner (from owner_user_id in organizations table)
   * 2. A member (from organization_members table with active status)
   *
   * This ensures users see all organizations they have access to, whether they own them or are members
   *
   * Returns organizations with role/ownership information
   */
  async listOrganizations(
    userId: string,
  ): Promise<Array<Organization & { isOwner: boolean; role?: string }>> {
    // Get organizations where user is an owner (using owner_user_id)
    const ownedOrgs = await this.organizationRepository.findByOwnerUserId(userId);
    const ownedOrgIds = new Set(ownedOrgs.map((o) => o.id));

    // Get organizations where user is a member (active memberships)
    const activeMembers = await this.memberRepository.findActiveMembershipsByUserId(userId);
    const memberOrgIds = new Set(activeMembers.map((m) => m.organizationId));
    const memberRoleMap = new Map<string, string>();
    activeMembers.forEach((m) => {
      memberRoleMap.set(m.organizationId, m.role);
    });

    // Combine both sets to get all unique organization IDs
    const allOrgIds = new Set([...ownedOrgIds, ...memberOrgIds]);

    if (allOrgIds.size === 0) {
      return [];
    }

    // Fetch all organizations
    const organizations = await Promise.all(
      Array.from(allOrgIds).map((id) => this.organizationRepository.findById(id)),
    );

    // Filter out null values and only return active organizations
    const activeOrganizations = organizations.filter(
      (org): org is Organization => org !== null && org.isActive === true,
    );

    // Return unique organizations with role/ownership information
    const uniqueOrgs = new Map<string, Organization & { isOwner: boolean; role?: string }>();
    activeOrganizations.forEach((org) => {
      if (!uniqueOrgs.has(org.id)) {
        const isOwner = ownedOrgIds.has(org.id);
        const role = memberRoleMap.get(org.id);
        uniqueOrgs.set(org.id, {
          ...org,
          isOwner,
          role: isOwner ? 'OWNER' : (role as string | undefined),
        });
      }
    });

    return Array.from(uniqueOrgs.values());
  }

  /**
   * Get organization by ID
   */
  async getOrganization(id: string): Promise<Organization> {
    const organization = await this.organizationRepository.findById(id);
    if (!organization) {
      throw new NotFoundException(`Organization with ID "${id}" not found`);
    }
    return organization;
  }

  /**
   * Get organization by slug
   */
  async getOrganizationBySlug(slug: string): Promise<Organization> {
    const organization = await this.organizationRepository.findBySlug(slug);
    if (!organization) {
      throw new NotFoundException(`Organization with slug "${slug}" not found`);
    }
    return organization;
  }

  /**
   * Update organization
   *
   * AUTHORIZATION: Only OWNER can update organization details
   */
  async updateOrganization(
    id: string,
    dto: UpdateOrganizationDto,
    userId?: string,
  ): Promise<Organization> {
    const organization = await this.getOrganization(id);

    // AUTHORIZATION CHECK: Only OWNER can update organization details
    if (userId) {
      const canUpdate = await this.roleService.canUpdateOrganization(userId, id);
      if (!canUpdate) {
        throw new ForbiddenException('Only OWNER can update organization details');
      }
    }

    // Check slug uniqueness if slug is being updated
    if (dto.slug && dto.slug !== organization.slug) {
      const existingOrg = await this.organizationRepository.findBySlug(dto.slug);
      if (existingOrg) {
        throw new BadRequestException(`Organization with slug "${dto.slug}" already exists`);
      }
    }

    const updated = await this.organizationRepository.update(id, dto);

    // Log activity
    try {
      const changes: string[] = [];
      if (dto.name && dto.name !== organization.name) {
        changes.push(`name: "${organization.name}" → "${dto.name}"`);
      }
      if (dto.description !== undefined && dto.description !== organization.description) {
        changes.push('description updated');
      }
      if (dto.slug && dto.slug !== organization.slug) {
        changes.push(`slug: "${organization.slug}" → "${dto.slug}"`);
      }

      await this.activityLogService.logActivity({
        organizationId: id,
        userId: userId || null,
        actionType: ORG_ACTIONS.UPDATED,
        entityType: ENTITY_TYPES.ORGANIZATION,
        entityId: id,
        message:
          changes.length > 0
            ? `Organization updated: ${changes.join(', ')}`
            : 'Organization updated',
        metadata: {
          changes: dto,
        },
      });
    } catch (error) {
      // Don't fail update if logging fails
      this.activityLogService.logger.error(
        'Failed to log organization update activity',
        error instanceof Error ? error.stack : String(error),
      );
    }

    return updated;
  }

  /**
   * Delete organization
   */
  async deleteOrganization(id: string): Promise<void> {
    await this.getOrganization(id); // Verify it exists
    await this.organizationRepository.delete(id);
  }

  /**
   * Get current organization for a user
   * Returns organization with user's role information
   */
  async getCurrentOrganization(
    userId: string,
  ): Promise<(Organization & { isOwner: boolean; role?: string }) | null> {
    const user = await this.userRepository.findById(userId);
    if (!user || !user.currentOrgId) {
      return null;
    }

    const organization = await this.getOrganization(user.currentOrgId);

    // Check if user is owner (using owner_user_id)
    const isOwner = organization.ownerUserId === userId;
    let role: string | undefined;

    if (isOwner) {
      role = 'OWNER';
    } else {
      const member = await this.memberRepository.findByOrganizationAndUserId(
        organization.id,
        userId,
      );
      if (member && (member.status === 'active' || member.status === 'accepted')) {
        role = member.role;
      }
    }

    return {
      ...organization,
      isOwner,
      role,
    };
  }

  /**
   * Set current organization for a user
   *
   * AUTHORIZATION: User must be a member of the organization
   */
  async setCurrentOrganization(userId: string, id: string): Promise<Organization> {
    // Verify organization exists
    const organization = await this.getOrganization(id);

    // AUTHORIZATION: Verify user is a member of this organization
    const userRole = await this.roleService.getUserRole(userId, id);
    if (!userRole) {
      throw new ForbiddenException(`You are not a member of this organization`);
    }

    // Set as current organization
    await this.userRepository.setCurrentOrganization(userId, id);

    // Log activity
    try {
      await this.activityLogService.logActivity({
        organizationId: id,
        userId,
        actionType: ORG_ACTIONS.SELECTED,
        entityType: ENTITY_TYPES.ORGANIZATION,
        entityId: id,
        message: `Organization "${organization.name}" selected`,
        metadata: {
          organizationName: organization.name,
        },
      });
    } catch (error) {
      // Don't fail organization switch if logging fails
      this.activityLogService.logger.error(
        'Failed to log organization selection activity',
        error instanceof Error ? error.stack : String(error),
      );
    }

    return organization;
  }

  /**
   * Transfer organization ownership
   *
   * AUTHORIZATION: Only current OWNER can transfer ownership
   * The new owner must be a member of the organization
   */
  async transferOwnership(
    organizationId: string,
    newOwnerId: string,
    currentOwnerId: string,
  ): Promise<Organization> {
    // Verify organization exists
    const organization = await this.getOrganization(organizationId);

    // AUTHORIZATION: Verify current user is the owner
    if (organization.ownerUserId !== currentOwnerId) {
      throw new ForbiddenException('Only the current owner can transfer ownership');
    }

    // Verify new owner is not the same as current owner
    if (newOwnerId === currentOwnerId) {
      throw new BadRequestException('New owner must be different from current owner');
    }

    // Verify new owner is a member of the organization
    const newOwnerMember = await this.memberRepository.findByOrganizationAndUserId(
      organizationId,
      newOwnerId,
    );
    if (
      !newOwnerMember ||
      (newOwnerMember.status !== 'active' && newOwnerMember.status !== 'accepted')
    ) {
      throw new BadRequestException(
        'New owner must be an active member of the organization. Please invite them first.',
      );
    }

    // Get old owner member record (if exists)
    const oldOwnerMember = await this.memberRepository.findByOrganizationAndUserId(
      organizationId,
      currentOwnerId,
    );

    // Update organization owner
    const updated = await this.organizationRepository.update(organizationId, {
      ownerUserId: newOwnerId,
    });

    // Update member roles:
    // 1. Set new owner's role to OWNER (or create member record if doesn't exist)
    if (newOwnerMember) {
      await this.memberRepository.update(newOwnerMember.id, {
        role: 'OWNER',
      });
    } else {
      // Get user details to get email
      const newOwnerUser = await this.userRepository.findById(newOwnerId);
      if (!newOwnerUser || !newOwnerUser.email) {
        throw new BadRequestException(
          `Cannot transfer ownership: User ${newOwnerId} not found or has no email`,
        );
      }

      // Create OWNER member record for new owner
      await this.memberRepository.create({
        organizationId,
        userId: newOwnerId,
        email: newOwnerUser.email.toLowerCase(),
        role: 'OWNER',
        status: 'active',
        acceptedAt: new Date(),
      });
    }

    // 2. Update old owner's role to ADMIN (if they have a member record)
    if (oldOwnerMember && oldOwnerMember.role === 'OWNER') {
      await this.memberRepository.update(oldOwnerMember.id, {
        role: 'ADMIN',
      });
    }

    // Get user details for logging
    const oldOwnerUser = await this.userRepository.findById(currentOwnerId);
    const newOwnerUser = await this.userRepository.findById(newOwnerId);

    // Log activity
    try {
      await this.activityLogService.logActivity({
        organizationId,
        userId: currentOwnerId,
        actionType: ORG_ACTIONS.OWNERSHIP_TRANSFERRED,
        entityType: ENTITY_TYPES.ORGANIZATION,
        entityId: organizationId,
        message: `Ownership transferred from ${oldOwnerUser?.email || currentOwnerId} to ${newOwnerUser?.email || newOwnerId}`,
        metadata: {
          oldOwnerId: currentOwnerId,
          oldOwnerEmail: oldOwnerUser?.email,
          newOwnerId,
          newOwnerEmail: newOwnerUser?.email,
        },
      });
    } catch (error) {
      // Don't fail transfer if logging fails
      this.activityLogService.logger.error(
        'Failed to log ownership transfer activity',
        error instanceof Error ? error.stack : String(error),
      );
    }

    return updated;
  }
}
