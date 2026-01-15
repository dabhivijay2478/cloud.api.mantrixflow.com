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
import { OrganizationOwnerRepository } from './repositories/organization-owner.repository';
import { OrganizationRepository } from './repositories/organization.repository';
import { OrganizationRoleService } from './services/organization-role.service';
import { UserRepository } from '../users/repositories/user.repository';

@Injectable()
export class OrganizationService {
  constructor(
    private readonly organizationRepository: OrganizationRepository,
    private readonly memberRepository: OrganizationMemberRepository,
    private readonly ownerRepository: OrganizationOwnerRepository,
    @Inject(forwardRef(() => UserRepository))
    private readonly userRepository: UserRepository,
    private readonly activityLogService: ActivityLogService,
    private readonly roleService: OrganizationRoleService,
  ) {}

  /**
   * Generate slug from name
   * Makes it unique by appending a timestamp if slug already exists
   */
  private async generateSlug(name: string): Promise<string> {
    const baseSlug = name
      .toLowerCase()
      .trim()
      .replace(/[^\w\s-]/g, '')
      .replace(/[\s_-]+/g, '-')
      .replace(/^-+|-+$/g, '');

    // Check if base slug exists, if so, make it unique
    const existing = await this.organizationRepository.findBySlug(baseSlug);
    if (!existing) {
      return baseSlug;
    }

    // Append timestamp to make it unique
    const timestamp = Date.now();
    const uniqueSlug = `${baseSlug}-${timestamp}`;

    // Double-check the unique slug doesn't exist (very unlikely but possible)
    const existingUnique = await this.organizationRepository.findBySlug(uniqueSlug);
    if (!existingUnique) {
      return uniqueSlug;
    }

    // Fallback: use random string if timestamp collision (extremely rare)
    const randomStr = Math.random().toString(36).substring(2, 10);
    return `${baseSlug}-${timestamp}-${randomStr}`;
  }

  /**
   * Check if a user is invited-only (i.e., all their organization memberships
   * are from invites, not from creating organizations)
   *
   * An invited-only user is one who:
   * - Has at least one organization membership
   * - Does NOT own any organizations (checked via organization_owners table)
   * - ALL of their memberships have invitedBy set (they were invited, not owners)
   *
   * @param userId The user ID to check
   * @returns true if user is invited-only, false otherwise
   */
  async isInvitedOnlyUser(userId: string): Promise<boolean> {
    // Check if user owns any organizations (most reliable check)
    const ownedOrgs = await this.ownerRepository.findByUserId(userId);
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
   *
   * @param skipRoleValidation - Skip validation for first-time users during onboarding
   */
  async createOrganization(
    userId: string,
    dto: CreateOrganizationDto,
    options?: { skipRoleValidation?: boolean },
  ): Promise<Organization> {
    // AUTHORIZATION CHECK: Block invited-only users from creating organizations
    // Skip this check for first-time users during onboarding
    if (!options?.skipRoleValidation) {
      const isInvitedOnly = await this.isInvitedOnlyUser(userId);
      if (isInvitedOnly) {
        throw new ForbiddenException(
          'Invited users are not allowed to create organizations. ' +
            'Only organization owners can create new organizations.',
        );
      }
    }

    // Generate unique slug (handles duplicates automatically)
    const slug = dto.slug || (await this.generateSlug(dto.name));

    // Create organization with owner_user_id set to the creator
    // Wrap in try-catch to handle potential race condition duplicates
    let organization: Organization;
    try {
      organization = await this.organizationRepository.create({
        name: dto.name,
        slug,
        description: dto.description,
        ownerUserId: userId, // Set the creator as the owner
        isActive: true,
      });
    } catch (error: any) {
      // Handle duplicate slug error (race condition)
      if (error?.code === '23505' && error?.constraint_name === 'organizations_slug_unique') {
        // Generate a new unique slug and try again
        const uniqueSlug = await this.generateSlug(dto.name);
        organization = await this.organizationRepository.create({
          name: dto.name,
          slug: uniqueSlug,
          description: dto.description,
          ownerUserId: userId,
          isActive: true,
        });
      } else {
        throw error;
      }
    }

    // Add the user as owner of the organization in the organization_owners table
    try {
      const user = await this.userRepository.findById(userId);
      if (user) {
        // Create ownership record in organization_owners table
        await this.ownerRepository.create({
          organizationId: organization.id,
          userId: userId,
        });

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
      console.error('Failed to add user as owner/member or set current organization:', error);
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
      console.error('Failed to log organization creation activity:', error);
    }

    return organization;
  }

  /**
   * List all organizations for a user
   * Returns organizations where the user is:
   * 1. An owner (from organization_owners table)
   * 2. A member (from organization_members table with active status)
   *
   * This ensures users see all organizations they have access to, whether they own them or are members
   *
   * Returns organizations with role/ownership information
   */
  async listOrganizations(
    userId: string,
  ): Promise<Array<Organization & { isOwner: boolean; role?: string }>> {
    // Get organizations where user is an owner
    const ownedOrgs = await this.ownerRepository.findByUserId(userId);
    const ownedOrgIds = new Set(ownedOrgs.map((o) => o.organizationId));

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
      console.error('Failed to log organization update activity:', error);
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

    // Get user's role in this organization
    const isOwner = await this.ownerRepository.isOwner(userId, organization.id);
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
      console.error('Failed to log organization selection activity:', error);
    }

    return organization;
  }
}
