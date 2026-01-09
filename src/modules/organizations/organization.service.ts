/**
 * Organization Service
 * Business logic for organization management
 */

import { BadRequestException, ForbiddenException, forwardRef, Inject, Injectable, NotFoundException } from '@nestjs/common';
import type { Organization } from '../../database/schemas/organizations';
import type { CreateOrganizationDto } from './dto/create-organization.dto';
import type { UpdateOrganizationDto } from './dto/update-organization.dto';
import { OrganizationMemberRepository } from './repositories/organization-member.repository';
import { OrganizationOwnerRepository } from './repositories/organization-owner.repository';
import { OrganizationRepository } from './repositories/organization.repository';
import { UserRepository } from '../users/repositories/user.repository';

@Injectable()
export class OrganizationService {
  constructor(
    private readonly organizationRepository: OrganizationRepository,
    private readonly memberRepository: OrganizationMemberRepository,
    private readonly ownerRepository: OrganizationOwnerRepository,
    @Inject(forwardRef(() => UserRepository))
    private readonly userRepository: UserRepository,
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
   */
  async createOrganization(userId: string, dto: CreateOrganizationDto): Promise<Organization> {
    // AUTHORIZATION CHECK: Block invited-only users from creating organizations
    const isInvitedOnly = await this.isInvitedOnlyUser(userId);
    if (isInvitedOnly) {
      throw new ForbiddenException(
        'Invited users are not allowed to create organizations. ' +
        'Only organization owners can create new organizations.'
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

    // Add the user as owner of the organization in the organization_owners table
    try {
      const user = await this.userRepository.findById(userId);
      if (user) {
        // Create ownership record in organization_owners table
        await this.ownerRepository.create({
          organizationId: organization.id,
          userId: userId,
        });

        // Create member record with owner role and active status
        // Note: This membership does NOT have invitedBy set, indicating the user created the org
        await this.memberRepository.create({
          organizationId: organization.id,
          userId: userId,
          email: user.email.toLowerCase(),
          role: 'owner',
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
  async listOrganizations(userId: string): Promise<Array<Organization & { isOwner: boolean; role?: string }>> {
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
      (org): org is Organization => org !== null && org.isActive === true
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
          role: isOwner ? 'owner' : role,
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
   */
  async updateOrganization(id: string, dto: UpdateOrganizationDto): Promise<Organization> {
    const organization = await this.getOrganization(id);

    // Check slug uniqueness if slug is being updated
    if (dto.slug && dto.slug !== organization.slug) {
      const existingOrg = await this.organizationRepository.findBySlug(dto.slug);
      if (existingOrg) {
        throw new BadRequestException(`Organization with slug "${dto.slug}" already exists`);
      }
    }

    return this.organizationRepository.update(id, dto);
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
   */
  async getCurrentOrganization(userId: string): Promise<Organization | null> {
    const user = await this.userRepository.findById(userId);
    if (!user || !user.currentOrgId) {
      return null;
    }
    return this.getOrganization(user.currentOrgId);
  }

  /**
   * Set current organization for a user
   */
  async setCurrentOrganization(userId: string, id: string): Promise<Organization> {
    // Verify organization exists
    const organization = await this.getOrganization(id);
    
    // Verify user is a member of this organization
    const member = await this.memberRepository.findByOrganizationAndUserId(id, userId);
    if (!member) {
      throw new BadRequestException(`User is not a member of organization "${id}"`);
    }

    // Set as current organization
    await this.userRepository.setCurrentOrganization(userId, id);
    
    return organization;
  }
}
