/**
 * Organization Role Service
 * Provides utilities for checking user roles and permissions in organizations
 * This is the AUTHORITATIVE source for role-based permission checks
 */

import { Injectable } from '@nestjs/common';
import type { OrganizationRole } from '../../../common/guards/organization-role.guard';
import { OrganizationMemberRepository } from '../repositories/organization-member.repository';
import { OrganizationOwnerRepository } from '../repositories/organization-owner.repository';

@Injectable()
export class OrganizationRoleService {
  constructor(
    private readonly memberRepository: OrganizationMemberRepository,
    private readonly ownerRepository: OrganizationOwnerRepository,
  ) {}

  /**
   * Get user's role in an organization
   * Returns null if user is not a member
   */
  async getUserRole(
    userId: string,
    organizationId: string,
  ): Promise<OrganizationRole | null> {
    // Check if user is owner (from organization_owners table)
    const isOwner = await this.ownerRepository.isOwner(userId, organizationId);
    if (isOwner) {
      return 'OWNER';
    }

    // Check membership and get role
    const member = await this.memberRepository.findByOrganizationAndUserId(
      organizationId,
      userId,
    );

    if (!member) {
      return null;
    }

    // Only return role if member is active or accepted
    if (member.status === 'active' || member.status === 'accepted') {
      return member.role as OrganizationRole;
    }

    return null;
  }

  /**
   * Check if user has a specific role or higher
   * Role hierarchy: OWNER > ADMIN > EDITOR > VIEWER
   */
  async hasRoleOrHigher(
    userId: string,
    organizationId: string,
    requiredRole: OrganizationRole,
  ): Promise<boolean> {
    const userRole = await this.getUserRole(userId, organizationId);
    if (!userRole) {
      return false;
    }

    const roleHierarchy: Record<OrganizationRole, number> = {
      OWNER: 4,
      ADMIN: 3,
      EDITOR: 2,
      VIEWER: 1,
    };

    return roleHierarchy[userRole] >= roleHierarchy[requiredRole];
  }

  /**
   * Check if user can invite members
   * Only OWNER and ADMIN can invite
   */
  async canInviteMembers(
    userId: string,
    organizationId: string,
  ): Promise<boolean> {
    return this.hasRoleOrHigher(userId, organizationId, 'ADMIN');
  }

  /**
   * Check if user can remove members
   * Only OWNER and ADMIN can remove members
   */
  async canRemoveMembers(
    userId: string,
    organizationId: string,
  ): Promise<boolean> {
    return this.hasRoleOrHigher(userId, organizationId, 'ADMIN');
  }

  /**
   * Check if user can change member roles
   * Only OWNER can change roles
   */
  async canChangeRoles(
    userId: string,
    organizationId: string,
  ): Promise<boolean> {
    const userRole = await this.getUserRole(userId, organizationId);
    return userRole === 'OWNER';
  }

  /**
   * Check if user can update organization details
   * Only OWNER can update organization details
   */
  async canUpdateOrganization(
    userId: string,
    organizationId: string,
  ): Promise<boolean> {
    const userRole = await this.getUserRole(userId, organizationId);
    return userRole === 'OWNER';
  }

  /**
   * Check if user can manage data sources
   * OWNER, ADMIN, and EDITOR can manage data sources
   */
  async canManageDataSources(
    userId: string,
    organizationId: string,
  ): Promise<boolean> {
    return this.hasRoleOrHigher(userId, organizationId, 'EDITOR');
  }

  /**
   * Check if user can manage data pipelines
   * OWNER, ADMIN, and EDITOR can manage data pipelines
   */
  async canManageDataPipelines(
    userId: string,
    organizationId: string,
  ): Promise<boolean> {
    return this.hasRoleOrHigher(userId, organizationId, 'EDITOR');
  }

  /**
   * Check if user can view organization data
   * All roles (OWNER, ADMIN, EDITOR, VIEWER) can view
   */
  async canViewOrganization(
    userId: string,
    organizationId: string,
  ): Promise<boolean> {
    return this.hasRoleOrHigher(userId, organizationId, 'VIEWER');
  }
}
