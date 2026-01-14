/**
 * Organization Role Guard
 * Enforces role-based access control for organization-scoped endpoints
 *
 * Usage:
 * @UseGuards(SupabaseAuthGuard, OrganizationRoleGuard)
 * @RequireRole('OWNER', 'ADMIN')
 * async someEndpoint(@Param('organizationId') organizationId: string, @Request() req) { ... }
 */

import {
  type CanActivate,
  type ExecutionContext,
  ForbiddenException,
  Injectable,
  SetMetadata,
} from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import type { Request } from 'express';
import { OrganizationMemberRepository } from '../../modules/organizations/repositories/organization-member.repository';
import { OrganizationOwnerRepository } from '../../modules/organizations/repositories/organization-owner.repository';

/**
 * Organization member role type
 * AUTHORITATIVE ROLES - must match database enum
 */
export type OrganizationRole = 'OWNER' | 'ADMIN' | 'EDITOR' | 'VIEWER';

/**
 * Metadata key for required roles
 */
export const REQUIRED_ROLES_KEY = 'required_roles';

/**
 * Decorator to specify required roles for an endpoint
 * @param roles - Array of roles that can access this endpoint
 * @example @RequireRole('OWNER', 'ADMIN')
 */
export const RequireRole = (...roles: OrganizationRole[]) => SetMetadata(REQUIRED_ROLES_KEY, roles);

/**
 * Get user's role in an organization
 * Returns the highest privilege role (OWNER > ADMIN > EDITOR > VIEWER)
 */
export async function getUserRoleInOrganization(
  userId: string,
  organizationId: string,
  memberRepository: OrganizationMemberRepository,
  ownerRepository: OrganizationOwnerRepository,
): Promise<OrganizationRole | null> {
  // Check if user is owner (from organization_owners table)
  const isOwner = await ownerRepository.isOwner(userId, organizationId);
  if (isOwner) {
    return 'OWNER';
  }

  // Check membership and get role
  const member = await memberRepository.findByOrganizationAndUserId(organizationId, userId);

  if (!member) {
    return null;
  }

  // Only return role if member is active or accepted
  if (member.status === 'active' || member.status === 'accepted') {
    return member.role as OrganizationRole;
  }

  return null;
}

@Injectable()
export class OrganizationRoleGuard implements CanActivate {
  constructor(
    private readonly reflector: Reflector,
    private readonly memberRepository: OrganizationMemberRepository,
    private readonly ownerRepository: OrganizationOwnerRepository,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    // Get required roles from metadata
    const requiredRoles = this.reflector.getAllAndOverride<OrganizationRole[]>(REQUIRED_ROLES_KEY, [
      context.getHandler(),
      context.getClass(),
    ]);

    // If no roles required, allow access
    if (!requiredRoles || requiredRoles.length === 0) {
      return true;
    }

    const request = context.switchToHttp().getRequest<Request>();
    const userId = request.user?.id;

    if (!userId) {
      throw new ForbiddenException('User not authenticated');
    }

    // Extract organizationId from request
    // Can be from params (as 'organizationId' or 'id'), body, or query
    const organizationId =
      request.params?.organizationId ||
      request.params?.id ||
      request.body?.organizationId ||
      request.body?.id ||
      request.query?.organizationId ||
      request.query?.id;

    if (!organizationId) {
      throw new ForbiddenException('Organization ID is required');
    }

    // Get user's role in the organization
    const userRole = await getUserRoleInOrganization(
      userId,
      organizationId,
      this.memberRepository,
      this.ownerRepository,
    );

    if (!userRole) {
      throw new ForbiddenException('You are not a member of this organization');
    }

    // Check if user's role is in the required roles list
    if (!requiredRoles.includes(userRole)) {
      throw new ForbiddenException(
        `This action requires one of the following roles: ${requiredRoles.join(', ')}. Your role: ${userRole}`,
      );
    }

    // Attach role to request for use in controllers
    if (request.user) {
      request.user.role = userRole;
      request.user.organizationId = organizationId;
    } else {
      // This shouldn't happen if SupabaseAuthGuard ran first, but handle it
      throw new ForbiddenException('User not authenticated');
    }

    return true;
  }
}
