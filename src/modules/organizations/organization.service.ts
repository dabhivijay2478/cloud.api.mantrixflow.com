/**
 * Organization Service
 * Business logic for organization management
 */

import { BadRequestException, forwardRef, Inject, Injectable, NotFoundException } from '@nestjs/common';
import type { Organization } from '../../database/schemas/organizations';
import type { CreateOrganizationDto } from './dto/create-organization.dto';
import type { UpdateOrganizationDto } from './dto/update-organization.dto';
import { OrganizationMemberRepository } from './repositories/organization-member.repository';
import { OrganizationRepository } from './repositories/organization.repository';
import { UserRepository } from '../users/repositories/user.repository';

@Injectable()
export class OrganizationService {
  constructor(
    private readonly organizationRepository: OrganizationRepository,
    private readonly memberRepository: OrganizationMemberRepository,
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
   * Create organization
   */
  async createOrganization(userId: string, dto: CreateOrganizationDto): Promise<Organization> {
    const slug = dto.slug || this.generateSlug(dto.name);

    // Check if slug already exists
    const existingOrg = await this.organizationRepository.findBySlug(slug);
    if (existingOrg) {
      throw new BadRequestException(`Organization with slug "${slug}" already exists`);
    }

    const organization = await this.organizationRepository.create({
      name: dto.name,
      slug,
      description: dto.description,
      isActive: true,
    });

    // Add the user as owner of the organization
    try {
      const user = await this.userRepository.findById(userId);
      if (user) {
        // Create member record with owner role and active status
        await this.memberRepository.create({
          organizationId: organization.id,
          userId: userId,
          email: user.email.toLowerCase(),
          role: 'owner',
          status: 'active',
          acceptedAt: new Date(),
        });

        // Set this organization as the user's current organization
        await this.userRepository.setCurrentOrganization(userId, organization.id);
      }
    } catch (error) {
      // Log error but don't fail organization creation
      console.error('Failed to add user as member or set current organization:', error);
    }

    return organization;
  }

  /**
   * List all organizations for a user (only organizations they are a member of)
   */
  async listOrganizations(userId: string): Promise<Organization[]> {
    // Get all organization memberships for this user
    const members = await this.memberRepository.findByUserId(userId);
    
    if (members.length === 0) {
      return [];
    }

    // Get organization IDs
    const orgIds = members.map((m) => m.organizationId);

    // Fetch organizations
    const organizations = await Promise.all(
      orgIds.map((id) => this.organizationRepository.findById(id)),
    );

    // Filter out null values and return
    return organizations.filter((org): org is Organization => org !== null);
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
