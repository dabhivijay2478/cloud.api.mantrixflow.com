/**
 * Organization Service
 * Business logic for organization management
 */

import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import type { Organization } from '../../database/schemas/organizations';
import { CreateOrganizationDto } from './dto/create-organization.dto';
import { UpdateOrganizationDto } from './dto/update-organization.dto';
import { OrganizationRepository } from './repositories/organization.repository';

@Injectable()
export class OrganizationService {
  constructor(private readonly organizationRepository: OrganizationRepository) {}

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
  async createOrganization(_userId: string, dto: CreateOrganizationDto): Promise<Organization> {
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

    return organization;
  }

  /**
   * List all organizations
   */
  async listOrganizations(): Promise<Organization[]> {
    return this.organizationRepository.findAll();
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
   * Get current organization (returns first active organization for now)
   * In the future, this should get from user's currentOrgId
   */
  async getCurrentOrganization(): Promise<Organization | null> {
    const orgs = await this.organizationRepository.findAll();
    return orgs.length > 0 ? orgs[0] : null;
  }

  /**
   * Set current organization (placeholder - should update user's currentOrgId)
   * For now, just returns the organization
   */
  async setCurrentOrganization(_userId: string, id: string): Promise<Organization> {
    return this.getOrganization(id);
  }
}
