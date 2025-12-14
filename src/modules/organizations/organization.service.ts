/**
 * Organization Service
 * Business logic for organization management
 */

import { Injectable, NotFoundException, BadRequestException } from '@nestjs/common';
import { CreateOrganizationDto } from './dto/create-organization.dto';
import { UpdateOrganizationDto } from './dto/update-organization.dto';

export interface Organization {
  id: string;
  name: string;
  slug: string;
  description?: string;
  createdAt: Date;
  updatedAt: Date;
}

@Injectable()
export class OrganizationService {
  // In-memory storage for now - replace with database repository
  private organizations: Map<string, Organization> = new Map();
  private currentOrgId: string | null = null;

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
  async createOrganization(
    userId: string,
    dto: CreateOrganizationDto,
  ): Promise<Organization> {
    const slug = dto.slug || this.generateSlug(dto.name);

    // Check if slug already exists
    const existingOrg = Array.from(this.organizations.values()).find(
      (org) => org.slug === slug,
    );
    if (existingOrg) {
      throw new BadRequestException(`Organization with slug "${slug}" already exists`);
    }

    const organization: Organization = {
      id: `org_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
      name: dto.name,
      slug,
      description: dto.description,
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    this.organizations.set(organization.id, organization);

    // Set as current if it's the first organization
    if (this.organizations.size === 1) {
      this.currentOrgId = organization.id;
    }

    return organization;
  }

  /**
   * List all organizations
   */
  async listOrganizations(): Promise<Organization[]> {
    return Array.from(this.organizations.values());
  }

  /**
   * Get organization by ID
   */
  async getOrganization(id: string): Promise<Organization> {
    const organization = this.organizations.get(id);
    if (!organization) {
      throw new NotFoundException(`Organization with ID "${id}" not found`);
    }
    return organization;
  }

  /**
   * Get organization by slug
   */
  async getOrganizationBySlug(slug: string): Promise<Organization> {
    const organization = Array.from(this.organizations.values()).find(
      (org) => org.slug === slug,
    );
    if (!organization) {
      throw new NotFoundException(`Organization with slug "${slug}" not found`);
    }
    return organization;
  }

  /**
   * Get current organization
   */
  async getCurrentOrganization(): Promise<Organization | null> {
    if (!this.currentOrgId) {
      return null;
    }
    return this.getOrganization(this.currentOrgId);
  }

  /**
   * Set current organization
   */
  async setCurrentOrganization(userId: string, id: string): Promise<Organization> {
    const organization = await this.getOrganization(id);
    this.currentOrgId = id;
    return organization;
  }

  /**
   * Update organization
   */
  async updateOrganization(
    id: string,
    dto: UpdateOrganizationDto,
  ): Promise<Organization> {
    const organization = await this.getOrganization(id);

    // Check slug uniqueness if slug is being updated
    if (dto.slug && dto.slug !== organization.slug) {
      const existingOrg = Array.from(this.organizations.values()).find(
        (org) => org.slug === dto.slug && org.id !== id,
      );
      if (existingOrg) {
        throw new BadRequestException(`Organization with slug "${dto.slug}" already exists`);
      }
    }

    const updated: Organization = {
      ...organization,
      ...dto,
      updatedAt: new Date(),
    };

    this.organizations.set(id, updated);
    return updated;
  }

  /**
   * Delete organization
   */
  async deleteOrganization(id: string): Promise<void> {
    const organization = await this.getOrganization(id);
    this.organizations.delete(id);

    // Clear current org if it was deleted
    if (this.currentOrgId === id) {
      const remaining = Array.from(this.organizations.values());
      this.currentOrgId = remaining.length > 0 ? remaining[0].id : null;
    }
  }
}
