/**
 * Organization Repository
 * Database operations for organizations
 */

import { Inject, Injectable } from '@nestjs/common';
import { eq } from 'drizzle-orm';
import type { DrizzleDatabase } from '../../../database/drizzle/database';
import {
  type NewOrganization,
  type Organization,
  organizations,
} from '../../../database/schemas/organizations';

@Injectable()
export class OrganizationRepository {
  constructor(@Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase) {}

  /**
   * Create a new organization
   */
  async create(data: NewOrganization): Promise<Organization> {
    const [organization] = await this.db.insert(organizations).values(data).returning();
    return organization;
  }

  /**
   * Find organization by ID
   */
  async findById(id: string): Promise<Organization | null> {
    const [organization] = await this.db
      .select()
      .from(organizations)
      .where(eq(organizations.id, id))
      .limit(1);
    return organization || null;
  }

  /**
   * Find organization by slug
   */
  async findBySlug(slug: string): Promise<Organization | null> {
    const [organization] = await this.db
      .select()
      .from(organizations)
      .where(eq(organizations.slug, slug))
      .limit(1);
    return organization || null;
  }

  /**
   * Find all organizations
   */
  async findAll(): Promise<Organization[]> {
    return this.db.select().from(organizations).where(eq(organizations.isActive, true));
  }

  /**
   * Find organizations by owner user ID
   */
  async findByOwnerUserId(ownerUserId: string): Promise<Organization[]> {
    return this.db
      .select()
      .from(organizations)
      .where(eq(organizations.ownerUserId, ownerUserId));
  }

  /**
   * Check if user is owner of organization
   */
  async isOwner(userId: string, organizationId: string): Promise<boolean> {
    const [org] = await this.db
      .select()
      .from(organizations)
      .where(eq(organizations.id, organizationId))
      .limit(1);
    return org?.ownerUserId === userId;
  }

  /**
   * Update organization
   */
  async update(id: string, data: Partial<NewOrganization>): Promise<Organization> {
    const [organization] = await this.db
      .update(organizations)
      .set({ ...data, updatedAt: new Date() })
      .where(eq(organizations.id, id))
      .returning();
    return organization;
  }

  /**
   * Delete organization (soft delete by setting isActive to false)
   */
  async delete(id: string): Promise<void> {
    await this.db
      .update(organizations)
      .set({ isActive: false, updatedAt: new Date() })
      .where(eq(organizations.id, id));
  }

  /**
   * Hard delete organization
   */
  async hardDelete(id: string): Promise<void> {
    await this.db.delete(organizations).where(eq(organizations.id, id));
  }
}
