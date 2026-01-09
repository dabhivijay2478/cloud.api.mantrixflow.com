/**
 * Organization Owner Repository
 * Data access layer for organization_owners table
 */

import { Inject, Injectable } from '@nestjs/common';
import { and, eq } from 'drizzle-orm';
import type { DrizzleDatabase } from '../../../database/drizzle/database';
import {
  type NewOrganizationOwner,
  type OrganizationOwner,
  organizationOwners,
} from '../../../database/schemas/organizations';

@Injectable()
export class OrganizationOwnerRepository {
  constructor(@Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase) {}

  /**
   * Create a new organization owner record
   */
  async create(data: NewOrganizationOwner): Promise<OrganizationOwner> {
    const [owner] = await this.db.insert(organizationOwners).values(data).returning();
    return owner;
  }

  /**
   * Find owner by ID
   */
  async findById(id: string): Promise<OrganizationOwner | null> {
    const [owner] = await this.db
      .select()
      .from(organizationOwners)
      .where(eq(organizationOwners.id, id))
      .limit(1);
    return owner || null;
  }

  /**
   * Find owner by organization ID and user ID
   */
  async findByOrganizationAndUserId(
    organizationId: string,
    userId: string,
  ): Promise<OrganizationOwner | null> {
    const [owner] = await this.db
      .select()
      .from(organizationOwners)
      .where(
        and(
          eq(organizationOwners.organizationId, organizationId),
          eq(organizationOwners.userId, userId),
        ),
      )
      .limit(1);
    return owner || null;
  }

  /**
   * Find all organizations owned by a user
   */
  async findByUserId(userId: string): Promise<OrganizationOwner[]> {
    return this.db
      .select()
      .from(organizationOwners)
      .where(eq(organizationOwners.userId, userId));
  }

  /**
   * Find all owners of an organization
   */
  async findByOrganizationId(organizationId: string): Promise<OrganizationOwner[]> {
    return this.db
      .select()
      .from(organizationOwners)
      .where(eq(organizationOwners.organizationId, organizationId));
  }

  /**
   * Check if a user is an owner of an organization
   */
  async isOwner(organizationId: string, userId: string): Promise<boolean> {
    const owner = await this.findByOrganizationAndUserId(organizationId, userId);
    return owner !== null;
  }

  /**
   * Delete owner record
   */
  async delete(id: string): Promise<void> {
    await this.db.delete(organizationOwners).where(eq(organizationOwners.id, id));
  }

  /**
   * Delete owner by organization and user
   */
  async deleteByOrganizationAndUserId(organizationId: string, userId: string): Promise<void> {
    await this.db
      .delete(organizationOwners)
      .where(
        and(
          eq(organizationOwners.organizationId, organizationId),
          eq(organizationOwners.userId, userId),
        ),
      );
  }
}
