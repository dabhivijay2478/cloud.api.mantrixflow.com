/**
 * Organization Member Repository
 * Data access layer for organization_members table
 */

import { Inject, Injectable } from '@nestjs/common';
import { and, desc, eq, or } from 'drizzle-orm';
import type { DrizzleDatabase } from '../../../database/drizzle/database';
import {
  type NewOrganizationMember,
  type OrganizationMember,
  organizationMembers,
} from '../../../database/schemas/organizations';

@Injectable()
export class OrganizationMemberRepository {
  constructor(@Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase) {}
  /**
   * Create a new organization member (invite)
   */
  async create(data: NewOrganizationMember): Promise<OrganizationMember> {
    const [member] = await this.db.insert(organizationMembers).values(data).returning();
    return member;
  }

  /**
   * Find member by ID
   */
  async findById(id: string): Promise<OrganizationMember | null> {
    const [member] = await this.db
      .select()
      .from(organizationMembers)
      .where(eq(organizationMembers.id, id))
      .limit(1);
    return member || null;
  }

  /**
   * Find member by organization ID and email
   * Used to check for duplicate invites
   */
  async findByOrganizationAndEmail(
    organizationId: string,
    email: string,
  ): Promise<OrganizationMember | null> {
    const [member] = await this.db
      .select()
      .from(organizationMembers)
      .where(
        and(
          eq(organizationMembers.organizationId, organizationId),
          eq(organizationMembers.email, email.toLowerCase()),
        ),
      )
      .limit(1);
    return member || null;
  }

  /**
   * Find active/invited member by organization ID and email
   * This checks for existing invites that haven't been declined
   */
  async findActiveInviteByEmail(
    organizationId: string,
    email: string,
  ): Promise<OrganizationMember | null> {
    const [member] = await this.db
      .select()
      .from(organizationMembers)
      .where(
        and(
          eq(organizationMembers.organizationId, organizationId),
          eq(organizationMembers.email, email.toLowerCase()),
          or(
            eq(organizationMembers.status, 'invited'),
            eq(organizationMembers.status, 'accepted'),
            eq(organizationMembers.status, 'active'),
          ),
        ),
      )
      .limit(1);
    return member || null;
  }

  /**
   * Find member by organization ID and user ID
   */
  async findByOrganizationAndUserId(
    organizationId: string,
    userId: string,
  ): Promise<OrganizationMember | null> {
    const [member] = await this.db
      .select()
      .from(organizationMembers)
      .where(
        and(
          eq(organizationMembers.organizationId, organizationId),
          eq(organizationMembers.userId, userId),
        ),
      )
      .limit(1);
    return member || null;
  }

  /**
   * Find all members for an organization
   */
  async findByOrganizationId(organizationId: string): Promise<OrganizationMember[]> {
    return this.db
      .select()
      .from(organizationMembers)
      .where(eq(organizationMembers.organizationId, organizationId))
      .orderBy(desc(organizationMembers.createdAt));
  }

  /**
   * Find invite by email (across all organizations)
   * Used when user signs up to find their invite
   */
  async findInviteByEmail(email: string): Promise<OrganizationMember | null> {
    const [member] = await this.db
      .select()
      .from(organizationMembers)
      .where(
        and(
          eq(organizationMembers.email, email.toLowerCase()),
          eq(organizationMembers.status, 'invited'),
        ),
      )
      .limit(1);
    return member || null;
  }

  /**
   * Find all invites by email (user might have multiple invites)
   */
  async findAllInvitesByEmail(email: string): Promise<OrganizationMember[]> {
    return this.db
      .select()
      .from(organizationMembers)
      .where(
        and(
          eq(organizationMembers.email, email.toLowerCase()),
          eq(organizationMembers.status, 'invited'),
        ),
      )
      .orderBy(desc(organizationMembers.invitedAt));
  }

  /**
   * Update member
   */
  async update(id: string, data: Partial<NewOrganizationMember>): Promise<OrganizationMember> {
    const [member] = await this.db
      .update(organizationMembers)
      .set({ ...data, updatedAt: new Date() })
      .where(eq(organizationMembers.id, id))
      .returning();
    return member;
  }

  /**
   * Link user to invite (when user signs up)
   */
  async linkUserToInvite(inviteId: string, userId: string): Promise<OrganizationMember> {
    const [member] = await this.db
      .update(organizationMembers)
      .set({
        userId,
        status: 'accepted',
        acceptedAt: new Date(),
        updatedAt: new Date(),
      })
      .where(eq(organizationMembers.id, inviteId))
      .returning();
    return member;
  }

  /**
   * Update member status
   */
  async updateStatus(
    id: string,
    status: OrganizationMember['status'],
  ): Promise<OrganizationMember> {
    const updateData: Partial<OrganizationMember> = {
      status,
      updatedAt: new Date(),
    };

    // Set acceptedAt when status changes to accepted
    if (status === 'accepted' || status === 'active') {
      updateData.acceptedAt = new Date();
    }

    const [member] = await this.db
      .update(organizationMembers)
      .set(updateData)
      .where(eq(organizationMembers.id, id))
      .returning();
    return member;
  }

  /**
   * Delete member
   */
  async delete(id: string): Promise<void> {
    await this.db.delete(organizationMembers).where(eq(organizationMembers.id, id));
  }
}
