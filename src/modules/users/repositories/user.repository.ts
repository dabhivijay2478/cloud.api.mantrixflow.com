/**
 * User Repository
 * Database operations for users
 */

import { Inject, Injectable } from '@nestjs/common';
import { eq } from 'drizzle-orm';
import type { DrizzleDatabase } from '../../../database/drizzle/database';
import { type NewUser, type User, users } from '../../../database/schemas/users';

@Injectable()
export class UserRepository {
  constructor(@Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase) {}
  /**
   * Create a new user
   */
  async create(data: NewUser): Promise<User> {
    const [user] = await this.db.insert(users).values(data).returning();
    return user;
  }

  /**
   * Find user by ID
   */
  async findById(id: string): Promise<User | null> {
    const [user] = await this.db.select().from(users).where(eq(users.id, id)).limit(1);
    return user || null;
  }

  /**
   * Find user by Supabase user ID
   */
  async findBySupabaseUserId(supabaseUserId: string): Promise<User | null> {
    const [user] = await this.db
      .select()
      .from(users)
      .where(eq(users.supabaseUserId, supabaseUserId))
      .limit(1);
    return user || null;
  }

  /**
   * Find user by email
   */
  async findByEmail(email: string): Promise<User | null> {
    const [user] = await this.db.select().from(users).where(eq(users.email, email)).limit(1);
    return user || null;
  }

  /**
   * Find user by billing customer ID (Dodo customer_id)
   */
  async findByBillingCustomerId(billingCustomerId: string): Promise<User | null> {
    const [user] = await this.db
      .select()
      .from(users)
      .where(eq(users.billingCustomerId, billingCustomerId))
      .limit(1);
    return user || null;
  }

  /**
   * Update user
   */
  async update(id: string, data: Partial<NewUser>): Promise<User> {
    const [user] = await this.db
      .update(users)
      .set({ ...data, updatedAt: new Date() })
      .where(eq(users.id, id))
      .returning();
    return user;
  }

  /**
   * Update last login timestamp
   */
  async updateLastLogin(id: string): Promise<void> {
    await this.db
      .update(users)
      .set({ lastLoginAt: new Date(), updatedAt: new Date() })
      .where(eq(users.id, id));
  }

  /**
   * Update onboarding status
   */
  async updateOnboarding(id: string, completed: boolean, step?: string): Promise<User> {
    const [user] = await this.db
      .update(users)
      .set({
        onboardingCompleted: completed,
        onboardingStep: step,
        updatedAt: new Date(),
      })
      .where(eq(users.id, id))
      .returning();
    return user;
  }

  /**
   * Set current organization
   */
  async setCurrentOrganization(userId: string, orgId: string | null): Promise<User> {
    const [user] = await this.db
      .update(users)
      .set({ currentOrgId: orgId, updatedAt: new Date() })
      .where(eq(users.id, userId))
      .returning();
    return user;
  }
}
