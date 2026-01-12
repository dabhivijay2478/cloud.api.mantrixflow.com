/**
 * Subscription Repository
 * Data access layer for subscriptions table (provider-agnostic)
 */

import { Inject, Injectable } from '@nestjs/common';
import { eq, and } from 'drizzle-orm';
import type { DrizzleDatabase } from '../../../database/drizzle/database';
import {
  type Subscription,
  type NewSubscription,
  subscriptions,
} from '../../../database/schemas/billing';

@Injectable()
export class SubscriptionRepository {
  constructor(@Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase) {}

  /**
   * Create a new subscription record
   */
  async create(data: NewSubscription): Promise<Subscription> {
    const [subscription] = await this.db
      .insert(subscriptions)
      .values(data)
      .returning();
    return subscription;
  }

  /**
   * Find subscription by organization ID
   */
  async findByOrganizationId(organizationId: string): Promise<Subscription | null> {
    const [subscription] = await this.db
      .select()
      .from(subscriptions)
      .where(eq(subscriptions.organizationId, organizationId))
      .orderBy(subscriptions.createdAt)
      .limit(1);
    return subscription || null;
  }

  /**
   * Find subscription by provider subscription ID
   */
  async findByProviderSubscriptionId(
    providerSubscriptionId: string,
  ): Promise<Subscription | null> {
    const [subscription] = await this.db
      .select()
      .from(subscriptions)
      .where(eq(subscriptions.providerSubscriptionId, providerSubscriptionId))
      .limit(1);
    return subscription || null;
  }

  /**
   * Update subscription
   */
  async update(id: string, data: Partial<NewSubscription>): Promise<Subscription> {
    const [subscription] = await this.db
      .update(subscriptions)
      .set({ ...data, updatedAt: new Date() })
      .where(eq(subscriptions.id, id))
      .returning();
    return subscription;
  }

  /**
   * Update subscription by provider subscription ID
   */
  async updateByProviderSubscriptionId(
    providerSubscriptionId: string,
    data: Partial<NewSubscription>,
  ): Promise<Subscription | null> {
    const [subscription] = await this.db
      .update(subscriptions)
      .set({ ...data, updatedAt: new Date() })
      .where(eq(subscriptions.providerSubscriptionId, providerSubscriptionId))
      .returning();
    return subscription || null;
  }

  /**
   * Delete subscription
   */
  async delete(id: string): Promise<void> {
    await this.db.delete(subscriptions).where(eq(subscriptions.id, id));
  }
}
