/**
 * Billing Repository
 * Data access layer for billing_subscriptions table
 */

import { Inject, Injectable } from '@nestjs/common';
import { eq } from 'drizzle-orm';
import type { DrizzleDatabase } from '../../../database/drizzle/database';
import {
  type BillingSubscription,
  type NewBillingSubscription,
  billingSubscriptions,
} from '../../../database/schemas/billing';

@Injectable()
export class BillingRepository {
  constructor(@Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase) {}

  /**
   * Create a new billing subscription record
   */
  async create(data: NewBillingSubscription): Promise<BillingSubscription> {
    const [subscription] = await this.db
      .insert(billingSubscriptions)
      .values(data)
      .returning();
    return subscription;
  }

  /**
   * Find billing subscription by organization ID
   */
  async findByOrganizationId(organizationId: string): Promise<BillingSubscription | null> {
    const [subscription] = await this.db
      .select()
      .from(billingSubscriptions)
      .where(eq(billingSubscriptions.organizationId, organizationId))
      .limit(1);
    return subscription || null;
  }

  /**
   * Find billing subscription by Stripe customer ID
   */
  async findByStripeCustomerId(
    stripeCustomerId: string,
  ): Promise<BillingSubscription | null> {
    const [subscription] = await this.db
      .select()
      .from(billingSubscriptions)
      .where(eq(billingSubscriptions.stripeCustomerId, stripeCustomerId))
      .limit(1);
    return subscription || null;
  }

  /**
   * Find billing subscription by Stripe subscription ID
   */
  async findByStripeSubscriptionId(
    stripeSubscriptionId: string,
  ): Promise<BillingSubscription | null> {
    const [subscription] = await this.db
      .select()
      .from(billingSubscriptions)
      .where(eq(billingSubscriptions.stripeSubscriptionId, stripeSubscriptionId))
      .limit(1);
    return subscription || null;
  }

  /**
   * Update billing subscription
   */
  async update(
    id: string,
    data: Partial<NewBillingSubscription>,
  ): Promise<BillingSubscription> {
    const [subscription] = await this.db
      .update(billingSubscriptions)
      .set({ ...data, updatedAt: new Date() })
      .where(eq(billingSubscriptions.id, id))
      .returning();
    return subscription;
  }

  /**
   * Update billing subscription by organization ID
   */
  async updateByOrganizationId(
    organizationId: string,
    data: Partial<NewBillingSubscription>,
  ): Promise<BillingSubscription | null> {
    const [subscription] = await this.db
      .update(billingSubscriptions)
      .set({ ...data, updatedAt: new Date() })
      .where(eq(billingSubscriptions.organizationId, organizationId))
      .returning();
    return subscription || null;
  }

  /**
   * Delete billing subscription
   */
  async delete(id: string): Promise<void> {
    await this.db.delete(billingSubscriptions).where(eq(billingSubscriptions.id, id));
  }
}
