import { Inject, Injectable } from '@nestjs/common';
import { eq } from 'drizzle-orm';
import type { DrizzleDatabase } from '../../../database/drizzle/database';
import { subscriptions } from '../../../database/schemas/billing/dodo-subscriptions.schema';
import type {
  NewSubscription,
  Subscription,
} from '../../../database/schemas/billing/dodo-subscriptions.schema';

@Injectable()
export class SubscriptionRepository {
  constructor(@Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase) {}

  async create(data: NewSubscription): Promise<Subscription> {
    const [subscription] = await this.db.insert(subscriptions).values(data).returning();
    return subscription;
  }

  async findByUserId(userId: string): Promise<Subscription | null> {
    // Use select() - Drizzle should automatically map dodo_customer_id to dodoCustomerId
    const [subscription] = await this.db
      .select()
      .from(subscriptions)
      .where(eq(subscriptions.userId, userId))
      .limit(1);

    if (!subscription) {
      return null;
    }

    // Debug: Log raw subscription data to see what Drizzle returns
    console.log('=== SUBSCRIPTION DEBUG ===');
    console.log('Raw subscription object:', JSON.stringify(subscription, null, 2));
    console.log('Subscription keys:', Object.keys(subscription));
    console.log('dodoCustomerId value:', (subscription as any).dodoCustomerId);
    console.log('dodoCustomerId type:', typeof (subscription as any).dodoCustomerId);
    console.log('Has dodoCustomerId property:', 'dodoCustomerId' in subscription);
    // Try accessing via index signature
    const subAny = subscription as any;
    console.log('dodo_customer_id (snake_case):', subAny.dodo_customer_id);
    console.log('All property names:', Object.getOwnPropertyNames(subscription));
    console.log('Full subscription:', subscription);
    console.log('========================');

    // If dodoCustomerId is missing but dodo_customer_id exists, map it manually
    if (!subAny.dodoCustomerId && subAny.dodo_customer_id) {
      console.log('⚠️  Mapping dodo_customer_id to dodoCustomerId manually');
      return {
        ...subscription,
        dodoCustomerId: subAny.dodo_customer_id,
      } as Subscription;
    }

    return subscription;
  }

  async findByDodoSubscriptionId(dodoSubscriptionId: string): Promise<Subscription | null> {
    const [subscription] = await this.db
      .select({
        id: subscriptions.id,
        userId: subscriptions.userId,
        dodoSubscriptionId: subscriptions.dodoSubscriptionId,
        dodoCustomerId: subscriptions.dodoCustomerId, // Explicitly select
        planId: subscriptions.planId,
        status: subscriptions.status,
        currentPeriodStart: subscriptions.currentPeriodStart,
        currentPeriodEnd: subscriptions.currentPeriodEnd,
        trialStart: subscriptions.trialStart,
        trialEnd: subscriptions.trialEnd,
        canceledAt: subscriptions.canceledAt,
        cancelAtPeriodEnd: subscriptions.cancelAtPeriodEnd,
        metadata: subscriptions.metadata,
        createdAt: subscriptions.createdAt,
        updatedAt: subscriptions.updatedAt,
      })
      .from(subscriptions)
      .where(eq(subscriptions.dodoSubscriptionId, dodoSubscriptionId))
      .limit(1);
    return subscription || null;
  }

  async update(id: string, data: Partial<NewSubscription>): Promise<Subscription> {
    const [subscription] = await this.db
      .update(subscriptions)
      .set({ ...data, updatedAt: new Date() })
      .where(eq(subscriptions.id, id))
      .returning();
    return subscription;
  }

  async updateByUserId(
    userId: string,
    data: Partial<NewSubscription>,
  ): Promise<Subscription | null> {
    const [subscription] = await this.db
      .update(subscriptions)
      .set({ ...data, updatedAt: new Date() })
      .where(eq(subscriptions.userId, userId))
      .returning();
    return subscription || null;
  }
}
