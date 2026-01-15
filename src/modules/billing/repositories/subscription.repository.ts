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
    const [subscription] = await this.db
      .select()
      .from(subscriptions)
      .where(eq(subscriptions.userId, userId))
      .limit(1);

    return subscription || null;
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
