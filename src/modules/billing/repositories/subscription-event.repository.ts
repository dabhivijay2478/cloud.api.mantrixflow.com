import { Inject, Injectable } from '@nestjs/common';
import { eq } from 'drizzle-orm';
import type { DrizzleDatabase } from '../../../database/drizzle/database';
import { subscriptionEvents } from '../../../database/schemas/billing/dodo-subscription-events.schema';
import type {
  NewSubscriptionEvent,
  SubscriptionEvent,
} from '../../../database/schemas/billing/dodo-subscription-events.schema';

@Injectable()
export class SubscriptionEventRepository {
  constructor(@Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase) {}

  async create(data: NewSubscriptionEvent): Promise<SubscriptionEvent> {
    const [event] = await this.db.insert(subscriptionEvents).values(data).returning();
    return event;
  }

  async findByDodoEventId(dodoEventId: string): Promise<SubscriptionEvent | null> {
    const [event] = await this.db
      .select()
      .from(subscriptionEvents)
      .where(eq(subscriptionEvents.dodoEventId, dodoEventId))
      .limit(1);
    return event || null;
  }

  async findBySubscriptionId(subscriptionId: string): Promise<SubscriptionEvent[]> {
    return this.db
      .select()
      .from(subscriptionEvents)
      .where(eq(subscriptionEvents.subscriptionId, subscriptionId))
      .orderBy(subscriptionEvents.createdAt);
  }
}
