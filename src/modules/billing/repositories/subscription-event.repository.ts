/**
 * Subscription Event Repository
 * Data access layer for subscription_events table (webhook audit log)
 */

import { Inject, Injectable } from '@nestjs/common';
import { eq } from 'drizzle-orm';
import type { DrizzleDatabase } from '../../../database/drizzle/database';
import { subscriptionEvents } from '../../../database/schemas/billing/subscription-events.schema';

@Injectable()
export class SubscriptionEventRepository {
  constructor(@Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase) {}

  /**
   * Create a new subscription event record
   */
  async create(data: {
    userId?: string;
    organizationId?: string;
    provider: string;
    eventType: string;
    payload: Record<string, unknown>;
  }) {
    const [event] = await this.db
      .insert(subscriptionEvents)
      .values({
        userId: data.userId,
        organizationId: data.organizationId,
        provider: data.provider,
        eventType: data.eventType,
        payload: data.payload,
      })
      .returning();
    return event;
  }

  /**
   * Find events by organization ID
   */
  async findByOrganizationId(organizationId: string) {
    return this.db
      .select()
      .from(subscriptionEvents)
      .where(eq(subscriptionEvents.organizationId, organizationId))
      .orderBy(subscriptionEvents.createdAt);
  }

  /**
   * Find events by event type
   */
  async findByEventType(eventType: string) {
    return this.db
      .select()
      .from(subscriptionEvents)
      .where(eq(subscriptionEvents.eventType, eventType))
      .orderBy(subscriptionEvents.createdAt);
  }
}
