import { SubscriptionPlan, SubscriptionStatus } from '../entities/subscription.entity';

export class SubscriptionResponseDto {
  id: string;
  userId: string;
  dodoSubscriptionId: string | null;
  dodoCustomerId: string | null;
  planId: SubscriptionPlan;
  status: SubscriptionStatus;
  currentPeriodStart: Date | null;
  currentPeriodEnd: Date | null;
  trialStart: Date | null;
  trialEnd: Date | null;
  canceledAt: Date | null;
  cancelAtPeriodEnd: Date | null;
  createdAt: Date;
  updatedAt: Date;
}
