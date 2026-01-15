import { IsEnum, IsNotEmpty, IsString } from 'class-validator';

export enum SubscriptionPlan {
  FREE = 'free',
  PRO = 'pro',
  SCALE = 'scale',
  ENTERPRISE = 'enterprise',
}

export class CreateCheckoutDto {
  @IsEnum(SubscriptionPlan)
  @IsNotEmpty()
  planId: SubscriptionPlan;

  @IsString()
  @IsNotEmpty()
  returnUrl: string;
}
