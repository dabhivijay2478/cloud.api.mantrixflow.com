import { IsEnum, IsNotEmpty, IsString } from 'class-validator';

export enum SubscriptionPlan {
  BASIC = 'basic',
  PRO = 'pro',
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
