import { IsEnum, IsInt, IsNotEmpty, IsOptional, IsString, Min } from 'class-validator';

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

  @IsOptional()
  @IsInt()
  @Min(0)
  seatCount?: number; // Optional: total seats desired (includes base seats)
}
