import { IsEnum, IsNotEmpty, IsOptional, IsString } from 'class-validator';
import { SubscriptionPlan } from './create-checkout.dto';

export class ChangePlanDto {
  @IsEnum(SubscriptionPlan)
  @IsNotEmpty()
  planId: SubscriptionPlan;

  @IsOptional()
  @IsString()
  returnUrl?: string;
}
