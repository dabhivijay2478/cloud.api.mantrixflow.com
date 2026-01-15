import { IsNotEmpty, IsString } from 'class-validator';

export class CreateOnDemandSubscriptionDto {
  @IsString()
  @IsNotEmpty()
  returnUrl: string;
}