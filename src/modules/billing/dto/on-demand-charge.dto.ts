import { IsInt, IsNotEmpty, IsOptional, IsString, Min } from 'class-validator';

export class OnDemandChargeDto {
  @IsInt()
  @IsNotEmpty()
  @Min(1)
  productPrice: number; // Amount in cents (e.g., 300 = $3.00)

  @IsOptional()
  @IsString()
  productDescription?: string; // Optional description for the charge

  @IsOptional()
  @IsString()
  productCurrency?: string; // Optional currency override (defaults to subscription currency)
}