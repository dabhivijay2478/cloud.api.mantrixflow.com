/**
 * Billing Response DTOs
 * Response structures for billing endpoints
 */

import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

/**
 * Billing Overview Response DTO
 */
export class BillingOverviewDto {
  @ApiProperty({
    description: 'Current subscription plan',
    example: 'pro',
  })
  currentPlan: string;

  @ApiProperty({
    description: 'Billing status',
    enum: ['active', 'trial', 'expired', 'incomplete'],
    example: 'active',
  })
  billingStatus: 'active' | 'trial' | 'expired' | 'incomplete';

  @ApiProperty({
    description: 'Next billing date',
    example: '2024-02-15T00:00:00.000Z',
    nullable: true,
  })
  nextBillingDate: Date | null;

  @ApiProperty({
    description: 'Billing amount',
    example: 99.99,
  })
  amount: number;

  @ApiProperty({
    description: 'Currency code',
    example: 'USD',
  })
  currency: string;
}

/**
 * Billing Usage Response DTO
 */
export class BillingUsageDto {
  @ApiProperty({
    description: 'Number of pipelines currently used',
    example: 5,
  })
  pipelinesUsed: number;

  @ApiProperty({
    description: 'Maximum number of pipelines allowed',
    example: 10,
  })
  pipelinesLimit: number;

  @ApiProperty({
    description: 'Number of data sources currently used',
    example: 3,
  })
  dataSourcesUsed: number;

  @ApiProperty({
    description: 'Maximum number of data sources allowed',
    example: 5,
  })
  dataSourcesLimit: number;

  @ApiProperty({
    description: 'Number of migrations run',
    example: 150,
  })
  migrationsRun: number;
}

/**
 * Billing Invoice Response DTO
 */
export class BillingInvoiceDto {
  @ApiProperty({
    description: 'Invoice ID',
    example: 'inv_1234567890',
  })
  invoiceId: string;

  @ApiProperty({
    description: 'Invoice date',
    example: '2024-01-15T00:00:00.000Z',
  })
  date: Date;

  @ApiProperty({
    description: 'Invoice amount',
    example: 99.99,
  })
  amount: number;

  @ApiProperty({
    description: 'Invoice status',
    enum: ['paid', 'pending', 'failed'],
    example: 'paid',
  })
  status: 'paid' | 'pending' | 'failed';

  @ApiPropertyOptional({
    description: 'Currency code',
    example: 'INR',
  })
  currency?: string;

  @ApiPropertyOptional({
    description: 'Download URL for the invoice from Dodo Payments',
    example: 'https://dodopayments.com/invoices/inv_1234567890.pdf',
  })
  downloadUrl?: string;
}
