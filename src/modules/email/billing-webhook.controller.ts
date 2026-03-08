/**
 * Billing Webhook Controller
 * Receives payment webhooks from billing provider (Stripe, Paddle, etc.)
 * Sends payment_failed, subscription_upgraded, subscription_cancelled emails
 */

import { Body, Controller, Headers, HttpCode, HttpStatus, Post } from '@nestjs/common';
import { ApiExcludeController } from '@nestjs/swagger';
import { EmailService } from './email.service';

@ApiExcludeController()
@Controller('webhooks/billing')
export class BillingWebhookController {
  constructor(private readonly emailService: EmailService) {}

  /**
   * POST /webhooks/billing
   * Stub for billing provider webhooks.
   * Integrate with Stripe/Paddle when billing is implemented.
   */
  @Post()
  @HttpCode(HttpStatus.OK)
  async handleBillingWebhook(
    @Body() _body: Record<string, unknown>,
    @Headers('x-webhook-signature') _signature?: string,
  ) {
    // TODO: Verify webhook signature, parse provider-specific payload
    // TODO: On payment_failed: send payment_failed email
    // TODO: On subscription_created/updated: send subscription_upgraded
    // TODO: On subscription_cancelled: send subscription_cancelled
    return { received: true };
  }
}
