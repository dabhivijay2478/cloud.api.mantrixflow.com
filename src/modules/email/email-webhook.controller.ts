/**
 * Email Webhook Controller
 * Receives UnoSend webhooks for bounces, unsubscribes, complaints
 */

import { Body, Controller, Headers, HttpCode, HttpStatus, Post } from '@nestjs/common';
import { ApiExcludeController } from '@nestjs/swagger';
import { EmailRepository } from './repositories/email-repository';

interface UnoSendWebhookPayload {
  type?: string;
  email?: string;
  reason?: string;
  event?: string;
}

@ApiExcludeController()
@Controller('webhooks/email')
export class EmailWebhookController {
  constructor(private readonly emailRepository: EmailRepository) {}

  @Post('unosend')
  @HttpCode(HttpStatus.OK)
  async handleUnoSendWebhook(
    @Body() body: UnoSendWebhookPayload,
    @Headers('x-unosend-signature') _signature?: string,
  ) {
    const email = body.email ?? (body as Record<string, unknown>).recipient_email as string;
    const reason =
      body.reason ??
      body.type ??
      (body.event === 'bounce' ? 'bounce' : body.event === 'unsubscribe' ? 'unsubscribe' : 'complaint');
    if (email) {
      await this.emailRepository.addSuppression(email, String(reason));
    }
    return { received: true };
  }
}
