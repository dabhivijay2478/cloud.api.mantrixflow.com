import { Body, Controller, Get, Headers, HttpCode, Post, Request, UseGuards } from '@nestjs/common';
import { ApiBearerAuth, ApiOperation, ApiTags } from '@nestjs/swagger';
import type { Request as ExpressRequest } from 'express';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import { BillingService } from './billing.service';
import { CreateCheckoutDto } from './dto/create-checkout.dto';
import { ChangePlanDto } from './dto/change-plan.dto';
import { SubscriptionResponseDto } from './dto/subscription-response.dto';

type ExpressRequestType = ExpressRequest;

@ApiTags('billing')
@Controller('billing')
export class BillingController {
  constructor(private readonly billingService: BillingService) {}

  @Post('create-checkout')
  @UseGuards(SupabaseAuthGuard)
  @ApiBearerAuth('JWT-auth')
  @ApiOperation({ summary: 'Create checkout session for subscription' })
  async createCheckout(
    @Request() req: ExpressRequestType,
    @Body() dto: CreateCheckoutDto,
  ): Promise<{ checkoutUrl: string; sessionId: string }> {
    const userId = req.user?.id;
    const userEmail = req.user?.email || '';

    if (!userId) {
      throw new Error('User not authenticated');
    }

    try {
      const result = await this.billingService.createCheckoutSession(
        userId,
        userEmail,
        userEmail.split('@')[0] || userEmail,
        dto,
      );

      // Ensure we return both fields
      if (!result.checkoutUrl) {
        throw new Error('Checkout URL is missing from service response');
      }

      // Log response to help debug
      console.log('Checkout response being sent to client:', JSON.stringify(result, null, 2));

      return result;
    } catch (error) {
      console.error('Error creating checkout session:', error);
      throw error;
    }
  }

  @Get('subscription')
  @UseGuards(SupabaseAuthGuard)
  @ApiBearerAuth('JWT-auth')
  @ApiOperation({ summary: 'Get current subscription for user' })
  @HttpCode(200)
  async getSubscription(
    @Request() req: ExpressRequestType,
  ): Promise<SubscriptionResponseDto | null> {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }

    const subscription = await this.billingService.getSubscription(userId);

    if (!subscription) {
      // Return null with 200 status - user just hasn't subscribed yet
      return null;
    }

    // Debug: Log subscription object to verify dodoCustomerId
    const subAny = subscription as any;
    console.log('=== CONTROLLER DEBUG ===');
    console.log('Subscription object from repository:', {
      id: subscription.id,
      dodoCustomerId: subAny.dodoCustomerId,
      dodo_customer_id: subAny.dodo_customer_id,
      dodoSubscriptionId: subscription.dodoSubscriptionId,
      hasDodoCustomerId: 'dodoCustomerId' in subscription,
      hasDodo_customer_id: 'dodo_customer_id' in subscription,
      allKeys: Object.keys(subscription),
      fullObject: JSON.stringify(subscription, null, 2),
    });
    console.log('========================');

    // Extract customer ID - try both camelCase and snake_case
    const customerId = subAny.dodoCustomerId || subAny.dodo_customer_id || null;

    return {
      id: subscription.id,
      userId: subscription.userId,
      dodoSubscriptionId: subscription.dodoSubscriptionId,
      dodoCustomerId: customerId, // Use extracted customer ID
      planId: subscription.planId as any,
      status: subscription.status as any,
      currentPeriodStart: subscription.currentPeriodStart,
      currentPeriodEnd: subscription.currentPeriodEnd,
      trialStart: subscription.trialStart,
      trialEnd: subscription.trialEnd,
      canceledAt: subscription.canceledAt,
      cancelAtPeriodEnd: subscription.cancelAtPeriodEnd,
      createdAt: subscription.createdAt,
      updatedAt: subscription.updatedAt,
    };
  }

  @Post('change-plan')
  @UseGuards(SupabaseAuthGuard)
  @ApiBearerAuth('JWT-auth')
  @ApiOperation({ summary: 'Change subscription plan - creates checkout session for new plan' })
  async changePlan(
    @Request() req: ExpressRequestType,
    @Body() dto: ChangePlanDto,
  ): Promise<{ checkoutUrl: string; sessionId: string }> {
    const userId = req.user?.id;
    const userEmail = req.user?.email || '';
    const userName = req.user?.email?.split('@')[0] || userEmail;

    if (!userId) {
      throw new Error('User not authenticated');
    }

    const result = await this.billingService.changePlan(userId, userEmail, userName, dto);

    // Ensure we return both fields
    if (!result.checkoutUrl) {
      throw new Error('Checkout URL is missing from service response');
    }

    console.log('Plan change checkout response:', JSON.stringify(result, null, 2));

    return result;
  }

  @Get('customer-portal')
  @UseGuards(SupabaseAuthGuard)
  @ApiBearerAuth('JWT-auth')
  @ApiOperation({ summary: 'Get customer portal URL for managing subscriptions and invoices' })
  async getCustomerPortalUrl(@Request() req: ExpressRequestType): Promise<{ portalUrl: string }> {
    const userId = req.user?.id;
    const userEmail = req.user?.email || '';

    if (!userId) {
      throw new Error('User not authenticated');
    }

    return await this.billingService.getCustomerPortalUrl(userId, userEmail);
  }

  @Post('webhook')
  @Post('webhook/') // Handle both with and without trailing slash (for ngrok redirects)
  @ApiOperation({ summary: 'Handle Dodo Payments webhook events' })
  async handleWebhook(
    @Body() body: unknown,
    @Headers('x-dodo-signature') signature?: string,
    @Headers('x-dodo-event-id') eventIdHeader?: string,
  ): Promise<{ received: boolean }> {
    // Log entire webhook request - use both console.log and logger for visibility
    const timestamp = new Date().toISOString();
    console.log('\n\n========================================');
    console.log('🔔 WEBHOOK RECEIVED');
    console.log('========================================');
    console.log('Timestamp:', timestamp);
    console.log('Signature:', signature || 'NOT PROVIDED');
    console.log('Event ID Header:', eventIdHeader || 'NOT PROVIDED');
    console.log('Full Body:', JSON.stringify(body, null, 2));
    console.log('Body Type:', typeof body);
    console.log('Body Keys:', body ? Object.keys(body as object) : 'null');
    console.log('========================================\n');

    try {
      // Dodo Payments webhook structure: { type, data, timestamp, business_id }
      const event = body as {
        type: string;
        data?: unknown;
        timestamp?: string;
        business_id?: string;
        id?: string;
      };

      console.log('📦 Parsed Event:');
      console.log('  - Type:', event.type);
      console.log('  - Business ID:', event.business_id);
      console.log('  - Timestamp:', event.timestamp);
      console.log('  - Event ID (root):', event.id || 'NOT PROVIDED');
      console.log('  - Event ID (header):', eventIdHeader || 'NOT PROVIDED');
      console.log('  - Data:', JSON.stringify(event.data, null, 2));

      // Extract event ID - priority: header > root > data fields > generated
      let eventId = eventIdHeader || event.id;
      if (!eventId && event.data) {
        const data = event.data as any;
        // Try to extract from common fields
        eventId = data.payment_id || data.subscription_id || data.event_id || data.id;
      }

      // If still no event ID, generate one (but log warning)
      if (!eventId) {
        eventId = `evt_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
        console.warn('⚠️  No event ID found, generated:', eventId);
      }

      console.log('🔍 Using Event ID:', eventId);

      await this.billingService.handleWebhook(event.type, eventId, event.data || event);

      console.log('✅ Webhook processed successfully');
      return { received: true };
    } catch (error) {
      console.error('\n❌ WEBHOOK ERROR:');
      console.error('Error:', error);
      console.error('Error Message:', error instanceof Error ? error.message : String(error));
      console.error('Stack:', error instanceof Error ? error.stack : 'No stack');
      console.error('========================================\n');

      // Don't throw error - return success so Dodo knows we received it
      // but log the error for debugging (Dodo will retry if needed)
      // Return received: true so Dodo doesn't keep retrying
      return { received: true };
    }
  }
}
