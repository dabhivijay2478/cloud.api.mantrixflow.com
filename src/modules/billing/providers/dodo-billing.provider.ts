/**
 * Dodo Payments Billing Provider
 * Implementation of IBillingProvider for Dodo Payments
 * Uses Dodo-hosted checkout pages (no custom payment UI)
 */

import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as crypto from 'crypto';
import type {
  IBillingProvider,
  CreateSubscriptionInput,
  SubscriptionResult,
  SubscriptionStatus,
  CancelSubscriptionInput,
} from './billing-provider.interface';
import { billingConfig, getPlanConfig } from '../../../config/billing.config';

@Injectable()
export class DodoBillingProvider implements IBillingProvider {
  private apiKey: string;
  private apiBaseUrl: string;
  private webhookSecret: string;

  constructor(private readonly configService: ConfigService) {
    // All keys come from environment variables (.env file) - no hardcoded values
    this.apiKey = this.configService.get<string>('DODO_API_KEY') || '';
    this.webhookSecret = this.configService.get<string>('DODO_WEBHOOK_SECRET') || '';
    this.apiBaseUrl =
      this.configService.get<string>('DODO_API_BASE_URL') ||
      billingConfig.dodo.apiBaseUrl;

    if (!this.apiKey) {
      console.warn(
        '⚠️  Dodo Payments API key not configured. Billing features will not work until DODO_API_KEY is set in .env file.',
      );
    }
  }

  /**
   * Create a new subscription
   * Returns checkout URL for Dodo-hosted checkout page
   */
  async createSubscription(input: CreateSubscriptionInput): Promise<SubscriptionResult> {
    if (!this.apiKey) {
      throw new Error(
        'Dodo Payments is not configured. Please set DODO_API_KEY in your .env file.',
      );
    }

    const { organizationId, planId, interval, customerEmail, customerName, returnUrl, cancelUrl } =
      input;

    // Validate plan ID
    if (!['free', 'pro', 'scale'].includes(planId)) {
      throw new Error(`Invalid plan ID: ${planId}`);
    }

    // Free plan doesn't use Dodo Payments
    if (planId === 'free') {
      throw new Error('Free plan does not require payment');
    }

    // Get plan config to get Dodo product ID
    const planConfig = getPlanConfig(planId as 'pro' | 'scale');
    const dodoProductId = planConfig.dodoProductId || billingConfig.dodo.productIds[planId];

    if (!dodoProductId) {
      throw new Error(
        `Dodo product ID not configured for plan ${planId}. Please set DODO_${planId.toUpperCase()}_PRODUCT_ID in .env file.`,
      );
    }

    try {
      // Create checkout session with Dodo Payments API
      const checkoutResponse = await this.callDodoAPI('/checkout/sessions', {
        method: 'POST',
        body: {
          product_id: dodoProductId,
          customer_email: customerEmail,
          customer_name: customerName,
          billing_cycle: interval === 'month' ? 'monthly' : 'yearly',
          metadata: {
            organization_id: organizationId,
            plan_id: planId,
          },
          success_url: returnUrl,
          cancel_url: cancelUrl,
        },
      });

      // Dodo returns a checkout URL and session ID
      const checkoutUrl = checkoutResponse.checkout_url;
      const sessionId = checkoutResponse.session_id;

      if (!checkoutUrl) {
        throw new Error('Dodo Payments did not return a checkout URL');
      }

      return {
        subscriptionId: sessionId, // Use session ID as temporary identifier
        checkoutUrl: checkoutUrl, // Dodo-hosted checkout URL
        status: 'pending',
      };
    } catch (error: any) {
      console.error('Error creating Dodo checkout session:', error);
      throw new Error(
        `Failed to create checkout session: ${error?.message || 'Unknown error'}`,
      );
    }
  }

  /**
   * Get subscription status
   */
  async getSubscription(subscriptionId: string): Promise<SubscriptionStatus> {
    if (!this.apiKey) {
      throw new Error('Dodo Payments is not configured');
    }

    try {
      const subscription = await this.callDodoAPI(`/subscriptions/${subscriptionId}`, {
        method: 'GET',
      });

      return {
        subscriptionId: subscription.id,
        planId: subscription.metadata?.plan_id || 'free',
        status: this.mapDodoStatus(subscription.status),
        currentPeriodStart: subscription.current_period_start
          ? new Date(subscription.current_period_start)
          : null,
        currentPeriodEnd: subscription.current_period_end
          ? new Date(subscription.current_period_end)
          : null,
        cancelAtPeriodEnd: subscription.cancel_at_period_end || false,
        amount: subscription.amount ? subscription.amount / 100 : 0, // Convert from cents
        currency: subscription.currency || 'INR',
      };
    } catch (error) {
      console.error('Error fetching Dodo subscription:', error);
      throw new Error('Failed to fetch subscription');
    }
  }

  /**
   * Cancel subscription
   */
  async cancelSubscription(input: CancelSubscriptionInput): Promise<void> {
    if (!this.apiKey) {
      throw new Error('Dodo Payments is not configured');
    }

    const { subscriptionId, cancelImmediately } = input;

    try {
      await this.callDodoAPI(`/subscriptions/${subscriptionId}/cancel`, {
        method: 'POST',
        body: {
          cancel_immediately: cancelImmediately || false,
        },
      });
    } catch (error) {
      console.error('Error canceling Dodo subscription:', error);
      throw new Error('Failed to cancel subscription');
    }
  }

  /**
   * Verify webhook signature
   */
  verifyWebhookSignature(payload: string | Buffer, signature: string): boolean {
    if (!this.webhookSecret) {
      console.warn('DODO_WEBHOOK_SECRET not set, skipping signature verification');
      return true; // In development, allow without secret
    }

    const payloadString = typeof payload === 'string' ? payload : payload.toString();
    const expectedSignature = crypto
      .createHmac('sha256', this.webhookSecret)
      .update(payloadString)
      .digest('hex');

    return crypto.timingSafeEqual(Buffer.from(signature), Buffer.from(expectedSignature));
  }

  /**
   * Handle webhook event
   */
  async handleWebhookEvent(event: unknown, signature: string): Promise<void> {
    // Signature verification is done in the controller
    const eventData = event as any;

    // Dodo Payments webhook events
    switch (eventData.event_type) {
      case 'subscription.created':
      case 'subscription.active':
        await this.handleSubscriptionActivated(eventData);
        break;

      case 'subscription.updated':
        await this.handleSubscriptionUpdated(eventData);
        break;

      case 'subscription.cancelled':
        await this.handleSubscriptionCancelled(eventData);
        break;

      case 'payment.succeeded':
        await this.handlePaymentSucceeded(eventData);
        break;

      case 'payment.failed':
        await this.handlePaymentFailed(eventData);
        break;

      default:
        console.log(`Unhandled Dodo webhook event: ${eventData.event_type}`);
    }
  }

  /**
   * Map Dodo status to our status
   */
  private mapDodoStatus(dodoStatus: string): SubscriptionStatus['status'] {
    const statusMap: Record<string, SubscriptionStatus['status']> = {
      active: 'active',
      trialing: 'trialing',
      past_due: 'past_due',
      cancelled: 'canceled',
      unpaid: 'unpaid',
      incomplete: 'incomplete',
      paused: 'incomplete',
    };

    return statusMap[dodoStatus] || 'incomplete';
  }

  /**
   * Handle subscription activated event
   */
  private async handleSubscriptionActivated(event: any): Promise<void> {
    // This will be handled by the billing service
    // which has access to repositories
    console.log('Subscription activated:', event.data?.subscription?.id);
  }

  /**
   * Handle subscription updated event
   */
  private async handleSubscriptionUpdated(event: any): Promise<void> {
    console.log('Subscription updated:', event.data?.subscription?.id);
  }

  /**
   * Handle subscription cancelled event
   */
  private async handleSubscriptionCancelled(event: any): Promise<void> {
    console.log('Subscription cancelled:', event.data?.subscription?.id);
  }

  /**
   * Handle payment succeeded event
   */
  private async handlePaymentSucceeded(event: any): Promise<void> {
    console.log('Payment succeeded:', event.data?.payment?.id);
  }

  /**
   * Handle payment failed event
   */
  private async handlePaymentFailed(event: any): Promise<void> {
    console.log('Payment failed:', event.data?.payment?.id);
  }

  /**
   * Call Dodo Payments API
   * Helper method to make authenticated API calls
   */
  private async callDodoAPI(
    endpoint: string,
    options: {
      method: 'GET' | 'POST' | 'PUT' | 'DELETE';
      body?: Record<string, unknown>;
    },
  ): Promise<any> {
    const url = `${this.apiBaseUrl}${endpoint}`;
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${this.apiKey}`,
    };

    const fetchOptions: RequestInit = {
      method: options.method,
      headers,
    };

    if (options.body && (options.method === 'POST' || options.method === 'PUT')) {
      fetchOptions.body = JSON.stringify(options.body);
    }

    const response = await fetch(url, fetchOptions);

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(
        `Dodo API error: ${response.status} ${response.statusText} - ${JSON.stringify(errorData)}`,
      );
    }

    return response.json();
  }
}
