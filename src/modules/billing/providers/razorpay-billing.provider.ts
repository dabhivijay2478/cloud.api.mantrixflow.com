/**
 * Razorpay Billing Provider
 * Implementation of IBillingProvider for Razorpay
 */

import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as crypto from 'crypto';

// Razorpay is a CommonJS module - use require for compatibility
// eslint-disable-next-line @typescript-eslint/no-require-imports
const Razorpay = require('razorpay');

// Type for Razorpay instance (using any since Razorpay doesn't have proper TypeScript types)
type RazorpayInstance = any;
import type {
  IBillingProvider,
  CreateSubscriptionInput,
  SubscriptionResult,
  SubscriptionStatus,
  CancelSubscriptionInput,
} from './billing-provider.interface';
import { getPlanPrice } from '../../../config/billing.config';

@Injectable()
export class RazorpayBillingProvider implements IBillingProvider {
  private razorpay: RazorpayInstance | null = null;
  private webhookSecret: string;

  constructor(private readonly configService: ConfigService) {
    // All keys come from environment variables (.env file) - no hardcoded values
    const keyId = this.configService.get<string>('RAZORPAY_KEY_ID');
    const keySecret = this.configService.get<string>('RAZORPAY_KEY_SECRET');
    this.webhookSecret = this.configService.get<string>('RAZORPAY_WEBHOOK_SECRET') || '';

    // Initialize Razorpay only if keys are provided in .env file
    // All keys come from environment variables - no hardcoded values
    if (keyId && keySecret) {
      this.razorpay = new Razorpay({
        key_id: keyId,
        key_secret: keySecret,
      });
    } else {
      console.warn(
        '⚠️  Razorpay keys not configured. Billing features will not work until RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET are set in .env file.',
      );
      // Don't initialize Razorpay - will throw error when actually used
      // This ensures no hardcoded keys are used
    }
  }

  /**
   * Create a new subscription
   */
  async createSubscription(input: CreateSubscriptionInput): Promise<SubscriptionResult> {
    // Get keys from environment variables (.env file) - no hardcoded values
    const keyId = this.configService.get<string>('RAZORPAY_KEY_ID');
    const keySecret = this.configService.get<string>('RAZORPAY_KEY_SECRET');

    if (!keyId || !keySecret) {
      throw new Error(
        'Razorpay is not configured. Please set RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET in your .env file.',
      );
    }

    // Initialize Razorpay with keys from .env file if not already initialized
    // All keys come from environment variables - no hardcoded values
    if (!this.razorpay) {
      this.razorpay = new Razorpay({
        key_id: keyId,
        key_secret: keySecret,
      });
    }

    const { organizationId, planId, interval, customerEmail, customerName, returnUrl, cancelUrl } =
      input;

    // Validate plan ID
    if (!['free', 'pro', 'scale'].includes(planId)) {
      throw new Error(`Invalid plan ID: ${planId}`);
    }

    // Get plan price (in paise for Razorpay - multiply by 100)
    const priceInBaseCurrency = getPlanPrice(planId as 'free' | 'pro' | 'scale', interval);
    const amountInPaise = Math.round(priceInBaseCurrency * 100); // Convert to paise

    // Create Razorpay plan (if not exists, create on-the-fly)
    const planName = `${planId}_${interval}`;
    let razorpayPlanId: string;

    if (!this.razorpay) {
      throw new Error('Razorpay is not initialized');
    }

    try {
      // Try to get existing plan
      const plans = await this.razorpay.plans.all({ count: 100 });
      const existingPlan = plans.items.find(
        (p: any) => p.item?.name === planName || p.name === planName,
      );

      if (existingPlan) {
        razorpayPlanId = existingPlan.id;
      } else {
        // Create new plan
        const newPlan = await this.razorpay.plans.create({
          period: interval === 'month' ? 'monthly' : 'yearly',
          interval: 1,
          item: {
            name: planName,
            amount: amountInPaise,
            currency: 'INR',
            description: `${planId} plan - ${interval}ly`,
          },
        });
        razorpayPlanId = newPlan.id;
      }
    } catch (error) {
      console.error('Error creating/fetching Razorpay plan:', error);
      throw new Error('Failed to create subscription plan');
    }

    // Create Razorpay customer
    if (!this.razorpay) {
      throw new Error('Razorpay is not initialized');
    }

    let customer;
    try {
      customer = await this.razorpay.customers.create({
        name: customerName,
        email: customerEmail,
        notes: {
          organizationId,
        },
      });
    } catch (error) {
      console.error('Error creating Razorpay customer:', error);
      throw new Error('Failed to create customer');
    }

    // Create subscription
    try {
      const subscription = await this.razorpay.subscriptions.create({
        plan_id: razorpayPlanId,
        customer_notify: 1,
        total_count: interval === 'month' ? 12 : 1, // Monthly: 12 cycles, Yearly: 1 cycle
        notes: {
          organizationId,
          planId,
          interval,
        },
      });

      return {
        subscriptionId: subscription.id,
        checkoutData: {
          subscriptionId: subscription.id,
          customerId: customer.id,
          amount: amountInPaise,
          currency: 'INR',
          keyId: this.configService.get<string>('RAZORPAY_KEY_ID'),
          returnUrl,
          cancelUrl,
          planId,
          interval,
        },
        status: subscription.status === 'created' ? 'pending' : 'active',
      };
    } catch (error) {
      console.error('Error creating Razorpay subscription:', error);
      throw new Error('Failed to create subscription');
    }
  }

  /**
   * Get subscription status
   */
  async getSubscription(subscriptionId: string): Promise<SubscriptionStatus> {
    if (!this.razorpay) {
      throw new Error('Razorpay is not initialized');
    }

    try {
      const subscription = await this.razorpay.subscriptions.fetch(subscriptionId);

      const planId = (subscription.notes as any)?.planId || 'free';
      let amount = 0;

      // Fetch plan to get amount
      if (subscription.plan_id && this.razorpay) {
        try {
          const plan = await this.razorpay.plans.fetch(subscription.plan_id as string);
          amount = ((plan as any).item?.amount || (plan as any).amount || 0) / 100;
        } catch (error) {
          console.error('Error fetching plan:', error);
        }
      }

      return {
        subscriptionId: subscription.id,
        planId,
        status: this.mapRazorpayStatus(subscription.status),
        currentPeriodStart: subscription.created_at
          ? new Date(subscription.created_at * 1000)
          : null,
        currentPeriodEnd: (subscription as any).end_at
          ? new Date((subscription as any).end_at * 1000)
          : null,
        cancelAtPeriodEnd: (subscription as any).end_at !== null,
        amount,
        currency: 'INR',
      };
    } catch (error) {
      console.error('Error fetching Razorpay subscription:', error);
      throw new Error('Failed to fetch subscription');
    }
  }

  /**
   * Cancel subscription
   */
  async cancelSubscription(input: CancelSubscriptionInput): Promise<void> {
    if (!this.razorpay) {
      throw new Error('Razorpay is not initialized');
    }

    const { subscriptionId, cancelImmediately } = input;

    try {
      if (cancelImmediately) {
        await this.razorpay.subscriptions.cancel(subscriptionId);
      } else {
        // Cancel at period end - Razorpay doesn't support cancel_at_cycle_end directly
        // We'll cancel immediately but can track in our DB
        await this.razorpay.subscriptions.cancel(subscriptionId);
      }
    } catch (error) {
      console.error('Error canceling Razorpay subscription:', error);
      throw new Error('Failed to cancel subscription');
    }
  }

  /**
   * Verify webhook signature
   */
  verifyWebhookSignature(payload: string | Buffer, signature: string): boolean {
    if (!this.webhookSecret) {
      console.warn('RAZORPAY_WEBHOOK_SECRET not set, skipping signature verification');
      return true; // In development, allow without secret
    }

    const payloadString = typeof payload === 'string' ? payload : payload.toString();
    const expectedSignature = crypto
      .createHmac('sha256', this.webhookSecret)
      .update(payloadString)
      .digest('hex');

    return crypto.timingSafeEqual(
      Buffer.from(signature),
      Buffer.from(expectedSignature),
    );
  }

  /**
   * Handle webhook event
   */
  async handleWebhookEvent(event: unknown, signature: string): Promise<void> {
    // Signature verification is done in the controller
    const eventData = event as any;

    // Razorpay webhook events
    switch (eventData.event) {
      case 'subscription.activated':
      case 'subscription.charged':
        await this.handleSubscriptionActivated(eventData);
        break;

      case 'subscription.cancelled':
        await this.handleSubscriptionCancelled(eventData);
        break;

      case 'payment.failed':
        await this.handlePaymentFailed(eventData);
        break;

      default:
        console.log(`Unhandled Razorpay webhook event: ${eventData.event}`);
    }
  }

  /**
   * Map Razorpay status to our status
   */
  private mapRazorpayStatus(razorpayStatus: string): SubscriptionStatus['status'] {
    const statusMap: Record<string, SubscriptionStatus['status']> = {
      created: 'incomplete',
      authenticated: 'incomplete',
      active: 'active',
      pending: 'incomplete',
      halted: 'past_due',
      cancelled: 'canceled',
      completed: 'canceled',
      expired: 'canceled',
    };

    return statusMap[razorpayStatus] || 'incomplete';
  }

  /**
   * Handle subscription activated event
   */
  private async handleSubscriptionActivated(event: any): Promise<void> {
    // This will be handled by the billing service
    // which has access to repositories
    const subscriptionId = event.payload?.subscription?.entity?.id;
    console.log('Subscription activated:', subscriptionId);
  }

  /**
   * Handle subscription cancelled event
   */
  private async handleSubscriptionCancelled(event: any): Promise<void> {
    const subscriptionId = event.payload?.subscription?.entity?.id;
    console.log('Subscription cancelled:', subscriptionId);
  }

  /**
   * Handle payment failed event
   */
  private async handlePaymentFailed(event: any): Promise<void> {
    const paymentId = event.payload?.payment?.entity?.id;
    console.log('Payment failed:', paymentId);
  }
}
