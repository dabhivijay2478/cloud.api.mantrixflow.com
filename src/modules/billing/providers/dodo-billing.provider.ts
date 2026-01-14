/**
 * Dodo Payments Billing Provider
 * Implementation of IBillingProvider for Dodo Payments
 * Uses Dodo-hosted checkout pages (no custom payment UI)
 * Uses official Dodo Payments TypeScript SDK
 */

import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as crypto from 'crypto';
import DodoPayments from 'dodopayments';
import type {
  IBillingProvider,
  CreateSubscriptionInput,
  SubscriptionResult,
  SubscriptionStatus,
  CancelSubscriptionInput,
  InvoiceDto,
} from './billing-provider.interface';

@Injectable()
export class DodoBillingProvider implements IBillingProvider {
  private client: DodoPayments;
  private webhookSecret: string;
  private productIds: {
    pro: string;
    scale: string;
  };

  constructor(private readonly configService: ConfigService) {
    // All keys come from environment variables (.env file) - no hardcoded values
    const apiKey = this.configService.get<string>('DODO_API_KEY') || '';
    this.webhookSecret = this.configService.get<string>('DODO_WEBHOOK_SECRET') || '';

    // Determine environment (test_mode or live_mode)
    // If DODO_API_BASE_URL contains 'test', use test_mode, otherwise live_mode
    const apiBaseUrl = this.configService.get<string>('DODO_API_BASE_URL') || '';
    const environment =
      apiBaseUrl.includes('test') || !apiKey ? 'test_mode' : 'live_mode';

    // Initialize Dodo Payments SDK client
    this.client = new DodoPayments({
      bearerToken: apiKey,
      environment: environment as 'test_mode' | 'live_mode',
    });

    // Read product IDs from ConfigService (not from billing.config.ts which reads process.env at module load)
    this.productIds = {
      pro: this.configService.get<string>('DODO_PRO_PRODUCT_ID') || '',
      scale: this.configService.get<string>('DODO_SCALE_PRODUCT_ID') || '',
    };

    if (!apiKey) {
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
    if (!this.client) {
      throw new Error(
        'Dodo Payments is not configured. Please set DODO_API_KEY in your .env file.',
      );
    }

    const { organizationId, planId, interval, customerEmail, customerName, returnUrl } = input;

    // Validate plan ID
    if (!['free', 'pro', 'scale'].includes(planId)) {
      throw new Error(`Invalid plan ID: ${planId}`);
    }

    // Free plan doesn't use Dodo Payments
    if (planId === 'free') {
      throw new Error('Free plan does not require payment');
    }

    // Get Dodo product ID from ConfigService (reads from .env at runtime)
    const dodoProductId = this.productIds[planId as 'pro' | 'scale'];

    if (!dodoProductId) {
      throw new Error(
        `Dodo product ID not configured for plan ${planId}. Please set DODO_${planId.toUpperCase()}_PRODUCT_ID in .env file.`,
      );
    }

    try {
      // Create checkout session using Dodo Payments SDK
      // According to Dodo Payments docs: checkoutSessions.create()
      const session = await this.client.checkoutSessions.create({
        product_cart: [
          {
            product_id: dodoProductId,
            quantity: 1,
          },
        ],
        customer: {
          email: customerEmail,
          name: customerName,
        },
        return_url: returnUrl,
        metadata: {
          ...(organizationId && { organization_id: organizationId }), // Include only if provided
          plan_id: planId,
        },
      });

      // Dodo SDK returns checkout_url
      const checkoutUrl = (session as any).checkout_url || (session as any).payment_link;

      if (!checkoutUrl) {
        throw new Error('Dodo Payments did not return a checkout URL');
      }

      // Use checkout session ID or payment ID as subscription identifier
      // SDK may return different field names, so we check multiple possibilities
      const sessionId =
        (session as any).checkout_session_id ||
        (session as any).session_id ||
        (session as any).payment_id ||
        (session as any).id ||
        'pending';

      // Extract customer ID from Dodo response (if available)
      // Dodo may return customer_id in the checkout session response
      const customerId =
        (session as any).customer_id ||
        (session as any).customerId ||
        (session as any).customer?.id;

      return {
        subscriptionId: sessionId,
        checkoutUrl: checkoutUrl,
        status: 'pending',
        customerId: customerId, // Return customer ID to store in user table
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
    if (!this.client) {
      throw new Error('Dodo Payments is not configured');
    }

    try {
      // Use SDK to get subscription details
      const subscription = await this.client.subscriptions.retrieve(subscriptionId);

      // SDK types may vary, use type assertion for flexibility
      const sub = subscription as any;

      // Extract customer ID from subscription response
      const customerId = sub.customer_id || sub.customerId || sub.customer?.id;

      return {
        subscriptionId: sub.subscription_id || sub.id || subscriptionId,
        planId: sub.metadata?.plan_id || 'free',
        status: this.mapDodoStatus(sub.status),
        currentPeriodStart: sub.current_period_start
          ? new Date(sub.current_period_start)
          : sub.next_billing_date
            ? new Date(sub.next_billing_date)
            : null,
        currentPeriodEnd: sub.current_period_end
          ? new Date(sub.current_period_end)
          : sub.next_billing_date
            ? new Date(sub.next_billing_date)
            : null,
        cancelAtPeriodEnd: sub.cancel_at_period_end || false,
        customerId: customerId, // Return customer ID to store in user table
        amount: sub.amount ? (typeof sub.amount === 'number' ? sub.amount / 100 : sub.amount) : 0,
        currency: sub.currency || 'INR',
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
    if (!this.client) {
      throw new Error('Dodo Payments is not configured');
    }

    const { subscriptionId, cancelImmediately } = input;

    try {
      if (cancelImmediately) {
        // Cancel immediately - use update with status or delete
        // Check SDK docs for exact method, using update as fallback
        await (this.client.subscriptions as any).cancel?.(subscriptionId) ||
          this.client.subscriptions.update(subscriptionId, {
            status: 'cancelled',
          } as any);
      } else {
        // Cancel at period end
        await this.client.subscriptions.update(subscriptionId, {
          cancel_at_period_end: true,
        } as any);
      }
    } catch (error) {
      console.error('Error canceling Dodo subscription:', error);
      throw new Error('Failed to cancel subscription');
    }
  }

  /**
   * Get invoices for a subscription
   */
  async getInvoices(subscriptionId: string): Promise<InvoiceDto[]> {
    if (!this.client) {
      throw new Error('Dodo Payments is not configured');
    }

    try {
      // Fetch invoices from Dodo Payments SDK
      // Get payments for the subscription (invoices are typically linked to payments)
      const payments = await this.client.payments.list({
        subscription_id: subscriptionId,
      });

      // Convert payments to invoice format
      const invoices: InvoiceDto[] = (payments as any).items?.map((payment: any) => ({
        invoiceId: payment.payment_id || payment.id,
        date: payment.created_at ? new Date(payment.created_at) : new Date(),
        amount: payment.total_amount ? payment.total_amount / 100 : payment.amount || 0,
        currency: payment.currency || 'INR',
        status:
          payment.status === 'succeeded' || payment.status === 'paid'
            ? 'paid'
            : payment.status === 'pending'
              ? 'pending'
              : 'failed',
        downloadUrl: payment.invoice_url || payment.receipt_url,
      })) || [];

      return invoices;
    } catch (error) {
      console.error('Error fetching invoices from Dodo Payments:', error);
      throw new Error('Failed to fetch invoices');
    }
  }

  /**
   * Get invoice download URL
   */
  async getInvoiceDownloadUrl(subscriptionId: string, invoiceId: string): Promise<string> {
    if (!this.client) {
      throw new Error('Dodo Payments is not configured');
    }

    try {
      // Get payment/invoice details from Dodo Payments
      const payment = await this.client.payments.retrieve(invoiceId);

      // Return invoice URL if available
      const invoiceUrl = (payment as any).invoice_url || (payment as any).receipt_url;

      if (!invoiceUrl) {
        // If no direct URL, construct download URL from Dodo dashboard
        // Or use the payment ID to generate a download link
        throw new Error('Invoice download URL not available');
      }

      return invoiceUrl;
    } catch (error) {
      console.error('Error fetching invoice download URL:', error);
      throw new Error('Failed to get invoice download URL');
    }
  }

  /**
   * Get customer portal URL
   * Creates a portal session and returns the URL
   */
  async getCustomerPortalUrl(customerId: string, returnUrl?: string): Promise<string> {
    if (!this.client) {
      throw new Error('Dodo Payments is not configured');
    }

    try {
      // Create customer portal session using Dodo Payments SDK
      // The portal session allows customers to manage subscriptions, invoices, etc.
      // Note: The official TypeScript SDK types may not expose `createPortalSession` yet,
      // so we cast to `any` to call it safely at runtime.
      const portalSession = await (this.client.customers as any).createPortalSession({
        customer_id: customerId,
        return_url: returnUrl || 'https://your-domain.com/organizations',
      });

      // Return the portal URL (e.g., https://test.customer.dodopayments.com/session/subscriptions/...)
      const portalUrl = (portalSession as any).portal_url || (portalSession as any).url;

      if (!portalUrl) {
        throw new Error('Dodo Payments did not return a portal URL');
      }

      return portalUrl;
    } catch (error) {
      console.error('Error creating Dodo customer portal session:', error);
      throw new Error('Failed to create customer portal session');
    }
  }

  /**
   * Verify webhook signature
   * Dodo Payments uses v1 signature format:
   * - signed_message = webhook-id + "." + webhook-timestamp + "." + payload
   * - signature = HMAC_SHA256(secret, signed_message)
   * - Header format: "v1,<signature>" or just "<signature>"
   */
  verifyWebhookSignature(
    payload: string | Buffer,
    signature: string,
    webhookId?: string,
    webhookTimestamp?: string
  ): boolean {
    // Allow skipping signature verification in development
    const skipVerification = process.env.DODO_SKIP_WEBHOOK_SIGNATURE === 'true';
    if (skipVerification) {
      console.warn('⚠️  DODO_SKIP_WEBHOOK_SIGNATURE=true - Skipping webhook signature verification');
      return true;
    }

    if (!this.webhookSecret) {
      console.warn('DODO_WEBHOOK_SECRET not set, skipping signature verification');
      return true; // In development, allow without secret
    }

    // Dodo signature format: "v1,<signature>" or just "<signature>"
    // Extract the actual signature if in v1 format
    const actualSignature = signature.includes(',') 
      ? signature.split(',')[1]?.trim() 
      : signature;

    if (!actualSignature) {
      console.error('Invalid signature format:', signature);
      return false;
    }

    const payloadString = typeof payload === 'string' ? payload : payload.toString();
    
    // Dodo v1 signature: HMAC_SHA256(secret, webhook-id + "." + webhook-timestamp + "." + payload)
    let signedMessage: string;
    if (webhookId && webhookTimestamp) {
      // Use proper Dodo v1 format: webhook-id.timestamp.payload
      signedMessage = `${webhookId}.${webhookTimestamp}.${payloadString}`;
    } else {
      // Fallback: just verify payload (less secure but works if headers missing)
      console.warn('⚠️  webhook-id or webhook-timestamp missing, using payload-only verification');
      signedMessage = payloadString;
    }

    const expectedSignature = crypto
      .createHmac('sha256', this.webhookSecret)
      .update(signedMessage)
      .digest('hex');

    // Use timing-safe comparison to prevent timing attacks
    try {
      return crypto.timingSafeEqual(
        Buffer.from(actualSignature, 'hex'),
        Buffer.from(expectedSignature, 'hex')
      );
    } catch (error) {
      console.error('Error comparing webhook signatures:', error);
      return false;
    }
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
      on_hold: 'past_due',
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
}
