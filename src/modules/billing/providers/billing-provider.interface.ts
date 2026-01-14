/**
 * Billing Provider Interface
 * Provider-agnostic interface for billing operations
 * Implementations: DodoBillingProvider, RazorpayBillingProvider (deprecated), StripeBillingProvider (future)
 */

export interface CreateSubscriptionInput {
  organizationId: string;
  planId: string;
  interval: 'month' | 'year';
  customerEmail: string;
  customerName: string;
  customerId?: string; // Optional: existing customer ID (provider-specific)
  returnUrl: string;
  cancelUrl: string;
}

export interface SubscriptionResult {
  subscriptionId: string;
  checkoutUrl?: string; // For redirect-based checkout
  checkoutData?: Record<string, unknown>; // For custom checkout (deprecated - use checkoutUrl)
  status: 'pending' | 'active' | 'trialing';
}

export interface SubscriptionStatus {
  subscriptionId: string;
  planId: string;
  status: 'active' | 'trialing' | 'past_due' | 'canceled' | 'unpaid' | 'incomplete';
  currentPeriodStart: Date | null;
  currentPeriodEnd: Date | null;
  cancelAtPeriodEnd: boolean;
  amount: number;
  currency: string;
}

export interface CancelSubscriptionInput {
  subscriptionId: string;
  organizationId: string;
  cancelImmediately?: boolean;
}

/**
 * Billing Provider Interface
 * All billing providers must implement this interface
 */
export interface IBillingProvider {
  /**
   * Create a new subscription
   */
  createSubscription(input: CreateSubscriptionInput): Promise<SubscriptionResult>;

  /**
   * Get subscription status
   */
  getSubscription(subscriptionId: string): Promise<SubscriptionStatus>;

  /**
   * Cancel a subscription
   */
  cancelSubscription(input: CancelSubscriptionInput): Promise<void>;

  /**
   * Handle webhook event
   */
  handleWebhookEvent(event: unknown, signature: string): Promise<void>;

  /**
   * Verify webhook signature
   */
  verifyWebhookSignature(payload: string | Buffer, signature: string): boolean;
}
