/**
 * Billing Provider Interface
 * Provider-agnostic interface for billing operations
 * Implementations: DodoBillingProvider
 */

export interface CreateSubscriptionInput {
  organizationId?: string; // Optional: for metadata only (billing is user-scoped)
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
  customerId?: string; // Customer ID returned from provider (for storing in user table)
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
  customerId?: string; // Customer ID from provider (for storing in user table)
}

export interface CancelSubscriptionInput {
  subscriptionId: string;
  organizationId?: string; // Optional: for metadata only (billing is user-scoped)
  cancelImmediately?: boolean;
}

export interface InvoiceDto {
  invoiceId: string;
  date: Date;
  amount: number;
  currency: string;
  status: 'paid' | 'pending' | 'failed';
  downloadUrl?: string;
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
   * Get invoices for a subscription
   */
  getInvoices(subscriptionId: string): Promise<InvoiceDto[]>;

  /**
   * Get invoice download URL
   */
  getInvoiceDownloadUrl(subscriptionId: string, invoiceId: string): Promise<string>;

  /**
   * Get customer portal URL
   * Returns Dodo-hosted customer portal session URL
   */
  getCustomerPortalUrl(customerId: string, returnUrl?: string): Promise<string>;

  /**
   * Handle webhook event
   */
  handleWebhookEvent(event: unknown, signature: string): Promise<void>;

  /**
   * Verify webhook signature
   */
  verifyWebhookSignature(
    payload: string | Buffer, 
    signature: string,
    webhookId?: string,
    webhookTimestamp?: string
  ): boolean;
}
