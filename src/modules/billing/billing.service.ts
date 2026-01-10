/**
 * Billing Service
 * Stripe integration for billing management
 * One Stripe Customer per organization
 */

import { BadRequestException, ForbiddenException, Injectable, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import Stripe from 'stripe';
import { BillingRepository } from './repositories/billing.repository';
import { OrganizationOwnerRepository } from '../organizations/repositories/organization-owner.repository';
import { OrganizationMemberRepository } from '../organizations/repositories/organization-member.repository';
import { OrganizationRepository } from '../organizations/repositories/organization.repository';
import type {
  BillingInvoiceDto,
  BillingOverviewDto,
  BillingUsageDto,
} from './dto/billing-response.dto';

@Injectable()
export class BillingService {
  private stripe: Stripe;

  constructor(
    private readonly configService: ConfigService,
    private readonly billingRepository: BillingRepository,
    private readonly organizationRepository: OrganizationRepository,
    private readonly ownerRepository: OrganizationOwnerRepository,
    private readonly memberRepository: OrganizationMemberRepository,
  ) {
    const stripeSecretKey = this.configService.get<string>('STRIPE_SECRET_KEY');
    if (!stripeSecretKey) {
      throw new Error('STRIPE_SECRET_KEY is required');
    }

    // Initialize Stripe with India configuration
    this.stripe = new Stripe(stripeSecretKey, {
      apiVersion: '2025-12-15.clover',
      typescript: true,
    });
  }

  /**
   * Check if user has permission to access billing (OWNER or ADMIN only)
   */
  private async checkBillingPermission(
    organizationId: string,
    userId: string,
  ): Promise<void> {
    // Check if organization exists
    const organization = await this.organizationRepository.findById(organizationId);
    if (!organization) {
      throw new NotFoundException(`Organization with ID "${organizationId}" not found`);
    }

    // Check if user is an owner
    const isOwner = await this.ownerRepository.isOwner(organizationId, userId);
    if (isOwner) {
      return; // Owners have access
    }

    // Check if user is an admin member
    const member = await this.memberRepository.findByOrganizationAndUserId(
      organizationId,
      userId,
    );
    if (member && (member.role === 'owner' || member.role === 'admin')) {
      return; // Admins have access
    }

    // User is not owner or admin
    throw new ForbiddenException(
      'Only organization owners and admins can access billing information',
    );
  }

  /**
   * Get or create Stripe customer for organization
   */
  private async getOrCreateStripeCustomer(organizationId: string): Promise<string> {
    // Check if billing subscription already exists
    let billing = await this.billingRepository.findByOrganizationId(organizationId);

    if (billing && billing.stripeCustomerId) {
      return billing.stripeCustomerId;
    }

    // Get organization details
    const organization = await this.organizationRepository.findById(organizationId);
    if (!organization) {
      throw new NotFoundException(`Organization with ID "${organizationId}" not found`);
    }

    // Create Stripe customer
    const customer = await this.stripe.customers.create({
      name: organization.name,
      metadata: {
        organizationId: organization.id,
      },
    });

    // Create or update billing record
    if (billing) {
      await this.billingRepository.update(billing.id, {
        stripeCustomerId: customer.id,
      });
    } else {
      billing = await this.billingRepository.create({
        organizationId: organization.id,
        stripeCustomerId: customer.id,
        billingStatus: 'incomplete',
      });
    }

    return customer.id;
  }

  /**
   * Get billing overview for an organization
   */
  async getBillingOverview(
    organizationId: string,
    userId: string,
  ): Promise<BillingOverviewDto> {
    await this.checkBillingPermission(organizationId, userId);

    // Get billing subscription
    const billing = await this.billingRepository.findByOrganizationId(organizationId);

    if (!billing || !billing.stripeCustomerId) {
      // No billing setup yet
      return {
        currentPlan: 'free',
        billingStatus: 'incomplete',
        nextBillingDate: null,
        amount: 0,
        currency: 'INR',
      };
    }

    // Fetch subscription from Stripe
    let subscription: Stripe.Subscription | null = null;
    if (billing.stripeSubscriptionId) {
      try {
        subscription = await this.stripe.subscriptions.retrieve(
          billing.stripeSubscriptionId,
        );
      } catch (error) {
        // Subscription might not exist in Stripe
        console.error('Error fetching Stripe subscription:', error);
      }
    }

    // Determine plan and status
    let currentPlan = billing.planId || 'free';
    let billingStatus: 'active' | 'trial' | 'expired' | 'incomplete' = 'incomplete';
    let nextBillingDate: Date | null = null;
    let amount = 0;
    const currency = 'INR'; // India

    if (subscription) {
      // Map Stripe status to our status
      if (subscription.status === 'active' || subscription.status === 'trialing') {
        billingStatus = subscription.status === 'trialing' ? 'trial' : 'active';
        // Stripe uses snake_case properties
        const currentPeriodEnd = (subscription as any).current_period_end;
        if (currentPeriodEnd) {
          nextBillingDate = new Date(currentPeriodEnd * 1000);
        }
        const unitAmount = (subscription.items.data[0]?.price as any)?.unit_amount;
        amount = (unitAmount || 0) / 100; // Convert from cents
      } else if (
        subscription.status === 'canceled' ||
        subscription.status === 'unpaid' ||
        subscription.status === 'past_due'
      ) {
        billingStatus = 'expired';
      }

      // Get plan from subscription
      const priceMetadata = (subscription.items.data[0]?.price as any)?.metadata;
      if (priceMetadata?.planId) {
        currentPlan = priceMetadata.planId;
      }
    }

    return {
      currentPlan,
      billingStatus,
      nextBillingDate,
      amount,
      currency,
    };
  }

  /**
   * Get billing usage for an organization
   */
  async getBillingUsage(
    organizationId: string,
    userId: string,
  ): Promise<BillingUsageDto> {
    await this.checkBillingPermission(organizationId, userId);

    // Mock usage data - in production, calculate from actual usage
    // This should aggregate across all user's organizations if needed
    const mockUsage: BillingUsageDto = {
      pipelinesUsed: 5,
      pipelinesLimit: 10,
      dataSourcesUsed: 3,
      dataSourcesLimit: 5,
      migrationsRun: 150,
    };

    return mockUsage;
  }

  /**
   * Get billing invoices for an organization
   */
  async getBillingInvoices(
    organizationId: string,
    userId: string,
  ): Promise<BillingInvoiceDto[]> {
    await this.checkBillingPermission(organizationId, userId);

    // Get billing subscription
    const billing = await this.billingRepository.findByOrganizationId(organizationId);

    if (!billing || !billing.stripeCustomerId) {
      return [];
    }

    // Fetch invoices from Stripe
    const invoices = await this.stripe.invoices.list({
      customer: billing.stripeCustomerId,
      limit: 10,
    });

    return invoices.data.map((invoice) => {
      const invoiceAny = invoice as any;
      return {
        invoiceId: invoice.id,
        date: new Date(invoice.created * 1000),
        amount: (invoiceAny.amount_paid || 0) / 100, // Convert from cents
        status: invoiceAny.paid ? 'paid' : 'pending',
        downloadUrl: invoiceAny.hosted_invoice_url || invoiceAny.invoice_pdf || '',
      };
    });
  }

  /**
   * Create Stripe Customer Portal session
   */
  async createPortalSession(organizationId: string, userId: string, returnUrl: string): Promise<string> {
    await this.checkBillingPermission(organizationId, userId);

    const stripeCustomerId = await this.getOrCreateStripeCustomer(organizationId);

    const session = await this.stripe.billingPortal.sessions.create({
      customer: stripeCustomerId,
      return_url: returnUrl,
    });

    const sessionUrl = (session as any).url;
    if (!sessionUrl) {
      throw new Error('Failed to create portal session URL');
    }

    return sessionUrl;
  }

  /**
   * Create Stripe Checkout session
   */
  async createCheckoutSession(
    organizationId: string,
    userId: string,
    planId: string,
    successUrl: string,
    cancelUrl: string,
  ): Promise<string> {
    await this.checkBillingPermission(organizationId, userId);

    const stripeCustomerId = await this.getOrCreateStripeCustomer(organizationId);

    // Get price ID from environment or config
    // For now, using a placeholder - you should configure this based on your Stripe products
    const priceId = this.configService.get<string>(`STRIPE_PRICE_ID_${planId.toUpperCase()}`);
    if (!priceId) {
      throw new BadRequestException(`Price ID not configured for plan: ${planId}`);
    }

    const session = await this.stripe.checkout.sessions.create({
      customer: stripeCustomerId,
      mode: 'subscription',
      line_items: [
        {
          price: priceId,
          quantity: 1,
        },
      ],
      success_url: successUrl,
      cancel_url: cancelUrl,
      metadata: {
        organizationId,
        planId,
      },
    });

    const sessionUrl = (session as any).url;
    if (!sessionUrl) {
      throw new Error('Failed to create checkout session URL');
    }

    return sessionUrl;
  }

  /**
   * Verify Stripe webhook signature
   */
  async verifyWebhookSignature(
    rawBody: Buffer | string | object,
    signature: string,
    webhookSecret: string,
  ): Promise<Stripe.Event> {
    // Convert object to string if needed (for development/testing)
    const bodyString = typeof rawBody === 'object' && !Buffer.isBuffer(rawBody)
      ? JSON.stringify(rawBody)
      : rawBody;
    
    return this.stripe.webhooks.constructEvent(
      bodyString as string | Buffer,
      signature,
      webhookSecret,
    );
  }

  /**
   * Handle Stripe webhook events
   */
  async handleWebhookEvent(event: Stripe.Event): Promise<void> {
    switch (event.type) {
      case 'customer.subscription.created':
      case 'customer.subscription.updated': {
        const subscription = event.data.object as Stripe.Subscription;
        await this.handleSubscriptionUpdate(subscription);
        break;
      }

      case 'customer.subscription.deleted': {
        const subscription = event.data.object as Stripe.Subscription;
        await this.handleSubscriptionDeleted(subscription);
        break;
      }

      case 'invoice.payment_succeeded':
      case 'invoice.payment_failed': {
        const invoice = event.data.object as Stripe.Invoice;
        await this.handleInvoiceEvent(invoice, event.type);
        break;
      }

      default:
        console.log(`Unhandled webhook event type: ${event.type}`);
    }
  }

  /**
   * Handle subscription created/updated
   */
  private async handleSubscriptionUpdate(subscription: Stripe.Subscription): Promise<void> {
    const billing = await this.billingRepository.findByStripeCustomerId(
      subscription.customer as string,
    );

    if (!billing) {
      console.error(`Billing record not found for customer: ${subscription.customer}`);
      return;
    }

    // Get plan ID from price metadata
    const planId =
      subscription.items.data[0]?.price.metadata?.planId ||
      subscription.items.data[0]?.price.nickname ||
      'unknown';

    // Map Stripe status to our billing status
    const billingStatus = subscription.status as
      | 'active'
      | 'trialing'
      | 'past_due'
      | 'canceled'
      | 'unpaid'
      | 'incomplete'
      | 'incomplete_expired'
      | 'paused';

    await this.billingRepository.update(billing.id, {
      stripeSubscriptionId: subscription.id,
      planId,
      billingStatus,
    });
  }

  /**
   * Handle subscription deleted
   */
  private async handleSubscriptionDeleted(subscription: Stripe.Subscription): Promise<void> {
    const billing = await this.billingRepository.findByStripeCustomerId(
      subscription.customer as string,
    );

    if (!billing) {
      console.error(`Billing record not found for customer: ${subscription.customer}`);
      return;
    }

    await this.billingRepository.update(billing.id, {
      stripeSubscriptionId: null,
      billingStatus: 'canceled',
    });
  }

  /**
   * Handle invoice events
   */
  private async handleInvoiceEvent(
    invoice: Stripe.Invoice,
    eventType: string,
  ): Promise<void> {
    if (!invoice.customer) {
      return;
    }

    const billing = await this.billingRepository.findByStripeCustomerId(
      invoice.customer as string,
    );

    if (!billing) {
      console.error(`Billing record not found for customer: ${invoice.customer}`);
      return;
    }

    // Update billing status based on payment result
    if (eventType === 'invoice.payment_failed' && billing.billingStatus === 'active') {
      await this.billingRepository.update(billing.id, {
        billingStatus: 'past_due',
      });
    }
  }
}
