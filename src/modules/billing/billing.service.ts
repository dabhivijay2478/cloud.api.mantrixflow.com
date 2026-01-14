/**
 * Billing Service
 * Provider-agnostic billing service
 * Routes to appropriate provider (Dodo) based on config
 */

import {
  BadRequestException,
  ForbiddenException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { DodoBillingProvider } from './providers/dodo-billing.provider';
import { SubscriptionRepository } from './repositories/subscription.repository';
import { SubscriptionEventRepository } from './repositories/subscription-event.repository';
import { OrganizationOwnerRepository } from '../organizations/repositories/organization-owner.repository';
import { OrganizationMemberRepository } from '../organizations/repositories/organization-member.repository';
import { OrganizationRepository } from '../organizations/repositories/organization.repository';
import { UserService } from '../users/user.service';
import { UserRepository } from '../users/repositories/user.repository';
import { billingConfig, getAllPlans, getPlanConfig, getPlanPrice } from '../../config/billing.config';
import type {
  BillingInvoiceDto,
  BillingOverviewDto,
  BillingUsageDto,
} from './dto/billing-response.dto';

@Injectable()
export class BillingService {
  private provider: DodoBillingProvider;

  constructor(
    private readonly configService: ConfigService,
    private readonly subscriptionRepository: SubscriptionRepository,
    private readonly subscriptionEventRepository: SubscriptionEventRepository,
    private readonly organizationRepository: OrganizationRepository,
    private readonly ownerRepository: OrganizationOwnerRepository,
    private readonly memberRepository: OrganizationMemberRepository,
    private readonly userService: UserService,
    private readonly userRepository: UserRepository,
    dodoProvider: DodoBillingProvider,
  ) {
    // Initialize provider based on config
    const providerName = billingConfig.provider;
    if (providerName === 'dodo') {
      this.provider = dodoProvider;
    } else {
      throw new Error(`Unsupported billing provider: ${providerName}`);
    }
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
   * Get billing overview
   * User-scoped billing (no organizationId needed)
   */
  async getBillingOverview(userId: string): Promise<BillingOverviewDto> {
    // Get user billing data (billing is user-scoped)
    const user = await this.userRepository.findById(userId);
    if (!user) {
      throw new NotFoundException(`User with ID "${userId}" not found`);
    }

    // Get subscription from database (user-scoped)
    const subscription = await this.subscriptionRepository.findByUserId(userId);

    if (!subscription) {
      // No subscription - return free plan
      return {
        currentPlan: 'free',
        billingStatus: 'incomplete',
        nextBillingDate: null,
        amount: 0,
        currency: 'INR',
      };
    }

    // Skip Dodo API call if subscription ID is a checkout/session ID (cks_...)
    // These are not valid subscription IDs and will always return 404
    const isCheckoutSessionId = subscription.providerSubscriptionId?.startsWith('cks_');
    
    // Get subscription status from provider (only if we have a real subscription ID)
    let subscriptionStatus;
    if (!isCheckoutSessionId) {
      try {
        subscriptionStatus = await this.provider.getSubscription(
          subscription.providerSubscriptionId,
        );
      } catch (error) {
        console.error('Error fetching subscription from provider:', error);
        // Return database status as fallback
        return {
          currentPlan: subscription.planId,
          billingStatus: this.mapStatusToOverviewStatus(subscription.status),
          nextBillingDate: subscription.currentPeriodEnd,
          amount: subscription.amount ? Number(subscription.amount) : 0,
          currency: subscription.currency || 'INR',
        };
      }
    } else {
      // If we have a checkout session ID, just return DB status
      // Webhook will update this to real subscription ID later
      return {
        currentPlan: subscription.planId,
        billingStatus: this.mapStatusToOverviewStatus(subscription.status),
        nextBillingDate: subscription.currentPeriodEnd,
        amount: subscription.amount ? Number(subscription.amount) : 0,
        currency: subscription.currency || 'INR',
      };
    }

    return {
      currentPlan: subscriptionStatus.planId,
      billingStatus: this.mapStatusToOverviewStatus(subscriptionStatus.status),
      nextBillingDate: subscriptionStatus.currentPeriodEnd,
      amount: subscriptionStatus.amount,
      currency: subscriptionStatus.currency,
    };
  }

  /**
   * Get billing usage
   * User-scoped billing (no organizationId needed)
   */
  async getBillingUsage(userId: string): Promise<BillingUsageDto> {
    // Get user billing data (billing is user-scoped)
    const user = await this.userRepository.findById(userId);
    if (!user) {
      throw new NotFoundException(`User with ID "${userId}" not found`);
    }

    // Get subscription to determine plan limits (user-scoped)
    const subscription = await this.subscriptionRepository.findByUserId(userId);
    const planId = (subscription?.planId || 'free') as 'free' | 'pro' | 'scale';
    const planConfig = getPlanConfig(planId);

    // TODO: Calculate actual usage from pipelines, data sources, migrations
    // For now, return mock data with plan limits
    return {
      pipelinesUsed: 5, // TODO: Calculate from actual data
      pipelinesLimit: planConfig.limits.pipelines === -1 ? 999999 : planConfig.limits.pipelines,
      dataSourcesUsed: 3, // TODO: Calculate from actual data
      dataSourcesLimit:
        planConfig.limits.dataSources === -1 ? 999999 : planConfig.limits.dataSources,
      migrationsRun: 150, // TODO: Calculate from actual data
    };
  }

  /**
   * Get billing invoices
   * User-scoped billing (no organizationId needed)
   */
  async getBillingInvoices(userId: string): Promise<BillingInvoiceDto[]> {
    // Get user billing data (billing is user-scoped)
    const user = await this.userRepository.findById(userId);
    if (!user) {
      throw new NotFoundException(`User with ID "${userId}" not found`);
    }

    // Get subscription (user-scoped)
    const subscription = await this.subscriptionRepository.findByUserId(userId);

    if (!subscription) {
      return [];
    }

    try {
      // Fetch invoices from Dodo Payments using SDK
      const invoices = await this.provider.getInvoices(subscription.providerSubscriptionId);

      return invoices.map((invoice) => ({
        invoiceId: invoice.invoiceId,
        date: invoice.date,
        amount: invoice.amount,
        currency: invoice.currency,
        status: invoice.status,
        downloadUrl: invoice.downloadUrl,
      }));
    } catch (error) {
      console.error('Error fetching invoices from Dodo Payments:', error);
      // Return empty array if there's an error
      return [];
    }
  }

  /**
   * Get invoice download URL
   * User-scoped billing (no organizationId needed)
   */
  async getInvoiceDownloadUrl(userId: string, invoiceId: string): Promise<string> {
    // Get subscription to verify it belongs to the user
    const subscription = await this.subscriptionRepository.findByUserId(userId);

    if (!subscription) {
      throw new NotFoundException('No active subscription found');
    }

    try {
      // Get invoice download URL from Dodo Payments
      const downloadUrl = await this.provider.getInvoiceDownloadUrl(
        subscription.providerSubscriptionId,
        invoiceId,
      );

      return downloadUrl;
    } catch (error) {
      console.error('Error fetching invoice download URL:', error);
      throw new NotFoundException('Invoice not found or could not be retrieved');
    }
  }

  /**
   * Create checkout session for subscription
   * Returns Dodo-hosted checkout URL
   * User-scoped billing (no organizationId needed)
   */
  async createCheckoutSession(
    userId: string,
    planId: string,
    interval: 'month' | 'year',
    returnUrl: string,
    cancelUrl: string,
  ): Promise<{ checkoutUrl: string; subscriptionId: string }> {
    // Validate plan
    if (!['free', 'pro', 'scale'].includes(planId)) {
      throw new BadRequestException(`Invalid plan: ${planId}`);
    }
    const planConfig = getPlanConfig(planId as 'free' | 'pro' | 'scale');

    // Free plan downgrade: no payment or checkout required
    if (planId === 'free') {
      // Ensure user exists
      const user = await this.userRepository.findById(userId);
      if (!user) {
        throw new NotFoundException(`User with ID "${userId}" not found`);
      }

      // If there is an existing paid subscription, cancel it at provider and
      // remove subscription data from our database
      const existingSubscription = await this.subscriptionRepository.findByUserId(userId);

      if (existingSubscription) {
        try {
          // Best-effort cancel on provider side (in case it's still active)
          if (existingSubscription.providerSubscriptionId) {
            await this.provider.cancelSubscription({
              subscriptionId: existingSubscription.providerSubscriptionId,
              organizationId: undefined,
              cancelImmediately: true,
            });
          }
        } catch (error) {
          // Do not block downgrade on provider errors; just log them
          console.error('Error cancelling subscription while downgrading to free:', error);
        }

        // Remove subscription row so the user is fully "free" in our DB
        await this.subscriptionRepository.delete(existingSubscription.id);
      }

      // Reset user billing fields to reflect free plan (no active subscription)
      await this.userRepository.update(userId, {
        billingProvider: null,
        billingCustomerId: null,
        billingSubscriptionId: null,
        billingPlanId: 'free',
        billingStatus: 'incomplete',
        billingCurrentPeriodEnd: null,
      });

      // Return a successful result without any checkout URL
      return {
        checkoutUrl: '',
        subscriptionId: 'free',
      };
    }

    // Get user billing data (billing is user-scoped)
    const user = await this.userRepository.findById(userId);
    if (!user) {
      throw new NotFoundException(`User with ID "${userId}" not found`);
    }

    // Determine customer email/name for Dodo customer from user
    const customerEmail = user.email || 'customer@example.com';
    const customerName = user.fullName || user.email?.split('@')[0] || 'Customer';

    // Process return URL (remove any placeholders)
    const processedReturnUrl = returnUrl.replace('{organizationId}', '').replace('//', '/');
    // Dodo Payments uses single return_url (not separate cancel_url)
    // Cancel URL is handled by Dodo's checkout page

    // Create checkout session via provider (returns Dodo-hosted checkout URL)
    // Pass existing customer ID from user table if available
    const result = await this.provider.createSubscription({
      organizationId: undefined, // No organization context needed
      planId,
      interval,
      customerEmail,
      customerName,
      customerId: user.billingCustomerId || undefined, // Use user's billing customer ID
      returnUrl: processedReturnUrl,
      cancelUrl: processedReturnUrl, // Dodo uses return_url for both success and cancel
    });

    // Extract customer ID from Dodo response (if returned)
    // Dodo may return customer_id in the checkout session response
    const dodoCustomerId = result.customerId;

    // Save subscription to database (pending status until payment completes)
    // IMPORTANT: Dodo returns a checkout/session/payment ID here (e.g. cks_...),
    // NOT the final subscription ID (sub_...). The real subscription ID will be
    // attached later via webhook in syncSubscriptionFromWebhook.
    await this.subscriptionRepository.create({
      userId: userId, // User who owns the subscription
      organizationId: null, // No organization reference needed
      provider: billingConfig.provider,
      planId,
      providerSubscriptionId: result.subscriptionId,
      status: result.status,
      currency: 'INR',
    });

    // Update user billing fields (billing is user-scoped)
    const userUpdateData: any = {
      billingProvider: billingConfig.provider,
      billingPlanId: planId,
      // Do NOT set billingSubscriptionId here, because we only have a checkout/session ID.
      // The real subscription ID (sub_...) will be saved from webhook via syncSubscriptionFromWebhook.
      billingStatus: result.status,
    };

    // Update customer ID if returned from Dodo (important for portal access)
    if (dodoCustomerId) {
      userUpdateData.billingCustomerId = dodoCustomerId;
    }

    await this.userRepository.update(userId, userUpdateData);

    return {
      checkoutUrl: result.checkoutUrl || '',
      subscriptionId: result.subscriptionId,
    };
  }

  /**
   * Get customer portal URL
   * Redirects to Dodo-hosted billing portal
   * User-scoped billing (no organizationId needed)
   */
  async getCustomerPortalUrl(userId: string): Promise<string> {
    // Get user billing data (billing is user-scoped)
    const user = await this.userRepository.findById(userId);
    if (!user) {
      throw new NotFoundException(`User with ID "${userId}" not found`);
    }

    if (!user.billingCustomerId) {
      throw new BadRequestException('No active subscription found. Please subscribe to a plan first.');
    }

    // Get return URL for after portal session (use workspace billing page)
    const returnUrl = `${process.env.FRONTEND_URL || 'http://localhost:3000'}/workspace/billing`;

    // Create portal session using Dodo Payments SDK
    // Use user's billing customer ID (billing is user-scoped)
    const portalUrl = await this.provider.getCustomerPortalUrl(
      user.billingCustomerId,
      returnUrl,
    );

    return portalUrl;
  }

  /**
   * Cancel subscription
   * User-scoped billing (no organizationId needed)
   */
  async cancelSubscription(
    userId: string,
    cancelImmediately: boolean = false,
  ): Promise<void> {
    // Get subscription by user ID (billing is user-scoped)
    const subscription = await this.subscriptionRepository.findByUserId(userId);
    if (!subscription) {
      throw new NotFoundException('No active subscription found');
    }

    await this.provider.cancelSubscription({
      subscriptionId: subscription.providerSubscriptionId,
      organizationId: undefined, // No organization context needed
      cancelImmediately,
    });

    // Update subscription status
    await this.subscriptionRepository.update(subscription.id, {
      status: 'canceled',
      cancelAtPeriodEnd: !cancelImmediately,
    });
  }

  /**
   * Handle webhook event
   */
  async handleWebhookEvent(
    event: unknown, 
    signature: string,
    webhookId?: string,
    webhookTimestamp?: string
  ): Promise<void> {
    // Verify signature using the raw payload string
    const payloadString = typeof event === 'string' ? event : JSON.stringify(event);
    const isValid = this.provider.verifyWebhookSignature(
      payloadString, 
      signature,
      webhookId,
      webhookTimestamp
    );

    if (!isValid) {
      throw new Error('Invalid webhook signature');
    }

    // Parse event into object for processing
    const eventData = typeof event === 'string' ? JSON.parse(event) : (event as any);

    // Extract user ID from subscription metadata or find from subscription
    let eventUserId: string | undefined;
    const subscriptionId =
      eventData.data?.subscription?.id ||
      eventData.subscription_id ||
      eventData.subscription?.id;

    if (subscriptionId) {
      const dbSubscription = await this.subscriptionRepository.findByProviderSubscriptionId(
        subscriptionId,
      );
      if (dbSubscription?.userId) {
        eventUserId = dbSubscription.userId;
      }
    }

    // Store raw webhook event for audit log
    await this.subscriptionEventRepository.create({
      userId: eventUserId,
      provider: billingConfig.provider,
      eventType: eventData.event_type || eventData.type || 'unknown',
      payload: eventData,
      organizationId: eventData.data?.subscription?.metadata?.organization_id,
    });

    // Handle event via provider (if needed for provider-specific logic)
    // Note: Signature verification already done above
    await this.provider.handleWebhookEvent(event, signature);

    // Update database based on event
    await this.syncSubscriptionFromWebhook(eventData);
  }

  /**
   * Sync subscription from webhook event
   */
  private async syncSubscriptionFromWebhook(event: any): Promise<void> {
    console.log('🔄 Syncing subscription from webhook event');
    console.log('Event type:', event.type || event.event_type);
    console.log('Event data keys:', event.data ? Object.keys(event.data) : 'no data');

    // Extract subscription ID from Dodo webhook payload
    // Dodo sends it as data.subscription.subscription_id (not data.subscription.id)
    const subscriptionId =
      event.data?.subscription?.subscription_id || // Preferred: sub_... from webhook payload
      event.data?.subscription?.id ||
      event.subscription_id ||
      event.subscription?.id ||
      event.data?.subscription_id; // Also check top-level data

    console.log('Extracted subscription ID:', subscriptionId);

    if (!subscriptionId) {
      console.warn('⚠️  No subscription ID found in webhook event. Event structure:', JSON.stringify(event, null, 2));
      return;
    }

    // Get subscription status from provider (this uses the real subscription ID: sub_...)
    try {
      const status = await this.provider.getSubscription(subscriptionId);

      // Try to find existing subscription by this provider subscription ID
      let dbSubscription = await this.subscriptionRepository.findByProviderSubscriptionId(
        subscriptionId,
      );

      // Determine user ID associated with this subscription
      let userId: string | null = dbSubscription?.userId ?? null;

      // If no userId on existing subscription, try to resolve user from Dodo data
      if (!userId) {
        // 1) Try mapping by billingCustomerId (status.customerId)
        if (status.customerId) {
          const userByCustomer = await this.userRepository.findByBillingCustomerId(
            status.customerId,
          );
          if (userByCustomer) {
            userId = userByCustomer.id;
          }
        }

        // 2) Fallback: try mapping by customer email from webhook payload
        if (!userId && event.data?.customer?.email) {
          const userByEmail = await this.userRepository.findByEmail(
            event.data.customer.email as string,
          );
          if (userByEmail) {
            userId = userByEmail.id;
          }
        }
      }

      if (!userId) {
        console.warn(
          `Could not resolve user for subscription ${subscriptionId} (customerId=${status.customerId}, email=${event.data?.customer?.email})`,
        );
        return;
      }

      // If we didn't find a subscription row by sub_..., try to reuse any pending row for this user
      if (!dbSubscription) {
        const pendingForUser = await this.subscriptionRepository.findByUserId(userId);
        if (pendingForUser) {
          dbSubscription = pendingForUser;
        }
      }

      if (dbSubscription) {
        // Update existing row: also normalize providerSubscriptionId to the real sub_... ID
        await this.subscriptionRepository.update(dbSubscription.id, {
          userId,
          providerSubscriptionId: subscriptionId, // overwrite any previous cks_... value
          status: status.status,
          currentPeriodStart: status.currentPeriodStart,
          currentPeriodEnd: status.currentPeriodEnd,
          cancelAtPeriodEnd: status.cancelAtPeriodEnd,
          amount: status.amount.toString(),
          currency: status.currency,
        });
      } else {
        // No existing row at all – create a fresh subscription row for this user
        await this.subscriptionRepository.create({
          userId,
          organizationId: null,
          provider: billingConfig.provider,
          planId: status.planId,
          providerSubscriptionId: subscriptionId,
          status: status.status,
          currentPeriodStart: status.currentPeriodStart,
          currentPeriodEnd: status.currentPeriodEnd,
          cancelAtPeriodEnd: status.cancelAtPeriodEnd,
          amount: status.amount.toString(),
          currency: status.currency,
        });
      }

      // Update user billing fields (billing is user-scoped)
      const userUpdateData: any = {
        billingStatus: status.status,
        billingCurrentPeriodEnd: status.currentPeriodEnd,
        billingSubscriptionId: status.subscriptionId, // sub_...
        billingPlanId: status.planId,
      };

      // Update customer ID if returned from provider (important for portal access)
      if (status.customerId) {
        userUpdateData.billingCustomerId = status.customerId;
      }

      await this.userRepository.update(userId, userUpdateData);
      
      console.log('✅ Updated user billing fields:', {
        userId,
        billingCustomerId: userUpdateData.billingCustomerId,
        billingSubscriptionId: userUpdateData.billingSubscriptionId,
        billingStatus: userUpdateData.billingStatus,
        billingPlanId: userUpdateData.billingPlanId,
      });
    } catch (error) {
      console.error('Error syncing subscription from webhook:', error);
    }
  }

  /**
   * Map subscription status to overview status
   */
  private mapStatusToOverviewStatus(
    status: string,
  ): 'active' | 'trial' | 'expired' | 'incomplete' {
    const statusMap: Record<string, 'active' | 'trial' | 'expired' | 'incomplete'> = {
      active: 'active',
      trialing: 'trial',
      past_due: 'expired',
      canceled: 'expired',
      unpaid: 'expired',
      incomplete: 'incomplete',
    };

    return statusMap[status] || 'incomplete';
  }

  /**
   * Get all available plans
   */
  getAvailablePlans() {
    return getAllPlans();
  }
}
