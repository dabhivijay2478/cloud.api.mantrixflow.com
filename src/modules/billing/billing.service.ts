/**
 * Billing Service
 * Provider-agnostic billing service
 * Routes to appropriate provider (Razorpay, Stripe future) based on config
 */

import {
  BadRequestException,
  ForbiddenException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { RazorpayBillingProvider } from './providers/razorpay-billing.provider';
import { SubscriptionRepository } from './repositories/subscription.repository';
import { OrganizationOwnerRepository } from '../organizations/repositories/organization-owner.repository';
import { OrganizationMemberRepository } from '../organizations/repositories/organization-member.repository';
import { OrganizationRepository } from '../organizations/repositories/organization.repository';
import { UserService } from '../users/user.service';
import { billingConfig, getAllPlans, getPlanConfig, getPlanPrice } from '../../config/billing.config';
import type {
  BillingInvoiceDto,
  BillingOverviewDto,
  BillingUsageDto,
} from './dto/billing-response.dto';

@Injectable()
export class BillingService {
  private provider: RazorpayBillingProvider;

  constructor(
    private readonly configService: ConfigService,
    private readonly subscriptionRepository: SubscriptionRepository,
    private readonly organizationRepository: OrganizationRepository,
    private readonly ownerRepository: OrganizationOwnerRepository,
    private readonly memberRepository: OrganizationMemberRepository,
    private readonly userService: UserService,
    razorpayProvider: RazorpayBillingProvider,
  ) {
    // Initialize provider based on config
    const providerName = billingConfig.provider;
    if (providerName === 'razorpay') {
      this.provider = razorpayProvider;
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
   * Get billing overview for an organization
   */
  async getBillingOverview(
    organizationId: string,
    userId: string,
  ): Promise<BillingOverviewDto> {
    await this.checkBillingPermission(organizationId, userId);

    // Get subscription from database
    const subscription = await this.subscriptionRepository.findByOrganizationId(organizationId);

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

    // Get subscription status from provider
    let subscriptionStatus;
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

    return {
      currentPlan: subscriptionStatus.planId,
      billingStatus: this.mapStatusToOverviewStatus(subscriptionStatus.status),
      nextBillingDate: subscriptionStatus.currentPeriodEnd,
      amount: subscriptionStatus.amount,
      currency: subscriptionStatus.currency,
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

    // Get subscription to determine plan limits
    const subscription = await this.subscriptionRepository.findByOrganizationId(organizationId);
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
   * Get billing invoices for an organization
   */
  async getBillingInvoices(
    organizationId: string,
    userId: string,
  ): Promise<BillingInvoiceDto[]> {
    await this.checkBillingPermission(organizationId, userId);

    // Get subscription
    const subscription = await this.subscriptionRepository.findByOrganizationId(organizationId);

    if (!subscription) {
      return [];
    }

    // TODO: Fetch invoices from Razorpay
    // For now, return empty array - invoices will be available in Razorpay dashboard
    // In production, you can fetch from Razorpay API
    return [];
  }

  /**
   * Create checkout session for subscription
   */
  async createCheckoutSession(
    organizationId: string,
    userId: string,
    planId: string,
    interval: 'month' | 'year',
    returnUrl: string,
    cancelUrl: string,
  ): Promise<{ checkoutData: Record<string, unknown>; subscriptionId: string }> {
    await this.checkBillingPermission(organizationId, userId);

    // Validate plan
    if (!['free', 'pro', 'scale'].includes(planId)) {
      throw new BadRequestException(`Invalid plan: ${planId}`);
    }
    const planConfig = getPlanConfig(planId as 'free' | 'pro' | 'scale');

    // Get organization
    const organization = await this.organizationRepository.findById(organizationId);
    if (!organization) {
      throw new NotFoundException(`Organization with ID "${organizationId}" not found`);
    }

    // Get owner email for customer
    const owners = await this.ownerRepository.findByOrganizationId(organizationId);
    let customerEmail = 'customer@example.com'; // Fallback
    let customerName = organization.name;

    if (owners && owners.length > 0) {
      // Get first owner's email
      const owner = owners[0];
      const ownerUser = await this.userService.getUserById(owner.userId);
      if (ownerUser?.email) {
        customerEmail = ownerUser.email;
        customerName = ownerUser.fullName || ownerUser.email.split('@')[0] || organization.name;
      }
    }

    // Create subscription via provider
    const result = await this.provider.createSubscription({
      organizationId,
      planId,
      interval,
      customerEmail,
      customerName,
      returnUrl,
      cancelUrl,
    });

    // Save subscription to database
    await this.subscriptionRepository.create({
      organizationId,
      provider: billingConfig.provider,
      planId,
      providerSubscriptionId: result.subscriptionId,
      status: result.status,
      currency: 'INR',
    });

    // Update organization billing fields
    await this.organizationRepository.update(organizationId, {
      billingProvider: billingConfig.provider,
      billingPlanId: planId,
      billingSubscriptionId: result.subscriptionId,
      billingStatus: result.status,
    });

    // Add Razorpay key ID to checkout data (safe to expose - it's public key)
    const checkoutDataWithKey = {
      ...result.checkoutData,
      keyId: this.configService.get<string>('RAZORPAY_KEY_ID'),
    };

    return {
      checkoutData: checkoutDataWithKey || {},
      subscriptionId: result.subscriptionId,
    };
  }

  /**
   * Cancel subscription
   */
  async cancelSubscription(
    organizationId: string,
    userId: string,
    cancelImmediately: boolean = false,
  ): Promise<void> {
    await this.checkBillingPermission(organizationId, userId);

    const subscription = await this.subscriptionRepository.findByOrganizationId(organizationId);
    if (!subscription) {
      throw new NotFoundException('No active subscription found');
    }

    await this.provider.cancelSubscription({
      subscriptionId: subscription.providerSubscriptionId,
      organizationId,
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
  async handleWebhookEvent(event: unknown, signature: string): Promise<void> {
    // Verify signature
    const payloadString = typeof event === 'string' ? event : JSON.stringify(event);
    const isValid = this.provider.verifyWebhookSignature(payloadString, signature);

    if (!isValid) {
      throw new Error('Invalid webhook signature');
    }

    // Handle event via provider
    await this.provider.handleWebhookEvent(event, signature);

    // Update database based on event
    const eventData = event as any;
    await this.syncSubscriptionFromWebhook(eventData);
  }

  /**
   * Sync subscription from webhook event
   */
  private async syncSubscriptionFromWebhook(event: any): Promise<void> {
    // Extract subscription ID from Razorpay webhook payload
    const subscriptionId =
      event.payload?.subscription?.entity?.id ||
      event.payload?.subscription_id ||
      event.subscription_id;

    if (!subscriptionId) {
      console.warn('No subscription ID found in webhook event');
      return;
    }

    // Get subscription status from provider
    try {
      const status = await this.provider.getSubscription(subscriptionId);

      // Update database
      const dbSubscription = await this.subscriptionRepository.findByProviderSubscriptionId(
        subscriptionId,
      );

      if (dbSubscription) {
        await this.subscriptionRepository.update(dbSubscription.id, {
          status: status.status,
          currentPeriodStart: status.currentPeriodStart,
          currentPeriodEnd: status.currentPeriodEnd,
          cancelAtPeriodEnd: status.cancelAtPeriodEnd,
          amount: status.amount.toString(),
          currency: status.currency,
        });

        // Update organization
        await this.organizationRepository.update(dbSubscription.organizationId, {
          billingStatus: status.status,
          billingCurrentPeriodEnd: status.currentPeriodEnd,
        });
      } else {
        // Subscription not found in DB - might be new, log for investigation
        console.warn(`Subscription ${subscriptionId} not found in database`);
      }
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
