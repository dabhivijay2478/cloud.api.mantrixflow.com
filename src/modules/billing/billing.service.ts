import { Injectable, Logger, NotFoundException, BadRequestException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

import DodoPayments from 'dodopayments';
import { SubscriptionRepository } from './repositories/subscription.repository';
import { SubscriptionEventRepository } from './repositories/subscription-event.repository';
import { DodoCustomerRepository } from './repositories/dodo-customer.repository';
import { OrganizationMemberRepository } from '../organizations/repositories/organization-member.repository';
import { OrganizationOwnerRepository } from '../organizations/repositories/organization-owner.repository';
import type { CreateCheckoutDto } from './dto/create-checkout.dto';
import type { ChangePlanDto } from './dto/change-plan.dto';
import type { ManageSeatsDto } from './dto/manage-seats.dto';
import type { OnDemandChargeDto } from './dto/on-demand-charge.dto';
import { SubscriptionPlan, SubscriptionStatus } from './entities/subscription.entity';

@Injectable()
export class BillingService {
  private readonly logger = new Logger(BillingService.name);

  private readonly dodoClient: any;

  constructor(
    private readonly configService: ConfigService,
    private readonly subscriptionRepository: SubscriptionRepository,
    private readonly subscriptionEventRepository: SubscriptionEventRepository,
    private readonly dodoCustomerRepository: DodoCustomerRepository,
    private readonly organizationMemberRepository: OrganizationMemberRepository,
    private readonly organizationOwnerRepository: OrganizationOwnerRepository,
  ) {
    const apiKey = this.configService.get<string>('DODO_PAYMENTS_API_KEY');
    const environment = this.configService.get<string>('DODO_PAYMENTS_ENVIRONMENT', 'test_mode');

    if (!apiKey) {
      this.logger.warn('DODO_PAYMENTS_API_KEY not set. Billing features will not work.');
    }

    if (!apiKey) {
      this.logger.warn('DODO_PAYMENTS_API_KEY not set. Billing features will not work.');
    }

    this.dodoClient = new DodoPayments({
      bearerToken: apiKey || '',
      environment: environment as 'test_mode' | 'live_mode',
    });
  }

  /**
   * Create a checkout session for subscription
   */
  async createCheckoutSession(
    userId: string,
    userEmail: string,
    userName: string,
    dto: CreateCheckoutDto,
  ): Promise<{ checkoutUrl: string; sessionId: string }> {
    const productId = this.getProductIdForPlan(dto.planId);
    if (!productId) {
      throw new Error(`Product ID not found for plan: ${dto.planId}`);
    }

    // Check if user already has a subscription (for upgrades)
    const existingSubscription = await this.subscriptionRepository.findByUserId(userId);
    const isUpgrade = existingSubscription && existingSubscription.planId !== dto.planId;

    // Calculate seat add-ons if seat count is provided
    const seatConfig = this.getSeatConfig(dto.planId);
    const addons: Array<{ addon_id: string; quantity: number }> = [];
    
    if (dto.seatCount && dto.seatCount > seatConfig.includedSeats && seatConfig.addonId) {
      const extraSeats = dto.seatCount - seatConfig.includedSeats;
      addons.push({
        addon_id: seatConfig.addonId,
        quantity: extraSeats,
      });
      this.logger.log(`Adding ${extraSeats} extra seats to checkout`);
    }

    // Build metadata object (all values must be strings for Dodo Payments)
    const metadata: Record<string, string> = {
      userId,
      planId: dto.planId,
      isUpgrade: isUpgrade ? 'true' : 'false',
    };
    
    // Add existing subscription ID if it exists
    if (existingSubscription?.dodoSubscriptionId) {
      metadata.existingSubscriptionId = existingSubscription.dodoSubscriptionId;
    }

    // Create checkout session with Dodo Payments for subscription
    // According to Dodo Payments docs, checkout sessions return checkout_url
    const session = await this.dodoClient.checkoutSessions.create({
      product_cart: [
        {
          product_id: productId,
          quantity: 1,
          ...(addons.length > 0 && { addons }),
        },
      ],
      customer: {
        email: userEmail,
        name: userName,
      },
      return_url: dto.returnUrl,
      metadata,
    });

    // According to SDK types (CheckoutSessionResponse):
    // - session_id: string (required)
    // - checkout_url?: string | null (optional, null if payment_method_id provided)

    // Log response for debugging
    this.logger.log('Dodo Payments response type:', typeof session);
    this.logger.log('Dodo Payments response keys:', Object.keys(session || {}));
    this.logger.debug('Dodo Payments full response:', JSON.stringify(session, null, 2));

    // Extract session_id (required field)
    const sessionId = (session as any).session_id;
    if (!sessionId) {
      this.logger.error('No session_id in response:', JSON.stringify(session, null, 2));
      throw new Error('Failed to get session_id from Dodo Payments response');
    }

    this.logger.log(`Checkout session created for user ${userId}: ${sessionId}`);

    // Extract checkout_url (optional field, can be null)
    const checkoutUrl = (session as any).checkout_url;

    if (!checkoutUrl || checkoutUrl === null || checkoutUrl === undefined) {
      this.logger.error('checkout_url is missing, null, or undefined');
      this.logger.error('Full response:', JSON.stringify(session, null, 2));
      this.logger.error('Available keys:', Object.keys(session || {}));
      this.logger.warn('checkout_url can be null if payment_method_id is provided (we are not)');
      this.logger.warn(
        'Possible causes: product not configured, not a subscription product, account issue',
      );

      throw new Error(
        'Checkout URL is not available. Possible causes: ' +
          '1) Product is not properly configured as a subscription in Dodo Payments dashboard, ' +
          '2) Product ID does not exist or is incorrect, ' +
          '3) Account/API key configuration issue. ' +
          'Please verify your product configuration in the Dodo Payments dashboard.',
      );
    }

    this.logger.log(`Checkout URL: ${checkoutUrl}`);

    return {
      checkoutUrl: checkoutUrl as string,
      sessionId: sessionId as string,
    };
  }

  /**
   * Get current subscription for user
   * Returns null if no subscription exists (user hasn't subscribed yet)
   */
  async getSubscription(userId: string) {
    const subscription = await this.subscriptionRepository.findByUserId(userId);
    return subscription || null;
  }

  /**
   * Get customer portal URL for user
   * Returns a magic link that allows customer to access their portal
   */
  async getCustomerPortalUrl(userId: string, _userEmail: string): Promise<{ portalUrl: string }> {
    const subscription = await this.subscriptionRepository.findByUserId(userId);

    if (!subscription) {
      throw new NotFoundException('Subscription not found');
    }

    if (!subscription.dodoCustomerId) {
      throw new NotFoundException('Customer ID not found. Please complete a payment first.');
    }

    try {
      // Use Dodo Payments API to generate customer portal magic link
      // API: POST /customers/{customer_id}/customer-portal/session
      let portalUrl: string;

      try {
        // Use Dodo Payments SDK to create customer portal session
        // This generates a secure magic link for the customer
        const portalSession = await this.dodoClient.customers.customerPortal.create(
          subscription.dodoCustomerId,
          {
            return_url: `${this.configService.get<string>('FRONTEND_URL') || 'http://localhost:3000'}/workspace/billing`,
          },
        );

        // Extract the magic link from the response
        portalUrl = portalSession?.link || portalSession?.url || portalSession?.portal_url;

        if (!portalUrl) {
          this.logger.warn(
            'Portal session created but no link in response:',
            JSON.stringify(portalSession, null, 2),
          );
          throw new Error('No portal link in API response');
        }
      } catch (sdkError: any) {
        // If SDK method fails or doesn't exist, log and throw
        this.logger.error('Failed to create customer portal session via SDK:', sdkError);
        this.logger.error('Error details:', {
          message: sdkError?.message,
          stack: sdkError?.stack,
          response: sdkError?.response?.data,
        });
        throw new Error(
          `Failed to generate customer portal link: ${sdkError?.message || 'Unknown error'}`,
        );
      }

      this.logger.log(
        `Generated customer portal URL for user ${userId} (customer: ${subscription.dodoCustomerId})`,
      );

      return { portalUrl };
    } catch (error) {
      this.logger.error('Failed to generate customer portal URL:', error);
      throw error;
    }
  }

  /**
   * Create or update customer record in dodo_customers table
   */
  private async createOrUpdateCustomer(
    userId: string,
    dodoCustomerId: string,
    metadata?: unknown,
  ): Promise<void> {
    try {
      // Check if customer already exists
      let customer = await this.dodoCustomerRepository.findByUserId(userId);

      if (customer) {
        // Update existing customer
        await this.dodoCustomerRepository.update(customer.id, {
          dodoCustomerId,
          metadata: metadata as any,
        });
        this.logger.log(`✅ Updated customer record for user ${userId}`);
      } else {
        // Create new customer
        customer = await this.dodoCustomerRepository.create({
          userId,
          dodoCustomerId,
          metadata: metadata as any,
        });
        this.logger.log(`✅ Created customer record for user ${userId}`);
      }
    } catch (error) {
      this.logger.error(`❌ Failed to create/update customer:`, error);
      // Don't throw - customer creation failure shouldn't break webhook processing
    }
  }

  /**
   * Link subscription to customer record
   */
  private async linkSubscriptionToCustomer(
    subscriptionId: string,
    dodoCustomerId: string,
  ): Promise<void> {
    try {
      const customer = await this.dodoCustomerRepository.findByDodoCustomerId(dodoCustomerId);
      if (customer) {
        await this.dodoCustomerRepository.update(customer.id, {
          subscriptionId,
        });
        this.logger.log(`✅ Linked subscription ${subscriptionId} to customer ${dodoCustomerId}`);
      }
    } catch (error) {
      this.logger.error(`❌ Failed to link subscription to customer:`, error);
      // Don't throw - linking failure shouldn't break webhook processing
    }
  }

  /**
   * Handle webhook event from Dodo Payments
   */
  async handleWebhook(eventType: string, eventId: string, payload: unknown): Promise<void> {
    this.logger.log('========================================');
    this.logger.log(`🔔 Processing webhook: ${eventType}`);
    this.logger.log(`📋 Event ID: ${eventId}`);
    this.logger.log(`📦 Payload: ${JSON.stringify(payload, null, 2)}`);
    this.logger.log('========================================');
    // Also log to console for terminal visibility
    console.log('\n========================================');
    console.log(`🔔 Processing webhook: ${eventType}`);
    console.log(`📋 Event ID: ${eventId}`);
    console.log(`📦 Payload: ${JSON.stringify(payload, null, 2)}`);
    console.log('========================================\n');

    try {
      // Check if event already processed (idempotency)
      const existingEvent = await this.subscriptionEventRepository.findByDodoEventId(eventId);
      if (existingEvent) {
        this.logger.log(`⏭️  Event ${eventId} already processed, skipping`);
        return;
      }

      // Extract data from payload - Dodo Payments wraps data in a 'data' field
      let eventData = payload;
      if (payload && typeof payload === 'object' && 'data' in payload) {
        eventData = (payload as { data: unknown }).data;
        this.logger.log('📦 Extracted data from payload.data');
      }

      // Extract customer ID and userId early for customer record creation
      const data = eventData as any;
      const customerId = data?.customer_id || (data?.customer as any)?.customer_id;
      const userId = data?.metadata?.userId;

      // Create/update customer record if we have customer ID and userId
      if (customerId && userId) {
        await this.createOrUpdateCustomer(userId, customerId, data?.customer || data);
      }

      // Process based on event type
      switch (eventType) {
        case 'payment.succeeded':
          this.logger.log('💳 Processing payment.succeeded event');
          await this.handlePaymentSucceeded(eventData);
          break;
        case 'subscription.active':
        case 'subscription.activated':
          this.logger.log('✅ Processing subscription.active/activated event');
          await this.handleSubscriptionActivated(eventData);
          break;
        case 'subscription.on_hold':
          this.logger.log('⏸️  Processing subscription.on_hold event');
          await this.handleSubscriptionOnHold(eventData);
          break;
        case 'subscription.renewed':
          this.logger.log('🔄 Processing subscription.renewed event');
          await this.handleSubscriptionRenewed(eventData);
          break;
        case 'subscription.updated':
          this.logger.log('📝 Processing subscription.updated event');
          await this.handleSubscriptionUpdated(eventData);
          break;
        case 'subscription.canceled':
          this.logger.log('❌ Processing subscription.canceled event');
          await this.handleSubscriptionCanceled(eventData);
          break;
        case 'subscription.failed':
          this.logger.log('⚠️  Processing subscription.failed event');
          await this.handleSubscriptionFailed(eventData);
          break;
        default:
          this.logger.warn(`⚠️  Unhandled event type: ${eventType}`);
      }

      // Store event - try to find subscription by subscription_id in data
      // Note: data was already extracted above, but we'll use it again here for clarity
      const subscriptionId = data?.subscription_id;

      this.logger.log(`🔍 Looking for subscription with ID: ${subscriptionId || 'NOT FOUND'}`);
      console.log(`🔍 Looking for subscription with ID: ${subscriptionId || 'NOT FOUND'}`);

      if (subscriptionId) {
        let subscription =
          await this.subscriptionRepository.findByDodoSubscriptionId(subscriptionId);

        // Customer ID was already extracted above, but ensure we have it
        // Check if this is a plan change - if subscription doesn't exist by subscription_id
        // but user already has a subscription, it's a plan change
        if (!subscription && data?.metadata?.userId) {
          // Find existing subscription by userId (for plan changes)
          const existingSubscription = await this.subscriptionRepository.findByUserId(
            data.metadata.userId,
          );

          if (existingSubscription) {
            this.logger.log(
              `🔄 Plan change detected - user has existing subscription, updating with new subscription ID`,
            );
            console.log(
              `🔄 Plan change detected - user has existing subscription, updating with new subscription ID`,
            );

            // Update existing subscription with new subscription ID and plan
            subscription = await this.subscriptionRepository.update(existingSubscription.id, {
              dodoSubscriptionId: subscriptionId, // Update to new subscription ID
              dodoCustomerId: customerId || existingSubscription.dodoCustomerId, // Store customer ID
              planId: (data.metadata.planId as SubscriptionPlan) || existingSubscription.planId,
              status: SubscriptionStatus.ACTIVE,
              currentPeriodStart: data.current_period_start
                ? new Date(data.current_period_start)
                : new Date(),
              currentPeriodEnd: data.current_period_end ? new Date(data.current_period_end) : null,
              trialStart: data.trial_start ? new Date(data.trial_start) : null,
              trialEnd: data.trial_end ? new Date(data.trial_end) : null,
            });
            this.logger.log(
              `✅ Updated subscription with new plan and subscription ID: ${subscription.id}`,
            );
            console.log(
              `✅ Updated subscription with new plan and subscription ID: ${subscription.id}`,
            );
            
            // Link subscription to customer
            if (customerId) {
              await this.linkSubscriptionToCustomer(subscription.id, customerId);
            }
          }
        }

        // If subscription doesn't exist but we have userId in metadata, create it
        if (!subscription && data?.metadata?.userId) {
          this.logger.log(`📝 Creating new subscription for user: ${data.metadata.userId}`);
          console.log(`📝 Creating new subscription for user: ${data.metadata.userId}`);
          try {
            subscription = await this.subscriptionRepository.create({
              userId: data.metadata.userId,
              dodoSubscriptionId: subscriptionId,
              dodoCustomerId: customerId, // Store customer ID
              planId: (data.metadata.planId as SubscriptionPlan) || SubscriptionPlan.FREE,
              status: SubscriptionStatus.ACTIVE,
              currentPeriodStart: data.current_period_start
                ? new Date(data.current_period_start)
                : new Date(),
              currentPeriodEnd: data.current_period_end ? new Date(data.current_period_end) : null,
              trialStart: data.trial_start ? new Date(data.trial_start) : null,
              trialEnd: data.trial_end ? new Date(data.trial_end) : null,
            });
            this.logger.log(`✅ Created subscription: ${subscription.id}`);
            console.log(`✅ Created subscription: ${subscription.id}`);
            
            // Link subscription to customer
            if (customerId) {
              await this.linkSubscriptionToCustomer(subscription.id, customerId);
            }
          } catch (error) {
            this.logger.error(`❌ Failed to create subscription:`, error);
            console.error(`❌ Failed to create subscription:`, error);
            // If duplicate user_id error, try to find and update existing subscription
            if (
              error instanceof Error &&
              error.message.includes('duplicate key') &&
              error.message.includes('user_id')
            ) {
              this.logger.log(`🔄 Duplicate user_id detected, finding existing subscription`);
              subscription = await this.subscriptionRepository.findByUserId(data.metadata.userId);
              if (subscription) {
                // Update existing subscription
                subscription = await this.subscriptionRepository.update(subscription.id, {
                  dodoSubscriptionId: subscriptionId,
                  dodoCustomerId: customerId || subscription.dodoCustomerId,
                  planId: (data.metadata.planId as SubscriptionPlan) || subscription.planId,
                  status: SubscriptionStatus.ACTIVE,
                  currentPeriodStart: data.current_period_start
                    ? new Date(data.current_period_start)
                    : new Date(),
                  currentPeriodEnd: data.current_period_end
                    ? new Date(data.current_period_end)
                    : null,
                });
                this.logger.log(`✅ Updated existing subscription: ${subscription.id}`);
                
                // Link subscription to customer
                if (customerId) {
                  await this.linkSubscriptionToCustomer(subscription.id, customerId);
                }
              }
            } else {
              // Try to find it again in case it was created by another process
              subscription =
                await this.subscriptionRepository.findByDodoSubscriptionId(subscriptionId);
            }
          }
        } else if (subscription && customerId && !subscription.dodoCustomerId) {
          // Update subscription with customer ID if not set
          this.logger.log(`📝 Updating subscription with customer ID: ${subscription.id}`);
          subscription = await this.subscriptionRepository.update(subscription.id, {
            dodoCustomerId: customerId,
          });
          this.logger.log(`✅ Updated subscription with customer ID: ${subscription.id}`);
          
          // Link subscription to customer
          await this.linkSubscriptionToCustomer(subscription.id, customerId);
        }

        if (subscription) {
          // Check if event already exists before storing (idempotency)
          const existingEvent = await this.subscriptionEventRepository.findByDodoEventId(eventId);
          if (existingEvent) {
            this.logger.log(`ℹ️  Event already stored (idempotency check): ${eventId}`);
          } else {
            try {
              await this.subscriptionEventRepository.create({
                subscriptionId: subscription.id,
                eventType: eventType as any,
                dodoEventId: eventId,
                payload: payload as any,
                processed: new Date(),
              });
              this.logger.log(`✅ Stored event for subscription: ${subscription.id}`);
            } catch (error) {
              this.logger.error(`❌ Failed to store event:`, error);
              // If it's a duplicate event ID, that's okay (idempotency)
              if (error instanceof Error && error.message.includes('duplicate')) {
                this.logger.log(`ℹ️  Event already stored (idempotency check)`);
              } else {
                throw error;
              }
            }
          }
        } else {
          this.logger.warn(`⚠️  Could not find or create subscription for ID: ${subscriptionId}`);
          this.logger.warn(`📦 Full data:`, JSON.stringify(data, null, 2));
        }
      } else {
        this.logger.warn(`⚠️  No subscription_id found in payload, cannot store event`);
        this.logger.warn(`📦 Full data:`, JSON.stringify(data, null, 2));
      }

      this.logger.log('✅ Webhook processing completed successfully');
    } catch (error) {
      this.logger.error('❌ Error processing webhook:');
      this.logger.error(error);
      if (error instanceof Error) {
        this.logger.error('Stack:', error.stack);
      }
      throw error;
    }
  }

  /**
   * Determine proration mode based on plan tier comparison
   * Upgrades typically charge immediately, downgrades can be immediate or at period end
   */
  private determineProrationMode(
    currentPlan: SubscriptionPlan | string,
    newPlan: SubscriptionPlan | string,
  ): 'prorated_immediately' | 'difference_immediately' | 'difference_at_period_end' {
    // Plan tier order: FREE < PRO < SCALE < ENTERPRISE
    // Map both enum values and string values to tier numbers
    const getPlanTier = (plan: SubscriptionPlan | string): number => {
      const planStr = String(plan).toLowerCase();
      if (planStr === 'free' || planStr === SubscriptionPlan.FREE) return 1;
      if (planStr === 'pro' || planStr === SubscriptionPlan.PRO) return 2;
      if (planStr === 'scale' || planStr === SubscriptionPlan.SCALE) return 3;
      if (planStr === 'enterprise' || planStr === SubscriptionPlan.ENTERPRISE) return 4;
      return 1; // Default to free tier
    };

    const currentTier = getPlanTier(currentPlan);
    const newTier = getPlanTier(newPlan);

    if (newTier > currentTier) {
      // Upgrade: charge difference immediately
      return 'difference_immediately';
    } else if (newTier < currentTier) {
      // Downgrade: apply difference immediately (can also use 'difference_at_period_end' if preferred)
      return 'difference_immediately';
    } else {
      // Same tier (shouldn't happen, but handle gracefully)
      return 'prorated_immediately';
    }
  }

  /**
   * Change subscription plan - uses Dodo Payments changePlan API directly
   * According to official docs: https://docs.dodopayments.com/features/subscription
   */
  async changePlan(
    userId: string,
    _userEmail: string,
    _userName: string,
    dto: ChangePlanDto,
  ): Promise<{ success: boolean; message: string }> {
    const subscription = await this.subscriptionRepository.findByUserId(userId);
    if (!subscription) {
      throw new NotFoundException('Subscription not found');
    }

    // Check if plan is actually changing
    if (subscription.planId === dto.planId) {
      this.logger.log(`Plan is already ${dto.planId}, no change needed`);
      return {
        success: true,
        message: `You are already on the ${dto.planId} plan.`,
      };
    }

    // Handle FREE plan - no Dodo subscription needed, just update database
    if (dto.planId === SubscriptionPlan.FREE) {
      this.logger.log(`🔄 Changing plan to FREE for user ${userId}`);
      
      // Update local DB
      await this.subscriptionRepository.update(subscription.id, {
        planId: dto.planId,
        updatedAt: new Date(),
        // Keep dodoSubscriptionId and dodoCustomerId for potential future upgrades
      });

      this.logger.log(`✅ Updated local subscription record to FREE: ${subscription.id}`);

      return {
        success: true,
        message: `Plan successfully changed to FREE. Changes will be reflected immediately.`,
      };
    }

    // For paid plans, need Dodo subscription
    if (!subscription.dodoSubscriptionId) {
      throw new NotFoundException('Active subscription ID not found. Please contact support.');
    }

    const productId = this.getProductIdForPlan(dto.planId);
    if (!productId) {
      throw new Error(`Product ID not found for plan: ${dto.planId}`);
    }

    // Determine proration mode based on upgrade/downgrade
    // Cast planId to SubscriptionPlan enum for type safety
    const currentPlan = subscription.planId as SubscriptionPlan;
    const newPlan = dto.planId as SubscriptionPlan;
    const prorationMode = this.determineProrationMode(currentPlan, newPlan);

    this.logger.log(
      `🔄 Changing plan from ${subscription.planId} to ${dto.planId} for user ${userId}`,
    );
    this.logger.log(`📊 Using proration mode: ${prorationMode}`);

    try {
      // Get current seat add-ons from subscription (if any)
      // Note: We'll need to fetch current subscription details from Dodo to get add-ons
      // For now, we'll change the plan and seats can be managed separately
      // In a production system, you'd want to preserve seat add-ons when changing plans
      
      // Use changePlan API directly - no checkout session needed!
      // According to Dodo Payments docs: subscriptions.changePlan()
      await this.dodoClient.subscriptions.changePlan(subscription.dodoSubscriptionId, {
        product_id: productId,
        quantity: 1,
        proration_billing_mode: prorationMode,
        // Note: Add-ons are preserved automatically by Dodo Payments when changing plans
        // If you want to reset add-ons, pass addons: []
      });

      this.logger.log(`✅ Plan change successful via Dodo Payments API`);

      // Update local DB immediately (webhook will also update, but this provides immediate feedback)
      await this.subscriptionRepository.update(subscription.id, {
        planId: dto.planId,
        updatedAt: new Date(),
      });

      this.logger.log(`✅ Updated local subscription record: ${subscription.id}`);

      return {
        success: true,
        message: `Plan successfully changed to ${dto.planId}. Changes will be reflected immediately.`,
      };
    } catch (error) {
      this.logger.error(`❌ Failed to change plan via Dodo Payments API:`, error);
      if (error instanceof Error) {
        throw new Error(`Failed to change plan: ${error.message}`);
      }
      throw new Error('Failed to change plan. Please try again.');
    }
  }

  /**
   * Handle payment succeeded event - creates subscription if it doesn't exist
   */
  private async handlePaymentSucceeded(payload: unknown): Promise<void> {
    this.logger.log('💳 handlePaymentSucceeded called');
    this.logger.log('📦 Payload:', JSON.stringify(payload, null, 2));
    console.log('💳 handlePaymentSucceeded called');
    console.log('📦 Payload:', JSON.stringify(payload, null, 2));

    const data = payload as {
      subscription_id?: string;
      payment_id?: string;
      customer?: { customer_id?: string; email?: string };
      metadata?: {
        userId?: string;
        planId?: string;
        changePlan?: boolean | string;
        existingSubscriptionId?: string;
      };
      current_period_start?: string;
      current_period_end?: string;
      trial_start?: string;
      trial_end?: string;
      status?: string;
    };

    this.logger.log('🔍 Extracted data:');
    this.logger.log('  - subscription_id:', data.subscription_id);
    this.logger.log('  - payment_id:', data.payment_id);
    this.logger.log('  - metadata:', JSON.stringify(data.metadata));
    this.logger.log('  - status:', data.status);
    console.log('🔍 Extracted data:');
    console.log('  - subscription_id:', data.subscription_id);
    console.log('  - payment_id:', data.payment_id);
    console.log('  - metadata:', JSON.stringify(data.metadata));
    console.log('  - status:', data.status);

    if (!data.subscription_id) {
      this.logger.warn('⚠️  No subscription_id in payment.succeeded event');
      console.warn('⚠️  No subscription_id in payment.succeeded event');
      return;
    }

    // Extract customer ID from data
    const customerId = (data.customer as any)?.customer_id;

    // Create/update customer record if we have customer ID and userId
    if (customerId && data.metadata?.userId) {
      await this.createOrUpdateCustomer(
        data.metadata.userId,
        customerId,
        data.customer || data,
      );
    }

    // Check if subscription already exists
    let subscription = await this.subscriptionRepository.findByDodoSubscriptionId(
      data.subscription_id,
    );

    // If not found by subscription_id, check by userId (for plan changes)
    if (!subscription && data.metadata?.userId) {
      subscription = await this.subscriptionRepository.findByUserId(data.metadata.userId);

      if (subscription) {
        // Plan change: update existing subscription with new subscription ID
        this.logger.log(
          `🔄 Plan change detected - updating existing subscription: ${subscription.id}`,
        );
        console.log(`🔄 Plan change detected - updating existing subscription: ${subscription.id}`);
        subscription = await this.subscriptionRepository.update(subscription.id, {
          dodoSubscriptionId: data.subscription_id,
          dodoCustomerId: customerId || subscription.dodoCustomerId,
          planId: (data.metadata?.planId as SubscriptionPlan) || subscription.planId,
          status: SubscriptionStatus.ACTIVE,
          currentPeriodStart: data.current_period_start
            ? new Date(data.current_period_start)
            : new Date(),
          currentPeriodEnd: data.current_period_end ? new Date(data.current_period_end) : null,
          trialStart: data.trial_start ? new Date(data.trial_start) : null,
          trialEnd: data.trial_end ? new Date(data.trial_end) : null,
        });
        this.logger.log(
          `✅ Updated subscription with new plan and subscription ID: ${subscription.id}`,
        );
        console.log(
          `✅ Updated subscription with new plan and subscription ID: ${subscription.id}`,
        );
        
        // Link subscription to customer
        if (customerId) {
          await this.linkSubscriptionToCustomer(subscription.id, customerId);
        }
      }
    }

    if (subscription) {
      this.logger.log(`✅ Subscription already exists: ${subscription.id}`);
      console.log(`✅ Subscription already exists: ${subscription.id}`);

      // Update subscription with customer ID if not set, and update status/periods
      subscription = await this.subscriptionRepository.update(subscription.id, {
        dodoCustomerId: customerId || subscription.dodoCustomerId,
        status: SubscriptionStatus.ACTIVE,
        currentPeriodStart: data.current_period_start
          ? new Date(data.current_period_start)
          : subscription.currentPeriodStart,
        currentPeriodEnd: data.current_period_end
          ? new Date(data.current_period_end)
          : subscription.currentPeriodEnd,
      });
      this.logger.log(`✅ Updated subscription: ${subscription.id}`);
      console.log(`✅ Updated subscription: ${subscription.id}`);
      
      // Link subscription to customer
      if (customerId) {
        await this.linkSubscriptionToCustomer(subscription.id, customerId);
      }
    } else if (data.metadata?.userId) {
      // Create new subscription
      this.logger.log(`📝 Creating new subscription for user: ${data.metadata.userId}`);
      console.log(`📝 Creating new subscription for user: ${data.metadata.userId}`);
      try {
        subscription = await this.subscriptionRepository.create({
          userId: data.metadata.userId,
          dodoSubscriptionId: data.subscription_id,
          dodoCustomerId: customerId, // Store customer ID
          planId: (data.metadata.planId as SubscriptionPlan) || SubscriptionPlan.FREE,
          status: SubscriptionStatus.ACTIVE,
          currentPeriodStart: data.current_period_start
            ? new Date(data.current_period_start)
            : new Date(),
          currentPeriodEnd: data.current_period_end ? new Date(data.current_period_end) : null,
          trialStart: data.trial_start ? new Date(data.trial_start) : null,
          trialEnd: data.trial_end ? new Date(data.trial_end) : null,
        });
        this.logger.log(`✅ Created subscription: ${subscription.id}`);
        console.log(`✅ Created subscription: ${subscription.id}`);
        
        // Link subscription to customer
        if (customerId) {
          await this.linkSubscriptionToCustomer(subscription.id, customerId);
        }
      } catch (error) {
        this.logger.error(`❌ Failed to create subscription:`, error);
        console.error(`❌ Failed to create subscription:`, error);
        // If duplicate user_id error, try to find and update existing subscription
        if (
          error instanceof Error &&
          error.message.includes('duplicate key') &&
          error.message.includes('user_id')
        ) {
          this.logger.log(`🔄 Duplicate user_id detected, finding existing subscription`);
          subscription = await this.subscriptionRepository.findByUserId(data.metadata.userId);
          if (subscription) {
            // Update existing subscription
            subscription = await this.subscriptionRepository.update(subscription.id, {
              dodoSubscriptionId: data.subscription_id,
              dodoCustomerId: customerId || subscription.dodoCustomerId,
              planId: (data.metadata.planId as SubscriptionPlan) || subscription.planId,
              status: SubscriptionStatus.ACTIVE,
              currentPeriodStart: data.current_period_start
                ? new Date(data.current_period_start)
                : new Date(),
              currentPeriodEnd: data.current_period_end ? new Date(data.current_period_end) : null,
            });
            this.logger.log(`✅ Updated existing subscription: ${subscription.id}`);
            
            // Link subscription to customer
            if (customerId) {
              await this.linkSubscriptionToCustomer(subscription.id, customerId);
            }
          }
        } else {
          throw error;
        }
      }
    } else {
      this.logger.warn('⚠️  Cannot create subscription: missing userId in metadata');
      this.logger.warn('📦 Full payload:', JSON.stringify(payload, null, 2));
      console.warn('⚠️  Cannot create subscription: missing userId in metadata');
      console.warn('📦 Full payload:', JSON.stringify(payload, null, 2));
    }
  }

  /**
   * Handle subscription updated event
   */
  private async handleSubscriptionUpdated(payload: unknown): Promise<void> {
    this.logger.log('📝 handleSubscriptionUpdated called');
    const data = payload as {
      subscription_id: string;
      current_period_start?: string;
      current_period_end?: string;
      status?: string;
    };

    const subscription = await this.subscriptionRepository.findByDodoSubscriptionId(
      data.subscription_id,
    );

    if (subscription) {
      const updateData: any = {};
      if (data.current_period_start) {
        updateData.currentPeriodStart = new Date(data.current_period_start);
      }
      if (data.current_period_end) {
        updateData.currentPeriodEnd = new Date(data.current_period_end);
      }
      if (data.status) {
        updateData.status = data.status as SubscriptionStatus;
      }

      await this.subscriptionRepository.update(subscription.id, updateData);
      this.logger.log(`✅ Updated subscription: ${subscription.id}`);
    } else {
      this.logger.warn(`⚠️  Subscription not found: ${data.subscription_id}`);
    }
  }

  /**
   * Handle subscription activated event
   */
  private async handleSubscriptionActivated(payload: unknown): Promise<void> {
    this.logger.log('✅ handleSubscriptionActivated called');
    const data = payload as {
      subscription_id: string;
      customer_id?: string;
      customer?: { customer_id?: string; email?: string };
      product_id?: string;
      status?: string;
      current_period_start?: string;
      current_period_end?: string;
      trial_start?: string;
      trial_end?: string;
      metadata?: {
        userId?: string;
        planId?: string;
        changePlan?: boolean | string;
        existingSubscriptionId?: string;
      };
    };

    this.logger.log('🔍 Subscription activated data:', JSON.stringify(data, null, 2));

    // Extract customer ID from data
    const customerId = data.customer_id || (data.customer as any)?.customer_id;

    // Create/update customer record if we have customer ID and userId
    if (customerId && data.metadata?.userId) {
      await this.createOrUpdateCustomer(
        data.metadata.userId,
        customerId,
        data.customer || data,
      );
    }

    // Find subscription by Dodo subscription ID first
    let subscription = await this.subscriptionRepository.findByDodoSubscriptionId(
      data.subscription_id,
    );

    // If not found by subscription_id, check by userId (for plan changes)
    if (!subscription && data.metadata?.userId) {
      subscription = await this.subscriptionRepository.findByUserId(data.metadata.userId);

      if (subscription) {
        // Plan change: update existing subscription with new subscription ID
        this.logger.log(
          `🔄 Plan change detected - updating existing subscription: ${subscription.id}`,
        );
        subscription = await this.subscriptionRepository.update(subscription.id, {
          dodoSubscriptionId: data.subscription_id,
          dodoCustomerId: customerId,
          planId: (data.metadata.planId as SubscriptionPlan) || subscription.planId,
          status: SubscriptionStatus.ACTIVE,
          currentPeriodStart: data.current_period_start
            ? new Date(data.current_period_start)
            : new Date(),
          currentPeriodEnd: data.current_period_end ? new Date(data.current_period_end) : null,
          trialStart: data.trial_start ? new Date(data.trial_start) : null,
          trialEnd: data.trial_end ? new Date(data.trial_end) : null,
        });
        this.logger.log(`✅ Updated subscription with new plan: ${subscription.id}`);
        
        // Link subscription to customer
        if (customerId) {
          await this.linkSubscriptionToCustomer(subscription.id, customerId);
        }
      }
    }

    // If still no subscription and we have userId, create new one
    if (!subscription && data.metadata?.userId) {
      this.logger.log(`📝 Creating new subscription for user: ${data.metadata.userId}`);
      try {
        subscription = await this.subscriptionRepository.create({
          userId: data.metadata.userId,
          dodoSubscriptionId: data.subscription_id,
          dodoCustomerId: customerId,
          planId: (data.metadata.planId as SubscriptionPlan) || SubscriptionPlan.FREE,
          status: SubscriptionStatus.ACTIVE,
          currentPeriodStart: data.current_period_start
            ? new Date(data.current_period_start)
            : new Date(),
          currentPeriodEnd: data.current_period_end ? new Date(data.current_period_end) : null,
          trialStart: data.trial_start ? new Date(data.trial_start) : null,
          trialEnd: data.trial_end ? new Date(data.trial_end) : null,
        });
        this.logger.log(`✅ Created subscription: ${subscription.id}`);
        
        // Link subscription to customer
        if (customerId) {
          await this.linkSubscriptionToCustomer(subscription.id, customerId);
        }
      } catch (error) {
        this.logger.error(`❌ Failed to create subscription:`, error);
        // If duplicate user_id error, try to find existing subscription
        if (
          error instanceof Error &&
          error.message.includes('duplicate key') &&
          error.message.includes('user_id')
        ) {
          this.logger.log(`🔄 Duplicate user_id detected, finding existing subscription`);
          subscription = await this.subscriptionRepository.findByUserId(data.metadata.userId);
          if (subscription) {
            // Update existing subscription
            subscription = await this.subscriptionRepository.update(subscription.id, {
              dodoSubscriptionId: data.subscription_id,
              dodoCustomerId: customerId,
              planId: (data.metadata.planId as SubscriptionPlan) || subscription.planId,
              status: SubscriptionStatus.ACTIVE,
              currentPeriodStart: data.current_period_start
                ? new Date(data.current_period_start)
                : new Date(),
              currentPeriodEnd: data.current_period_end ? new Date(data.current_period_end) : null,
            });
            this.logger.log(`✅ Updated existing subscription: ${subscription.id}`);
            
            // Link subscription to customer
            if (customerId) {
              await this.linkSubscriptionToCustomer(subscription.id, customerId);
            }
          }
        } else {
          throw error;
        }
      }
    } else if (subscription) {
      // Update existing subscription with customer ID if not set
      if (customerId && !subscription.dodoCustomerId) {
        this.logger.log(`📝 Updating subscription with customer ID: ${subscription.id}`);
        subscription = await this.subscriptionRepository.update(subscription.id, {
          dodoCustomerId: customerId,
          status: SubscriptionStatus.ACTIVE,
          currentPeriodStart: data.current_period_start
            ? new Date(data.current_period_start)
            : subscription.currentPeriodStart,
          currentPeriodEnd: data.current_period_end
            ? new Date(data.current_period_end)
            : subscription.currentPeriodEnd,
        });
        this.logger.log(`✅ Updated subscription: ${subscription.id}`);
        
        // Link subscription to customer
        if (customerId) {
          await this.linkSubscriptionToCustomer(subscription.id, customerId);
        }
      }
    } else {
      this.logger.warn(`⚠️  Cannot create subscription: missing userId in metadata`);
    }
  }

  /**
   * Handle subscription on hold event
   */
  private async handleSubscriptionOnHold(payload: unknown): Promise<void> {
    const data = payload as { subscription_id: string };
    const subscription = await this.subscriptionRepository.findByDodoSubscriptionId(
      data.subscription_id,
    );
    if (subscription) {
      await this.subscriptionRepository.update(subscription.id, {
        status: SubscriptionStatus.ON_HOLD,
      });
    }
  }

  /**
   * Handle subscription renewed event
   */
  private async handleSubscriptionRenewed(payload: unknown): Promise<void> {
    const data = payload as {
      subscription_id: string;
      current_period_start?: string;
      current_period_end?: string;
    };
    const subscription = await this.subscriptionRepository.findByDodoSubscriptionId(
      data.subscription_id,
    );
    if (subscription) {
      await this.subscriptionRepository.update(subscription.id, {
        status: SubscriptionStatus.ACTIVE,
        currentPeriodStart: data.current_period_start
          ? new Date(data.current_period_start)
          : new Date(),
        currentPeriodEnd: data.current_period_end ? new Date(data.current_period_end) : null,
      });
    }
  }

  /**
   * Handle subscription canceled event
   */
  private async handleSubscriptionCanceled(payload: unknown): Promise<void> {
    const data = payload as {
      subscription_id: string;
      canceled_at?: string;
    };
    const subscription = await this.subscriptionRepository.findByDodoSubscriptionId(
      data.subscription_id,
    );
    if (subscription) {
      await this.subscriptionRepository.update(subscription.id, {
        status: SubscriptionStatus.CANCELED,
        canceledAt: data.canceled_at ? new Date(data.canceled_at) : new Date(),
      });
    }
  }

  /**
   * Handle subscription failed event
   */
  private async handleSubscriptionFailed(payload: unknown): Promise<void> {
    const data = payload as { subscription_id: string };
    const subscription = await this.subscriptionRepository.findByDodoSubscriptionId(
      data.subscription_id,
    );
    if (subscription) {
      await this.subscriptionRepository.update(subscription.id, {
        status: SubscriptionStatus.FAILED,
      });
    }
  }

  /**
   * Cancel subscription at period end
   * According to Dodo Payments docs: https://docs.dodopayments.com/features/subscription
   */
  async cancelAtPeriodEnd(userId: string): Promise<{ success: boolean; message: string }> {
    const subscription = await this.subscriptionRepository.findByUserId(userId);
    if (!subscription) {
      throw new NotFoundException('Subscription not found');
    }

    if (!subscription.dodoSubscriptionId) {
      throw new NotFoundException('Active subscription ID not found. Please contact support.');
    }

    this.logger.log(`🛑 Canceling subscription at period end for user ${userId}`);

    try {
      // Use subscriptions.update API to set cancel_at_period_end
      await this.dodoClient.subscriptions.update(subscription.dodoSubscriptionId, {
        cancel_at_period_end: true,
      });

      this.logger.log(`✅ Subscription will be canceled at period end`);

      // Update local DB
      // Store the current period end date as the cancellation date
      const cancelDate = subscription.currentPeriodEnd || new Date();
      await this.subscriptionRepository.update(subscription.id, {
        cancelAtPeriodEnd: cancelDate,
        updatedAt: new Date(),
      });

      return {
        success: true,
        message: 'Subscription will be canceled at the end of the current billing period.',
      };
    } catch (error) {
      this.logger.error(`❌ Failed to cancel subscription:`, error);
      if (error instanceof Error) {
        throw new Error(`Failed to cancel subscription: ${error.message}`);
      }
      throw new Error('Failed to cancel subscription. Please try again.');
    }
  }

  /**
   * Resume subscription (undo cancel)
   */
  async resumeSubscription(userId: string): Promise<{ success: boolean; message: string }> {
    const subscription = await this.subscriptionRepository.findByUserId(userId);
    if (!subscription) {
      throw new NotFoundException('Subscription not found');
    }

    if (!subscription.dodoSubscriptionId) {
      throw new NotFoundException('Active subscription ID not found. Please contact support.');
    }

    this.logger.log(`▶️  Resuming subscription for user ${userId}`);

    try {
      // Use subscriptions.update API to unset cancel_at_period_end
      await this.dodoClient.subscriptions.update(subscription.dodoSubscriptionId, {
        cancel_at_period_end: false,
      });

      this.logger.log(`✅ Subscription resumed`);

      // Update local DB
      // Set to null to indicate cancellation is no longer scheduled
      await this.subscriptionRepository.update(subscription.id, {
        cancelAtPeriodEnd: null,
        updatedAt: new Date(),
      });

      return {
        success: true,
        message: 'Subscription has been resumed and will continue after the current period.',
      };
    } catch (error) {
      this.logger.error(`❌ Failed to resume subscription:`, error);
      if (error instanceof Error) {
        throw new Error(`Failed to resume subscription: ${error.message}`);
      }
      throw new Error('Failed to resume subscription. Please try again.');
    }
  }

  /**
   * Update payment method for subscription
   * According to Dodo Payments docs: https://docs.dodopayments.com/features/subscription
   * Useful for handling failed payments (on_hold status)
   */
  async updatePaymentMethod(
    userId: string,
    returnUrl: string,
  ): Promise<{ url: string; sessionId?: string }> {
    const subscription = await this.subscriptionRepository.findByUserId(userId);
    if (!subscription) {
      throw new NotFoundException('Subscription not found');
    }

    if (!subscription.dodoSubscriptionId) {
      throw new NotFoundException('Active subscription ID not found. Please contact support.');
    }

    this.logger.log(`💳 Updating payment method for subscription: ${subscription.dodoSubscriptionId}`);

    try {
      // Use subscriptions.updatePaymentMethod API
      const response = await this.dodoClient.subscriptions.updatePaymentMethod(
        subscription.dodoSubscriptionId,
        {
          type: 'new',
          return_url: returnUrl,
        },
      );

      this.logger.log(`✅ Payment method update URL generated`);

      // Extract URL from response
      const url = (response as any).url || (response as any).checkout_url;
      const sessionId = (response as any).session_id;

      if (!url) {
        this.logger.error('No URL in payment method update response:', JSON.stringify(response, null, 2));
        throw new Error('Failed to get payment method update URL from Dodo Payments');
      }

      return {
        url: url as string,
        sessionId: sessionId as string | undefined,
      };
    } catch (error) {
      this.logger.error(`❌ Failed to update payment method:`, error);
      if (error instanceof Error) {
        throw new Error(`Failed to update payment method: ${error.message}`);
      }
      throw new Error('Failed to update payment method. Please try again.');
    }
  }

  /**
   * Calculate billable seats for an organization
   * Counts OWNER, ADMIN, EDITOR roles (VIEWER does not count)
   */
  async calculateBillableSeats(organizationId: string): Promise<number> {
    // Count owners (from organization_owners table)
    const owners = await this.organizationOwnerRepository.findByOrganizationId(organizationId);
    const ownerCount = owners.length;

    // Count members with billable roles (ADMIN, EDITOR - not VIEWER)
    const members = await this.organizationMemberRepository.findByOrganizationId(organizationId);
    const billableMembers = members.filter(
      (m) =>
        (m.status === 'active' || m.status === 'accepted') &&
        (m.role === 'ADMIN' || m.role === 'EDITOR'),
    );

    return ownerCount + billableMembers.length;
  }

  /**
   * Get seat configuration for a plan
   */
  getSeatConfig(planId: SubscriptionPlan): {
    includedSeats: number;
    extraSeatPrice: number; // in cents
    addonId: string;
  } {
    const configs: Record<SubscriptionPlan, { includedSeats: number; extraSeatPrice: number; addonId: string }> = {
      [SubscriptionPlan.FREE]: {
        includedSeats: 1,
        extraSeatPrice: 0,
        addonId: '',
      },
      [SubscriptionPlan.PRO]: {
        includedSeats: 3,
        extraSeatPrice: 500, // $5.00 in cents
        addonId: this.configService.get<string>('DODO_ADDON_ID_PRO_SEAT') || 'addon_pro_seat',
      },
      [SubscriptionPlan.SCALE]: {
        includedSeats: 10,
        extraSeatPrice: 400, // $4.00 in cents
        addonId: this.configService.get<string>('DODO_ADDON_ID_SCALE_SEAT') || 'addon_scale_seat',
      },
      [SubscriptionPlan.ENTERPRISE]: {
        includedSeats: 0, // Contract-based
        extraSeatPrice: 300, // $3.00 in cents (typical)
        addonId: this.configService.get<string>('DODO_ADDON_ID_ENTERPRISE_SEAT') || 'addon_enterprise_seat',
      },
    };

    return configs[planId] || configs[SubscriptionPlan.FREE];
  }

  /**
   * Manage seats for a subscription
   * Adds or removes seats based on the desired count
   */
  async manageSeats(
    userId: string,
    organizationId: string,
    dto: ManageSeatsDto,
  ): Promise<{ success: boolean; message: string; newSeatCount: number }> {
    const subscription = await this.subscriptionRepository.findByUserId(userId);
    if (!subscription) {
      throw new NotFoundException('Subscription not found');
    }

    if (!subscription.dodoSubscriptionId) {
      throw new NotFoundException('Active subscription ID not found. Please contact support.');
    }

    // Get current billable seats
    const currentBillableSeats = await this.calculateBillableSeats(organizationId);
    const seatConfig = this.getSeatConfig(subscription.planId as SubscriptionPlan);

    // Validate seat count
    if (dto.seatCount < seatConfig.includedSeats) {
      throw new BadRequestException(
        `Minimum seat count for ${subscription.planId} plan is ${seatConfig.includedSeats}`,
      );
    }

    // Calculate extra seats needed
    const extraSeatsNeeded = Math.max(0, dto.seatCount - seatConfig.includedSeats);

    this.logger.log(
      `🪑 Managing seats for user ${userId}: Current=${currentBillableSeats}, Desired=${dto.seatCount}, Extra=${extraSeatsNeeded}`,
    );

    try {
      // Use changePlan API to update seat add-ons
      const addons = extraSeatsNeeded > 0
        ? [{ addon_id: seatConfig.addonId, quantity: extraSeatsNeeded }]
        : [];

      await this.dodoClient.subscriptions.changePlan(subscription.dodoSubscriptionId, {
        product_id: this.getProductIdForPlan(subscription.planId as SubscriptionPlan),
        quantity: 1,
        proration_billing_mode: 'prorated_immediately',
        addons,
      });

      this.logger.log(`✅ Seats updated successfully`);

      return {
        success: true,
        message: `Seats updated to ${dto.seatCount} (${seatConfig.includedSeats} included + ${extraSeatsNeeded} extra)`,
        newSeatCount: dto.seatCount,
      };
    } catch (error) {
      this.logger.error(`❌ Failed to manage seats:`, error);
      if (error instanceof Error) {
        throw new Error(`Failed to manage seats: ${error.message}`);
      }
      throw new Error('Failed to manage seats. Please try again.');
    }
  }

  /**
   * Get product ID for a plan
   */
  private getProductIdForPlan(planId: SubscriptionPlan): string {
    const productIdMap: Record<SubscriptionPlan, string> = {
      [SubscriptionPlan.FREE]: '',
      [SubscriptionPlan.PRO]: this.configService.get<string>('DODO_PRODUCT_ID_PRO') || 'prod_pro',
      [SubscriptionPlan.SCALE]:
        this.configService.get<string>('DODO_PRODUCT_ID_SCALE') || 'prod_scale',
      [SubscriptionPlan.ENTERPRISE]:
        this.configService.get<string>('DODO_PRODUCT_ID_ENTERPRISE') || 'prod_enterprise',
    };

    return productIdMap[planId] || '';
  }

  /**
   * Create on-demand subscription (mandate) for execution overages
   * This authorizes the payment method for variable charges later
   */
  async createOnDemandSubscription(
    userId: string,
    userEmail: string,
    userName: string,
    returnUrl: string,
  ): Promise<{ checkoutUrl: string; sessionId: string }> {
    // Get or create customer
    const subscription = await this.subscriptionRepository.findByUserId(userId);
    let customerId = subscription?.dodoCustomerId;

    // If no customer ID, we'll create one during checkout
    // For on-demand, we need a product - use a special on-demand product
    const onDemandProductId =
      this.configService.get<string>('DODO_PRODUCT_ID_ON_DEMAND') || 'prod_on_demand';

    this.logger.log(`🔄 Creating on-demand subscription (mandate) for user ${userId}`);

    try {
      // Create checkout session with on-demand subscription_data
      // According to docs: subscription_data.on_demand.mandate_only: true
      // This authorizes a payment method (mandate) for variable charges later
      const session = await this.dodoClient.checkoutSessions.create({
        product_cart: [
          {
            product_id: onDemandProductId,
            quantity: 1,
          },
        ],
        customer: customerId
          ? { customer_id: customerId }
          : {
              email: userEmail,
              name: userName,
            },
        return_url: returnUrl,
        subscription_data: {
          on_demand: {
            mandate_only: true, // Authorize mandate only, no initial charge
            // Optional: set mandate_only: false and provide product_price to charge immediately
            // product_price: 1000, // optional: charge $10.00 now if mandate_only is false
            // product_currency: 'USD',
            // product_description: 'Custom initial charge',
            // adaptive_currency_fees_inclusive: false,
          },
        },
        metadata: {
          userId,
          type: 'on_demand_mandate',
        },
      });

      const sessionId = (session as any).session_id;
      const checkoutUrl = (session as any).checkout_url;

      if (!sessionId) {
        throw new Error('No session_id in response');
      }

      if (!checkoutUrl) {
        throw new Error('No checkout_url in response');
      }

      this.logger.log(`✅ On-demand subscription checkout created: ${sessionId}`);

      return {
        checkoutUrl: checkoutUrl as string,
        sessionId: sessionId as string,
      };
    } catch (error) {
      this.logger.error(`❌ Failed to create on-demand subscription:`, error);
      if (error instanceof Error) {
        throw new Error(`Failed to create on-demand subscription: ${error.message}`);
      }
      throw new Error('Failed to create on-demand subscription. Please try again.');
    }
  }

  /**
   * Create on-demand charge for execution overages
   * According to docs: POST /subscriptions/{subscription_id}/charge
   * 
   * IMPORTANT: The subscription must be an on-demand subscription (created with mandate_only: true)
   * Regular subscriptions cannot be charged - they are billed automatically on schedule.
   * 
   * For execution overages, you need to:
   * 1. First create an on-demand subscription using createOnDemandSubscription()
   * 2. Then use this method to charge variable amounts
   */
  async createOnDemandCharge(
    userId: string,
    dto: OnDemandChargeDto,
  ): Promise<{ success: boolean; paymentId: string; message: string }> {
    const subscription = await this.subscriptionRepository.findByUserId(userId);
    if (!subscription) {
      throw new NotFoundException('Subscription not found');
    }

    if (!subscription.dodoSubscriptionId) {
      throw new NotFoundException('Active subscription ID not found. Please contact support.');
    }

    this.logger.log(
      `💳 Creating on-demand charge for user ${userId}: ${dto.productPrice} cents`,
    );

    try {
      // Use subscriptions.charge API for on-demand charges
      // According to docs: POST /subscriptions/{subscription_id}/charge
      // Required: product_price (in cents) - amount to charge in smallest currency unit
      // Optional: product_currency, product_description, adaptive_currency_fees_inclusive, metadata
      // 
      // Example: to charge $25.00, pass product_price: 2500
      const response = await this.dodoClient.subscriptions.charge(subscription.dodoSubscriptionId, {
        product_price: dto.productPrice, // Required: amount in cents (e.g., 2500 = $25.00)
        ...(dto.productCurrency && { product_currency: dto.productCurrency }),
        ...(dto.productDescription && { product_description: dto.productDescription }),
        // Note: adaptive_currency_fees_inclusive and metadata can be added if needed
      });

      // Response should contain payment_id according to docs
      const paymentId = (response as any).payment_id;

      if (!paymentId) {
        this.logger.error('No payment_id in charge response:', JSON.stringify(response, null, 2));
        throw new Error('No payment_id in charge response. The subscription may not be on-demand type.');
      }

      this.logger.log(`✅ On-demand charge created: ${paymentId}`);

      return {
        success: true,
        paymentId,
        message: 'On-demand charge created successfully',
      };
    } catch (error) {
      this.logger.error(`❌ Failed to create on-demand charge:`, error);
      
      // Provide helpful error message based on common issues
      if (error instanceof Error) {
        if (error.message.includes('on-demand') || error.message.includes('not on-demand')) {
          throw new BadRequestException(
            'This subscription is not an on-demand subscription. ' +
            'Please create an on-demand subscription first using the on-demand subscription endpoint.',
          );
        }
        throw new Error(`Failed to create on-demand charge: ${error.message}`);
      }
      throw new Error('Failed to create on-demand charge. Please try again.');
    }
  }
}
