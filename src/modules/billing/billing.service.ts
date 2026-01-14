import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

import DodoPayments from 'dodopayments';
import { SubscriptionRepository } from './repositories/subscription.repository';
import { SubscriptionEventRepository } from './repositories/subscription-event.repository';
import type { CreateCheckoutDto } from './dto/create-checkout.dto';
import type { ChangePlanDto } from './dto/change-plan.dto';
import { SubscriptionPlan, SubscriptionStatus } from './entities/subscription.entity';

@Injectable()
export class BillingService {
  private readonly logger = new Logger(BillingService.name);

  private readonly dodoClient: any;

  constructor(
    private readonly configService: ConfigService,
    private readonly subscriptionRepository: SubscriptionRepository,
    private readonly subscriptionEventRepository: SubscriptionEventRepository,
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
    // Map plan to product ID (should be configured in env or database)
    const productIdMap: Record<SubscriptionPlan, string> = {
      [SubscriptionPlan.BASIC]:
        this.configService.get<string>('DODO_PRODUCT_ID_BASIC') || 'prod_basic',
      [SubscriptionPlan.PRO]: this.configService.get<string>('DODO_PRODUCT_ID_PRO') || 'prod_pro',
      [SubscriptionPlan.ENTERPRISE]:
        this.configService.get<string>('DODO_PRODUCT_ID_ENTERPRISE') || 'prod_enterprise',
    };

    const productId = productIdMap[dto.planId];
    if (!productId) {
      throw new Error(`Product ID not found for plan: ${dto.planId}`);
    }

    // Create checkout session with Dodo Payments for subscription
    // According to Dodo Payments docs, checkout sessions return checkout_url
    const session = await this.dodoClient.checkoutSessions.create({
      product_cart: [
        {
          product_id: productId,
          quantity: 1,
        },
      ],
      customer: {
        email: userEmail,
        name: userName,
      },
      return_url: dto.returnUrl,
      metadata: {
        userId,
        planId: dto.planId,
      },
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
      const data = eventData as any;
      const subscriptionId = data?.subscription_id;

      this.logger.log(`🔍 Looking for subscription with ID: ${subscriptionId || 'NOT FOUND'}`);
      console.log(`🔍 Looking for subscription with ID: ${subscriptionId || 'NOT FOUND'}`);

      if (subscriptionId) {
        let subscription =
          await this.subscriptionRepository.findByDodoSubscriptionId(subscriptionId);

        // Extract customer ID from data
        const customerId = data?.customer_id || (data?.customer as any)?.customer_id;

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
              planId: (data.metadata.planId as SubscriptionPlan) || SubscriptionPlan.BASIC,
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
   * Change subscription plan - creates a checkout session for the new plan
   * After payment, webhook will update the subscription with new subscription ID
   */
  async changePlan(
    userId: string,
    userEmail: string,
    userName: string,
    dto: ChangePlanDto,
  ): Promise<{ checkoutUrl: string; sessionId: string }> {
    const subscription = await this.subscriptionRepository.findByUserId(userId);
    if (!subscription) {
      throw new NotFoundException('Subscription not found');
    }

    // Map plan to product ID (should be configured in env or database)
    const productIdMap: Record<SubscriptionPlan, string> = {
      [SubscriptionPlan.BASIC]:
        this.configService.get<string>('DODO_PRODUCT_ID_BASIC') || 'prod_basic',
      [SubscriptionPlan.PRO]: this.configService.get<string>('DODO_PRODUCT_ID_PRO') || 'prod_pro',
      [SubscriptionPlan.ENTERPRISE]:
        this.configService.get<string>('DODO_PRODUCT_ID_ENTERPRISE') || 'prod_enterprise',
    };

    const productId = productIdMap[dto.planId];
    if (!productId) {
      throw new Error(`Product ID not found for plan: ${dto.planId}`);
    }

    this.logger.log(
      `🔄 Creating checkout session for plan change: ${dto.planId} for user ${userId}`,
    );

    // Prepare metadata - only strings allowed by Dodo API
    const metadata = {
      userId: String(userId),
      planId: String(dto.planId),
    };

    this.logger.log(`📦 Metadata being sent:`, JSON.stringify(metadata, null, 2));
    console.log(`📦 Metadata being sent:`, JSON.stringify(metadata, null, 2));

    // Create checkout session for the new plan
    // This will create a new subscription after payment
    const session = await this.dodoClient.checkoutSessions.create({
      product_cart: [
        {
          product_id: productId,
          quantity: 1,
        },
      ],
      customer: {
        email: userEmail,
        name: userName,
      },
      return_url:
        dto.returnUrl ||
        `${this.configService.get<string>('FRONTEND_URL') || 'http://localhost:3000'}/workspace/billing?payment=success&planChanged=true`,
      metadata: metadata,
    });

    this.logger.log('Dodo Payments response type:', typeof session);
    this.logger.log('Dodo Payments response keys:', Object.keys(session || {}));
    this.logger.debug('Dodo Payments full response:', JSON.stringify(session, null, 2));

    // Extract session_id (required field)
    const sessionId = (session as any).session_id;
    if (!sessionId) {
      this.logger.error('No session_id in response:', JSON.stringify(session, null, 2));
      throw new Error('Failed to get session_id from Dodo Payments response');
    }

    this.logger.log(`Checkout session created for plan change - user ${userId}: ${sessionId}`);

    // Extract checkout_url (optional field, can be null)
    const checkoutUrl = (session as any).checkout_url;

    if (!checkoutUrl || checkoutUrl === null || checkoutUrl === undefined) {
      this.logger.error('checkout_url is missing, null, or undefined');
      this.logger.error('Full response:', JSON.stringify(session, null, 2));
      throw new Error(
        'Checkout URL is not available. Please verify your product configuration in the Dodo Payments dashboard.',
      );
    }

    this.logger.log(`Checkout URL for plan change: ${checkoutUrl}`);

    return {
      checkoutUrl: checkoutUrl as string,
      sessionId: sessionId as string,
    };
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
    } else if (data.metadata?.userId) {
      // Create new subscription
      this.logger.log(`📝 Creating new subscription for user: ${data.metadata.userId}`);
      console.log(`📝 Creating new subscription for user: ${data.metadata.userId}`);
      try {
        subscription = await this.subscriptionRepository.create({
          userId: data.metadata.userId,
          dodoSubscriptionId: data.subscription_id,
          dodoCustomerId: customerId, // Store customer ID
          planId: (data.metadata.planId as SubscriptionPlan) || SubscriptionPlan.BASIC,
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
          planId: (data.metadata.planId as SubscriptionPlan) || SubscriptionPlan.BASIC,
          status: SubscriptionStatus.ACTIVE,
          currentPeriodStart: data.current_period_start
            ? new Date(data.current_period_start)
            : new Date(),
          currentPeriodEnd: data.current_period_end ? new Date(data.current_period_end) : null,
          trialStart: data.trial_start ? new Date(data.trial_start) : null,
          trialEnd: data.trial_end ? new Date(data.trial_end) : null,
        });
        this.logger.log(`✅ Created subscription: ${subscription.id}`);
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
}
