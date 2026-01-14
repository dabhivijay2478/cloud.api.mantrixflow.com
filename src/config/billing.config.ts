/**
 * Billing Configuration
 * Centralized configuration for billing plans, pricing, and features
 * All pricing and feature limits are defined here for easy updates
 * Provider configuration is also centralized here
 */

export type BillingProvider = 'dodo';

export type PlanId = 'free' | 'pro' | 'scale';
export type BillingInterval = 'month' | 'year';

export interface PlanFeature {
  label: string;
  value: string | number;
  unit?: string;
}

export interface PlanPricing {
  month: number; // Price in base currency (USD)
  year: number; // Price in base currency (USD)
  // Country-specific pricing (optional, falls back to base pricing)
  countryPricing?: Record<string, { month: number; year: number }>;
}

export interface PlanConfig {
  id: PlanId;
  name: string;
  description: string;
  pricing: PlanPricing;
  features: PlanFeature[];
  limits: {
    pipelines: number;
    dataSources: number;
    migrationsPerMonth: number;
  };
  // Dodo Payments product ID (only for paid plans)
  dodoProductId?: string;
}

/**
 * Billing Plans Configuration
 * Update pricing and features here - no code changes needed
 */
export const billingPlans: Record<PlanId, PlanConfig> = {
  free: {
    id: 'free',
    name: 'Free',
    description: 'Perfect for getting started',
    pricing: {
      month: 0,
      year: 0,
    },
    features: [
      { label: 'Pipelines', value: 2 },
      { label: 'Data Sources', value: 1 },
      { label: 'Migrations per month', value: 100 },
      { label: 'Email support', value: 'Community' },
    ],
    limits: {
      pipelines: 2,
      dataSources: 1,
      migrationsPerMonth: 100,
    },
    // Free plan doesn't use Dodo Payments
  },
  pro: {
    id: 'pro',
    name: 'Pro',
    description: 'For growing teams',
    pricing: {
      month: 1, // Testing: $1, Production: $29
      year: 10, // Testing: $10, Production: $290 (2 months free)
      countryPricing: {
        // India-specific pricing (optional)
        IN: {
          month: 1, // Testing: ₹1, Production: ₹2400
          year: 10, // Testing: ₹10, Production: ₹24000
        },
      },
    },
    features: [
      { label: 'Pipelines', value: 10 },
      { label: 'Data Sources', value: 5 },
      { label: 'Migrations per month', value: 1000 },
      { label: 'Email support', value: 'Priority' },
      { label: 'Daily backups', value: '7 days' },
    ],
    limits: {
      pipelines: 10,
      dataSources: 5,
      migrationsPerMonth: 1000,
    },
    // Dodo Payments product ID - set via environment variable
    dodoProductId: process.env.DODO_PRO_PRODUCT_ID,
  },
  scale: {
    id: 'scale',
    name: 'Scale',
    description: 'For enterprise teams',
    pricing: {
      month: 1, // Testing: $1, Production: $99
      year: 10, // Testing: $10, Production: $990 (2 months free)
      countryPricing: {
        IN: {
          month: 1, // Testing: ₹1, Production: ₹8200
          year: 10, // Testing: ₹10, Production: ₹82000
        },
      },
    },
    features: [
      { label: 'Pipelines', value: 'Unlimited' },
      { label: 'Data Sources', value: 'Unlimited' },
      { label: 'Migrations per month', value: 'Unlimited' },
      { label: 'Email support', value: 'Priority' },
      { label: 'Daily backups', value: '30 days' },
      { label: 'Dedicated support', value: 'Yes' },
    ],
    limits: {
      pipelines: -1, // -1 means unlimited
      dataSources: -1,
      migrationsPerMonth: -1,
    },
    // Dodo Payments product ID - set via environment variable
    dodoProductId: process.env.DODO_SCALE_PRODUCT_ID,
  },
};

/**
 * Get plan configuration by ID
 */
export function getPlanConfig(planId: PlanId): PlanConfig {
  return billingPlans[planId];
}

/**
 * Get plan price for a specific country and interval
 */
export function getPlanPrice(
  planId: PlanId,
  interval: BillingInterval,
  countryCode?: string,
): number {
  const plan = billingPlans[planId];
  if (!plan) {
    throw new Error(`Plan ${planId} not found`);
  }

  // Check for country-specific pricing
  if (countryCode && plan.pricing.countryPricing?.[countryCode]) {
    return plan.pricing.countryPricing[countryCode][interval];
  }

  // Fall back to base pricing
  return plan.pricing[interval];
}

/**
 * Get all available plans
 */
export function getAllPlans(): PlanConfig[] {
  return Object.values(billingPlans);
}

/**
 * Billing Provider Configuration
 * All provider configs are centralized here
 */
export const billingConfig = {
  provider: (process.env.BILLING_PROVIDER || 'dodo') as BillingProvider,
  // Dodo Payments Configuration
  dodo: {
    apiKey: process.env.DODO_API_KEY || '',
    webhookSecret: process.env.DODO_WEBHOOK_SECRET || '',
    // Product IDs for paid plans (set in Dodo Payments dashboard)
    productIds: {
      pro: process.env.DODO_PRO_PRODUCT_ID || '',
      scale: process.env.DODO_SCALE_PRODUCT_ID || '',
    },
    // URLs for redirects
    successUrl: process.env.DODO_SUCCESS_URL || '',
    cancelUrl: process.env.DODO_CANCEL_URL || '',
    // Base URL for Dodo Payments API (optional - SDK handles this automatically)
    // Set to 'test.dodopayments.com' for test mode, or leave empty for SDK default
    apiBaseUrl: process.env.DODO_API_BASE_URL || '',
  },
};
