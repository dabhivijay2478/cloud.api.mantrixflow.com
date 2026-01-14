# Billing Module - Dodo Payments Integration

This module provides Dodo Payments billing integration for organizations. Billing is organization-scoped with OWNER/ADMIN access control.

## Architecture

- **Provider-Agnostic**: Uses `IBillingProvider` interface for easy provider switching
- **One Subscription per Organization**: Each organization has a single subscription
- **Config-Driven Pricing**: All pricing and features defined in `billing.config.ts`
- **Dodo-Hosted Checkout**: Uses Dodo-hosted checkout pages (no custom payment UI)
- **Webhook-Driven**: Subscription updates are handled via Dodo Payments webhooks

## Database Schema

### Organizations Table (Updated)
Billing fields:
- `billing_provider` - Provider name ('dodo')
- `billing_customer_id` - Dodo customer ID
- `billing_subscription_id` - Dodo subscription ID
- `billing_plan_id` - Plan ID ('free', 'pro', 'scale')
- `billing_status` - Current billing status
- `billing_current_period_end` - Next billing date

### Subscriptions Table
Provider-agnostic subscription records:
- `organization_id` - Reference to organization
- `provider` - Provider name ('dodo')
- `plan_id` - Plan identifier
- `provider_subscription_id` - Dodo subscription ID
- `status` - Subscription status
- `current_period_start/end` - Billing period
- `amount`, `currency` - Pricing info

### Subscription Events Table (New)
Webhook audit log:
- `organization_id` - Reference to organization
- `provider` - Provider name ('dodo')
- `event_type` - Webhook event type
- `payload` - Raw webhook payload (JSONB)
- `created_at` - Event timestamp

## Configuration

### Billing Plans (`billing.config.ts`)

All pricing and features are defined in `apps/api/src/config/billing.config.ts`:

```typescript
export const billingPlans = {
  free: {
    pricing: { month: 0, year: 0 },
    limits: { pipelines: 2, dataSources: 1, migrationsPerMonth: 100 },
    // Free plan doesn't use Dodo Payments
  },
  pro: {
    pricing: { month: 1, year: 10 }, // Testing prices
    limits: { pipelines: 10, dataSources: 5, migrationsPerMonth: 1000 },
    dodoProductId: process.env.DODO_PRO_PRODUCT_ID, // From .env
  },
  scale: {
    pricing: { month: 1, year: 10 }, // Testing prices
    limits: { pipelines: -1, dataSources: -1, migrationsPerMonth: -1 }, // -1 = unlimited
    dodoProductId: process.env.DODO_SCALE_PRODUCT_ID, // From .env
  },
};
```

**To update pricing**: Edit `billing.config.ts` - no code changes needed!

### Environment Variables

Required environment variables (see `.env.example`):

```bash
# Billing Provider
BILLING_PROVIDER=dodo

# Dodo Payments API Keys (get from Dodo Payments Dashboard)
DODO_API_KEY=your_dodo_api_key_here
DODO_WEBHOOK_SECRET=your_dodo_webhook_secret_here

# Dodo Payments Product IDs (for Pro and Scale plans)
DODO_PRO_PRODUCT_ID=prod_pro_xxxxx
DODO_SCALE_PRODUCT_ID=prod_scale_xxxxx

# Dodo Payments API Base URL (optional, defaults to production)
DODO_API_BASE_URL=https://api.dodopayments.com

# Redirect URLs after checkout (use {organizationId} placeholder)
DODO_SUCCESS_URL=https://your-domain.com/organizations/{organizationId}/billing?success=true
DODO_CANCEL_URL=https://your-domain.com/organizations/{organizationId}/billing?canceled=true
```

## Setup Instructions

### 1. Run Database Migration

```bash
# Run the billing schema migration
psql $DATABASE_URL -f apps/api/src/database/drizzle/migrations/0014_update_billing_schema_dodo.sql
```

### 2. Configure Dodo Payments

1. Create Dodo Payments account
2. Create Products in Dodo Dashboard:
   - Pro plan (monthly/yearly)
   - Scale plan (monthly/yearly)
3. Get API keys from Dodo Dashboard > Settings > API Keys
4. Copy Product IDs to `.env` file
5. Set up webhook endpoint:
   - URL: `https://your-api-domain.com/api/billing/webhook`
   - Events to listen for:
     - `subscription.created`
     - `subscription.active`
     - `subscription.updated`
     - `subscription.cancelled`
     - `payment.succeeded`
     - `payment.failed`
6. Copy webhook signing secret to `DODO_WEBHOOK_SECRET`

### 3. Update Pricing (When Ready)

Edit `apps/api/src/config/billing.config.ts`:

```typescript
pro: {
  pricing: {
    month: 29,  // Change from 1 to 29
    year: 290,  // Change from 10 to 290
  },
},
scale: {
  pricing: {
    month: 99,  // Change from 1 to 99
    year: 990,  // Change from 10 to 990
  },
},
```

**No code changes needed** - just update the config file!

## API Endpoints

### Get Billing Overview
```
GET /api/billing/overview?organizationId=UUID
```
Returns current plan, status, and billing information.

### Get Billing Usage
```
GET /api/billing/usage?organizationId=UUID
```
Returns usage statistics (pipelines, data sources, migrations).

### Get Available Plans
```
GET /api/billing/plans
```
Returns all available plans with pricing and features.

### Create Checkout Session
```
POST /api/billing/checkout
Body: { organizationId, planId, interval, returnUrl, cancelUrl }
```
Creates Dodo checkout session and returns Dodo-hosted checkout URL.

### Get Customer Portal URL
```
GET /api/billing/portal?organizationId=UUID
```
Returns Dodo-hosted billing portal URL.

### Cancel Subscription
```
POST /api/billing/cancel
Body: { organizationId, cancelImmediately? }
```
Cancels active subscription.

### Webhook Handler
```
POST /api/billing/webhook
```
Handles Dodo Payments webhook events. **No authentication required** - uses Dodo signature verification.

## Frontend Flow

1. **Billing Page** (`/organizations/[id]/billing`):
   - Shows current plan and usage
   - Displays pricing cards for all plans
   - "Upgrade" button → calls `/api/billing/checkout` → redirects to Dodo-hosted checkout
   - "Manage Billing" button → calls `/api/billing/portal` → redirects to Dodo-hosted portal

2. **No Custom Checkout Page**:
   - All payment collection happens on Dodo-hosted pages
   - No card/UPI inputs in our frontend
   - Fully PCI-compliant

## Webhook Configuration

**Important**: For Dodo webhook signature verification, you need to preserve the raw request body.

In production, configure your NestJS app to preserve raw body for the webhook route.

## Security

- ✅ No card/UPI data collection
- ✅ No custom checkout UI
- ✅ No payment data storage
- ✅ Webhooks are source of truth
- ✅ Server-side validation only
- ✅ Signature verification for webhooks

## Testing

### Local Testing

1. Use Dodo Payments test mode API keys
2. Test checkout flow:
   - Click "Upgrade" on billing page
   - Redirects to Dodo checkout
   - Complete payment on Dodo-hosted page
   - Redirects back to billing page

### Test Pricing

Current testing prices (defined in `billing.config.ts`):
- Free: $0
- Pro: $1/month (will change to $29)
- Scale: $1/month (will change to $99)

## Production Checklist

- [ ] Set `BILLING_PROVIDER=dodo`
- [ ] Set `DODO_API_KEY` to live key
- [ ] Set `DODO_WEBHOOK_SECRET` from Dodo Dashboard
- [ ] Set `DODO_PRO_PRODUCT_ID` to live product ID
- [ ] Set `DODO_SCALE_PRODUCT_ID` to live product ID
- [ ] Configure webhook endpoint in Dodo Dashboard
- [ ] Update pricing in `billing.config.ts` to production values
- [ ] Set `DODO_SUCCESS_URL` and `DODO_CANCEL_URL` to production URLs
- [ ] Test webhook delivery
- [ ] Configure raw body parsing for webhook route
- [ ] Set up monitoring for webhook failures

## Migration from Razorpay

All Razorpay code has been removed. The system now uses Dodo Payments exclusively.

To switch providers in the future:
1. Create new provider implementation (e.g., `StripeBillingProvider`)
2. Set `BILLING_PROVIDER=stripe` in environment
3. No database migration needed - same schema supports all providers!
