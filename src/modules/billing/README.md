# Billing Module - Razorpay Integration

This module provides Razorpay billing integration for organizations. Billing is organization-scoped with OWNER/ADMIN access control.

## Architecture

- **Provider-Agnostic**: Uses `IBillingProvider` interface for easy provider switching
- **One Subscription per Organization**: Each organization has a single subscription
- **Config-Driven Pricing**: All pricing and features defined in `billing.config.ts`
- **Custom Checkout**: Uses Razorpay Checkout JS with custom UI
- **Webhook-Driven**: Subscription updates are handled via Razorpay webhooks

## Database Schema

### Organizations Table (Updated)
Added billing fields:
- `billing_provider` - Provider name ('razorpay')
- `billing_customer_id` - Razorpay customer ID
- `billing_subscription_id` - Razorpay subscription ID
- `billing_plan_id` - Plan ID ('free', 'pro', 'scale')
- `billing_status` - Current billing status
- `billing_current_period_end` - Next billing date

### Subscriptions Table (New)
Provider-agnostic subscription records:
- `organization_id` - Reference to organization
- `provider` - Provider name ('razorpay')
- `plan_id` - Plan identifier
- `provider_subscription_id` - Razorpay subscription ID
- `status` - Subscription status
- `current_period_start/end` - Billing period
- `amount`, `currency` - Pricing info

## Configuration

### Billing Plans (`billing.config.ts`)

All pricing and features are defined in `apps/api/src/config/billing.config.ts`:

```typescript
export const billingPlans = {
  free: {
    pricing: { month: 0, year: 0 },
    limits: { pipelines: 2, dataSources: 1, migrationsPerMonth: 100 },
  },
  pro: {
    pricing: { month: 1, year: 10 }, // Testing prices
    limits: { pipelines: 10, dataSources: 5, migrationsPerMonth: 1000 },
  },
  scale: {
    pricing: { month: 1, year: 10 }, // Testing prices
    limits: { pipelines: -1, dataSources: -1, migrationsPerMonth: -1 }, // -1 = unlimited
  },
};
```

**To update pricing**: Edit `billing.config.ts` - no code changes needed!

### Environment Variables

Required environment variables:

```bash
# Billing Provider
BILLING_PROVIDER=razorpay

# Razorpay API Keys (get from Razorpay Dashboard)
RAZORPAY_KEY_ID=rzp_test_...  # or rzp_live_... for production
RAZORPAY_KEY_SECRET=...       # Secret key (server-side only)
RAZORPAY_WEBHOOK_SECRET=...   # Webhook signing secret

# Frontend (for Razorpay Checkout JS)
NEXT_PUBLIC_RAZORPAY_KEY_ID=rzp_test_...  # Public key (safe to expose)
```

## Setup Instructions

### 1. Run Database Migration

```bash
# Run the billing schema migration
psql $DATABASE_URL -f apps/api/src/database/drizzle/migrations/0013_update_billing_schema_razorpay.sql
```

### 2. Configure Razorpay

1. Create Razorpay account (India)
2. Get API keys from Razorpay Dashboard > Settings > API Keys
3. Create Plans in Razorpay Dashboard (or let the system create them automatically)
4. Set up webhook endpoint:
   - URL: `https://your-api-domain.com/api/billing/webhook`
   - Events to listen for:
     - `subscription.activated`
     - `subscription.charged`
     - `subscription.cancelled`
     - `payment.failed`
5. Copy webhook signing secret to `RAZORPAY_WEBHOOK_SECRET`

### 3. Update Pricing (When Ready)

Edit `apps/api/src/config/billing.config.ts`:

```typescript
pro: {
  pricing: {
    month: 29,  // $29/month (or ₹2400)
    year: 290,  // $290/year
  },
  // ...
}
```

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
Creates Razorpay subscription and returns checkout data.

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
Handles Razorpay webhook events. **No authentication required** - uses Razorpay signature verification.

## Frontend Flow

1. **Billing Page** (`/workspace/billing`):
   - Shows current plan and usage
   - Displays pricing cards for all plans
   - "Upgrade" button → creates checkout session → redirects to checkout page

2. **Checkout Page** (`/workspace/billing/checkout`):
   - Custom payment form
   - Uses Razorpay Checkout JS
   - Handles payment completion

## Webhook Configuration

**Important**: For Razorpay webhook signature verification, you need to preserve the raw request body.

In production, configure your NestJS app to preserve raw body for the webhook route.

## Testing

### Local Testing

1. Use Razorpay test mode API keys
2. Test cards:
   - Success: `4111 1111 1111 1111`
   - Decline: `4000 0000 0000 0002`
3. Test webhooks using Razorpay Dashboard > Webhooks > Test

### Test Pricing

Current testing prices (defined in `billing.config.ts`):
- Free: $0
- Pro: $1/month (will change to $29)
- Scale: $1/month (will change to $99)

## Production Checklist

- [ ] Set `BILLING_PROVIDER=razorpay`
- [ ] Set `RAZORPAY_KEY_ID` to live key
- [ ] Set `RAZORPAY_KEY_SECRET` to live secret
- [ ] Configure webhook endpoint in Razorpay Dashboard
- [ ] Set `RAZORPAY_WEBHOOK_SECRET` from Razorpay Dashboard
- [ ] Set `NEXT_PUBLIC_RAZORPAY_KEY_ID` to live public key
- [ ] Update pricing in `billing.config.ts` to production values
- [ ] Test webhook delivery
- [ ] Configure raw body parsing for webhook route
- [ ] Set up monitoring for webhook failures

## Future: Adding Stripe Support

To add Stripe later (without code changes):

1. Create `StripeBillingProvider` implementing `IBillingProvider`
2. Set `BILLING_PROVIDER=stripe` in environment
3. Add Stripe config to `billing.config.ts`
4. No database migration needed - same schema supports both providers!
