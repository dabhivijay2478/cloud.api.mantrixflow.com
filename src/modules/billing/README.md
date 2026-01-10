# Billing Module - Stripe Integration

This module provides Stripe billing integration for organizations. Billing is organization-scoped with OWNER/ADMIN access control.

## Architecture

- **One Stripe Customer per Organization**: Each organization has a single Stripe customer
- **Stripe as Source of Truth**: All billing data comes from Stripe; we only store references
- **Minimal UI**: Stripe handles all billing UX (checkout, portal, invoices)
- **Webhook-Driven**: Subscription updates are handled via Stripe webhooks

## Database Schema

The `billing_subscriptions` table stores:
- `organization_id` - Reference to organization
- `stripe_customer_id` - Stripe customer ID
- `stripe_subscription_id` - Stripe subscription ID (nullable)
- `plan_id` - Plan identifier (e.g., 'pro', 'enterprise')
- `billing_status` - Current billing status (synced from Stripe)

## Environment Variables

Required environment variables:

```bash
# Stripe API Keys (get from Stripe Dashboard)
STRIPE_SECRET_KEY=sk_test_...  # or sk_live_... for production

# Stripe Webhook Secret (get from Stripe Dashboard > Webhooks)
STRIPE_WEBHOOK_SECRET=whsec_...

# Stripe Price IDs (configure based on your Stripe products)
STRIPE_PRICE_ID_PRO=price_...  # Price ID for Pro plan
STRIPE_PRICE_ID_ENTERPRISE=price_...  # Price ID for Enterprise plan
```

## Setup Instructions

### 1. Run Database Migration

```bash
# Run the billing subscriptions table migration
psql $DATABASE_URL -f src/database/drizzle/migrations/0012_add_billing_subscriptions_table.sql
```

### 2. Configure Stripe

1. Create a Stripe account (India)
2. Create Products and Prices in Stripe Dashboard
3. Get your API keys from Stripe Dashboard > Developers > API keys
4. Set up webhook endpoint in Stripe Dashboard:
   - URL: `https://your-api-domain.com/api/billing/webhook`
   - Events to listen for:
     - `customer.subscription.created`
     - `customer.subscription.updated`
     - `customer.subscription.deleted`
     - `invoice.payment_succeeded`
     - `invoice.payment_failed`
5. Copy webhook signing secret to `STRIPE_WEBHOOK_SECRET`

### 3. Configure Price IDs

Set environment variables for your Stripe Price IDs:
```bash
STRIPE_PRICE_ID_PRO=price_xxxxx
STRIPE_PRICE_ID_ENTERPRISE=price_yyyyy
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

### Get Billing Invoices
```
GET /api/billing/invoices?organizationId=UUID
```
Returns list of invoices from Stripe.

### Create Portal Session
```
POST /api/billing/create-portal-session
Body: { organizationId, returnUrl }
```
Creates Stripe Customer Portal session URL for managing billing.

### Create Checkout Session
```
POST /api/billing/create-checkout-session
Body: { organizationId, planId, successUrl, cancelUrl }
```
Creates Stripe Checkout session URL for subscribing to a plan.

### Webhook Handler
```
POST /api/billing/webhook
```
Handles Stripe webhook events. **No authentication required** - uses Stripe signature verification.

## Webhook Configuration

**Important**: For Stripe webhook signature verification to work, you need to preserve the raw request body.

In production, configure your NestJS app to preserve raw body for the webhook route. You may need to:

1. Use a custom body parser middleware
2. Or configure your reverse proxy (nginx, etc.) to pass raw body
3. Or use Stripe CLI for local testing: `stripe listen --forward-to localhost:8000/api/billing/webhook`

## Frontend Integration

The billing page (`/workspace/billing`) provides:
- **Manage Billing** button → Redirects to Stripe Customer Portal
- **Upgrade Plan** button → Redirects to Stripe Checkout
- Minimal invoice display → Full history in Stripe Portal

All billing management happens in Stripe-hosted pages.

## Testing

### Local Testing with Stripe CLI

1. Install Stripe CLI: `brew install stripe/stripe-cli/stripe`
2. Login: `stripe login`
3. Forward webhooks: `stripe listen --forward-to localhost:8000/api/billing/webhook`
4. Use test mode API keys in `.env`

### Test Cards

Use Stripe test cards:
- Success: `4242 4242 4242 4242`
- Decline: `4000 0000 0000 0002`
- 3D Secure: `4000 0027 6000 3184`

## Production Checklist

- [ ] Set `STRIPE_SECRET_KEY` to live key
- [ ] Configure webhook endpoint in Stripe Dashboard
- [ ] Set `STRIPE_WEBHOOK_SECRET` from Stripe Dashboard
- [ ] Configure all `STRIPE_PRICE_ID_*` environment variables
- [ ] Test webhook delivery
- [ ] Configure raw body parsing for webhook route
- [ ] Set up monitoring for webhook failures
