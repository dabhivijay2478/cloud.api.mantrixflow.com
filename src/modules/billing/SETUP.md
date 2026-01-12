# Razorpay Billing Setup Guide

## Quick Start

### 1. Database Migration

```bash
# Run the migration to create billing tables
psql $DATABASE_URL -f apps/api/src/database/drizzle/migrations/0013_update_billing_schema_razorpay.sql
```

### 2. Environment Variables

Add to your `.env` file:

```bash
# Billing Provider
BILLING_PROVIDER=razorpay

# Razorpay API Keys (get from https://dashboard.razorpay.com)
RAZORPAY_KEY_ID=rzp_test_xxxxxxxxxxxxx
RAZORPAY_KEY_SECRET=xxxxxxxxxxxxxxxxxxxxx
RAZORPAY_WEBHOOK_SECRET=xxxxxxxxxxxxxxxxxxxxx

# Frontend (public key - safe to expose)
NEXT_PUBLIC_RAZORPAY_KEY_ID=rzp_test_xxxxxxxxxxxxx
```

### 3. Razorpay Dashboard Setup

1. **Create Razorpay Account**: https://razorpay.com
2. **Get API Keys**: Dashboard > Settings > API Keys
3. **Create Plans** (optional): Plans will be created automatically, or create manually in dashboard
4. **Setup Webhook**:
   - URL: `https://your-api-domain.com/api/billing/webhook`
   - Events:
     - `subscription.activated`
     - `subscription.charged`
     - `subscription.cancelled`
     - `payment.failed`
   - Copy webhook secret to `RAZORPAY_WEBHOOK_SECRET`

### 4. Update Pricing (When Ready)

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

### 5. Restart Server

```bash
cd apps/api
npm run start:dev
```

## Testing

### Test Cards (Razorpay Test Mode)

- **Success**: `4111 1111 1111 1111`
- **Decline**: `4000 0000 0000 0002`
- **3D Secure**: `4000 0027 6000 3184`

### Test Flow

1. Go to `/workspace/billing`
2. Click "Upgrade" on a plan
3. Fill payment form on checkout page
4. Use test card: `4111 1111 1111 1111`
5. Complete payment
6. Verify subscription in Razorpay Dashboard

## Current Pricing (Testing)

- **Free**: $0
- **Pro**: $1/month (will change to $29)
- **Scale**: $1/month (will change to $99)

Update in `billing.config.ts` when ready for production.

## Architecture

- **Provider-Agnostic**: Easy to add Stripe later
- **Config-Driven**: All pricing in one file
- **Organization-Scoped**: One subscription per organization
- **OWNER/ADMIN Only**: Billing access restricted

## Files Changed

### Backend
- ✅ Removed all Stripe code
- ✅ Created Razorpay provider
- ✅ Updated database schema
- ✅ Created billing config file
- ✅ Updated billing service

### Frontend
- ✅ Updated billing page with pricing cards
- ✅ Created custom checkout page
- ✅ Removed Stripe references

## Next Steps

1. Test with Razorpay test mode
2. Update pricing in `billing.config.ts`
3. Configure production webhook
4. Deploy!
