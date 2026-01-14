# Dodo Payments Implementation Review & Improvements

## ✅ FIXED - All Critical Issues Resolved

**Last Updated:** Implementation completed according to official Dodo Payments documentation.

### ✅ Fixed Issues

1. **Plan Changes** - ✅ FIXED
   - Now uses `subscriptions.changePlan()` API directly
   - No checkout session needed for plan changes
   - Supports proration modes: `difference_immediately` (upgrades/downgrades)
   - Plan changes happen immediately without redirect

2. **Cancel at Period End** - ✅ IMPLEMENTED
   - New endpoint: `POST /api/billing/cancel`
   - Uses `subscriptions.update({ cancel_at_period_end: true })`
   - Resume endpoint: `POST /api/billing/resume`

3. **Payment Method Updates** - ✅ IMPLEMENTED
   - New endpoint: `POST /api/billing/update-payment-method`
   - Uses `subscriptions.updatePaymentMethod()` API
   - Handles `on_hold` status for failed payments

## Current Status vs Official Docs

### ✅ What's Already Correct

1. **Checkout Session Structure** - Correctly using `product_cart`, `customer`, `return_url`, and `metadata`
2. **Webhook Handling** - Properly handling events and storing them
3. **Basic Subscription Management** - Storing subscription state correctly
4. **Customer Portal** - Already implemented

### ❌ Critical Issues to Fix

#### 1. **Plan Changes - WRONG APPROACH** ⚠️ CRITICAL

**Current Implementation:**
- Creates a NEW checkout session for plan changes
- This creates a NEW subscription instead of updating existing one
- User has to go through checkout again

**According to Official Docs:**
- Should use `subscriptions.changePlan()` API directly
- No checkout session needed for plan changes
- Supports proration modes: `prorated_immediately`, `difference_immediately`, `difference_at_period_end`

**Required Fix:**
```typescript
// Instead of creating checkout session, use:
await client.subscriptions.changePlan(subscriptionId, {
  product_id: newProductId,
  quantity: 1,
  proration_billing_mode: 'difference_immediately' // or 'prorated_immediately'
});
```

#### 2. **Missing: Cancel at Period End**

**Required Implementation:**
```typescript
await client.subscriptions.update(subscriptionId, {
  cancel_at_period_end: true
});
```

#### 3. **Missing: Payment Method Updates**

**Required Implementation:**
```typescript
// For failed payments (on_hold status)
await client.subscriptions.updatePaymentMethod(subscriptionId, {
  type: 'new',
  return_url: 'https://yourapp.com/billing/payment-method-updated'
});
```

#### 4. **Missing: Trial Periods**

**Current:** No trial support in checkout sessions

**Required:** Add `subscription_data.trial_period_days` to checkout session:
```typescript
const session = await client.checkoutSessions.create({
  product_cart: [{ product_id: productId, quantity: 1 }],
  subscription_data: {
    trial_period_days: 14 // Optional trial
  },
  // ... rest
});
```

#### 5. **Missing: Add-ons/Seat-based Billing**

**Required:** Support add-ons in checkout and plan changes:
```typescript
// In checkout session:
product_cart: [{
  product_id: 'prod_subscription',
  quantity: 1,
  addons: [{
    addon_id: 'addon_seat',
    quantity: 10 // 10 additional seats
  }]
}]

// In plan change:
await client.subscriptions.changePlan(subscriptionId, {
  product_id: productId,
  quantity: 1,
  proration_billing_mode: 'prorated_immediately',
  addons: [{ addon_id: 'addon_seat', quantity: 15 }]
});
```

## Recommended Implementation Plan

### Phase 1: Critical Fixes (Do First)

1. **Fix Plan Changes**
   - Replace `changePlan()` method to use `subscriptions.changePlan()` API
   - Remove checkout session creation for plan changes
   - Add proration mode selection (upgrade vs downgrade)

2. **Add Cancel at Period End**
   - New endpoint: `POST /api/billing/cancel`
   - Uses `subscriptions.update({ cancel_at_period_end: true })`

### Phase 2: Important Features

3. **Add Payment Method Update**
   - New endpoint: `POST /api/billing/update-payment-method`
   - Handle `on_hold` status in UI
   - Prompt user to update payment method

4. **Add Trial Support**
   - Optional `trial_period_days` in checkout session
   - Update UI to show trial information

### Phase 3: Advanced Features (If Needed)

5. **Add-ons/Seat-based Billing**
   - Add add-on support to checkout
   - Add seat management endpoints
   - Update UI for seat management

## Code Changes Required

### 1. Fix `changePlan()` in `billing.service.ts`

**Current (WRONG):**
```typescript
async changePlan(...) {
  // Creates new checkout session - WRONG!
  const session = await this.dodoClient.checkoutSessions.create({...});
}
```

**Should be:**
```typescript
async changePlan(userId: string, dto: ChangePlanDto) {
  const subscription = await this.subscriptionRepository.findByUserId(userId);
  if (!subscription || !subscription.dodoSubscriptionId) {
    throw new NotFoundException('Active subscription not found');
  }

  const productId = productIdMap[dto.planId];
  
  // Determine proration mode based on plan tier
  const prorationMode = this.determineProrationMode(
    subscription.planId,
    dto.planId
  );

  // Use changePlan API directly - no checkout needed!
  await this.dodoClient.subscriptions.changePlan(
    subscription.dodoSubscriptionId,
    {
      product_id: productId,
      quantity: 1,
      proration_billing_mode: prorationMode,
    }
  );

  // Update local DB (webhook will also update, but this is immediate)
  await this.subscriptionRepository.update(subscription.id, {
    planId: dto.planId,
  });
}
```

### 2. Add Cancel Endpoint

```typescript
async cancelAtPeriodEnd(userId: string): Promise<void> {
  const subscription = await this.subscriptionRepository.findByUserId(userId);
  if (!subscription?.dodoSubscriptionId) {
    throw new NotFoundException('Subscription not found');
  }

  await this.dodoClient.subscriptions.update(
    subscription.dodoSubscriptionId,
    { cancel_at_period_end: true }
  );

  await this.subscriptionRepository.update(subscription.id, {
    cancelAtPeriodEnd: true,
  });
}
```

### 3. Add Payment Method Update

```typescript
async updatePaymentMethod(
  userId: string,
  returnUrl: string
): Promise<{ url: string }> {
  const subscription = await this.subscriptionRepository.findByUserId(userId);
  if (!subscription?.dodoSubscriptionId) {
    throw new NotFoundException('Subscription not found');
  }

  const response = await this.dodoClient.subscriptions.updatePaymentMethod(
    subscription.dodoSubscriptionId,
    {
      type: 'new',
      return_url: returnUrl,
    }
  );

  return { url: response.url };
}
```

## Summary

**Current approach is mostly correct for:**
- Initial subscription creation ✅
- Webhook handling ✅
- Customer portal ✅

**Needs immediate fixes:**
- Plan changes (critical - wrong approach) ❌
- Cancel at period end (missing) ❌
- Payment method updates (missing) ❌

**Nice to have:**
- Trial periods
- Add-ons/seat-based billing

The most critical issue is the plan change implementation - it should NOT create a new checkout session, but instead use the `changePlan` API directly.
