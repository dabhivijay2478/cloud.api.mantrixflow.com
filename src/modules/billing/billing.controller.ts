/**
 * Billing Controller
 * REST API endpoints for billing information and Dodo Payments integration
 */

import { Body, Controller, Get, Param, Post, Query, Request, UseGuards } from '@nestjs/common';
import type { Request as ExpressRequest } from 'express';
import {
  ApiBearerAuth,
  ApiBody,
  ApiOperation,
  ApiQuery,
  ApiResponse,
  ApiTags,
} from '@nestjs/swagger';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import { createListResponse, createSuccessResponse } from '../../common/dto/api-response.dto';
import { BillingService } from './billing.service';
import {
  BillingInvoiceDto,
  BillingOverviewDto,
  BillingUsageDto,
} from './dto/billing-response.dto';

type ExpressRequestType = ExpressRequest;

@ApiTags('billing')
@Controller('billing')
export class BillingController {
  constructor(private readonly billingService: BillingService) {}

  /**
   * Get billing overview
   * User-scoped billing (no organizationId needed)
   */
  @Get('overview')
  @UseGuards(SupabaseAuthGuard)
  @ApiBearerAuth('JWT-auth')
  @ApiOperation({
    summary: 'Get billing overview',
    description: 'Get billing overview for the authenticated user including current plan, status, and next billing date',
  })
  @ApiResponse({
    status: 200,
    description: 'Billing overview retrieved successfully',
    type: BillingOverviewDto,
  })
  @ApiResponse({
    status: 404,
    description: 'User not found',
  })
  async getBillingOverview(@Request() req: ExpressRequestType) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User ID is required');
    }

    const overview = await this.billingService.getBillingOverview(userId);
    return createSuccessResponse(overview, 'Billing overview retrieved successfully');
  }

  /**
   * Get billing usage
   * User-scoped billing (no organizationId needed)
   */
  @Get('usage')
  @UseGuards(SupabaseAuthGuard)
  @ApiBearerAuth('JWT-auth')
  @ApiOperation({
    summary: 'Get billing usage',
    description: 'Get usage statistics for the authenticated user including pipelines, migrations, and data sources',
  })
  @ApiResponse({
    status: 200,
    description: 'Billing usage retrieved successfully',
    type: BillingUsageDto,
  })
  @ApiResponse({
    status: 404,
    description: 'User not found',
  })
  async getBillingUsage(@Request() req: ExpressRequestType) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User ID is required');
    }

    const usage = await this.billingService.getBillingUsage(userId);
    return createSuccessResponse(usage, 'Billing usage retrieved successfully');
  }

  /**
   * Get billing invoices
   * User-scoped billing (no organizationId needed)
   */
  @Get('invoices')
  @UseGuards(SupabaseAuthGuard)
  @ApiBearerAuth('JWT-auth')
  @ApiOperation({
    summary: 'Get billing invoices',
    description: 'Get list of invoices for the authenticated user',
  })
  @ApiResponse({
    status: 200,
    description: 'Billing invoices retrieved successfully',
    type: [BillingInvoiceDto],
  })
  @ApiResponse({
    status: 404,
    description: 'User not found',
  })
  async getBillingInvoices(@Request() req: ExpressRequestType) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User ID is required');
    }

    const invoices = await this.billingService.getBillingInvoices(userId);
    return createListResponse(invoices, 'Billing invoices retrieved successfully');
  }

  /**
   * Get invoice download URL
   * User-scoped billing (no organizationId needed)
   */
  @Get('invoices/:invoiceId/download')
  @UseGuards(SupabaseAuthGuard)
  @ApiBearerAuth('JWT-auth')
  @ApiOperation({
    summary: 'Get invoice download URL',
    description: 'Get download URL for a specific invoice from Dodo Payments',
  })
  @ApiResponse({
    status: 200,
    description: 'Invoice download URL retrieved successfully',
  })
  @ApiResponse({
    status: 404,
    description: 'Invoice not found',
  })
  async getInvoiceDownloadUrl(
    @Param('invoiceId') invoiceId: string,
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User ID is required');
    }

    if (!invoiceId) {
      throw new Error('Invoice ID is required');
    }

    const downloadUrl = await this.billingService.getInvoiceDownloadUrl(userId, invoiceId);
    return createSuccessResponse({ downloadUrl }, 'Invoice download URL retrieved successfully');
  }

  /**
   * Get available plans
   */
  @Get('plans')
  @UseGuards(SupabaseAuthGuard)
  @ApiBearerAuth('JWT-auth')
  @ApiOperation({
    summary: 'Get available billing plans',
    description: 'Get all available billing plans with pricing and features',
  })
  @ApiResponse({
    status: 200,
    description: 'Plans retrieved successfully',
  })
  async getPlans() {
    const plans = this.billingService.getAvailablePlans();
    return createSuccessResponse(plans, 'Plans retrieved successfully');
  }

  /**
   * Create checkout session for subscription
   * Returns Dodo-hosted checkout URL
   * User-scoped billing (no organizationId needed)
   */
  @Post('checkout')
  @UseGuards(SupabaseAuthGuard)
  @ApiBearerAuth('JWT-auth')
  @ApiOperation({
    summary: 'Create checkout session',
    description: 'Create a checkout session and get Dodo-hosted checkout URL',
  })
  @ApiBody({
    schema: {
      type: 'object',
      properties: {
        planId: {
          type: 'string',
          description: 'Plan ID (pro, scale)',
          enum: ['pro', 'scale'],
        },
        interval: {
          type: 'string',
          description: 'Billing interval',
          enum: ['month', 'year'],
        },
        returnUrl: {
          type: 'string',
          description: 'URL to return to after successful checkout',
        },
        cancelUrl: {
          type: 'string',
          description: 'URL to return to after canceled checkout',
        },
      },
      required: ['planId', 'interval', 'returnUrl', 'cancelUrl'],
    },
  })
  @ApiResponse({
    status: 200,
    description: 'Checkout session created successfully',
  })
  async createCheckoutSession(
    @Body()
    body: {
      planId: string;
      interval: 'month' | 'year';
      returnUrl: string;
      cancelUrl: string;
    },
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User ID is required');
    }

    const result = await this.billingService.createCheckoutSession(
      userId,
      body.planId,
      body.interval,
      body.returnUrl,
      body.cancelUrl,
    );

    return createSuccessResponse(result, 'Checkout session created successfully');
  }

  /**
   * Get customer portal URL
   * Returns Dodo-hosted billing portal URL
   * User-scoped billing (no organizationId needed)
   */
  @Get('portal')
  @UseGuards(SupabaseAuthGuard)
  @ApiBearerAuth('JWT-auth')
  @ApiOperation({
    summary: 'Get customer portal URL',
    description: 'Get Dodo-hosted billing portal URL for managing subscription',
  })
  @ApiResponse({
    status: 200,
    description: 'Portal URL retrieved successfully',
  })
  @ApiResponse({
    status: 404,
    description: 'User not found or no active subscription',
  })
  async getCustomerPortal(@Request() req: ExpressRequestType) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User ID is required');
    }

    const portalUrl = await this.billingService.getCustomerPortalUrl(userId);
    return createSuccessResponse({ url: portalUrl }, 'Portal URL retrieved successfully');
  }

  /**
   * Cancel subscription
   * User-scoped billing (no organizationId needed)
   */
  @Post('cancel')
  @UseGuards(SupabaseAuthGuard)
  @ApiBearerAuth('JWT-auth')
  @ApiOperation({
    summary: 'Cancel subscription',
    description: 'Cancel an active subscription',
  })
  @ApiBody({
    schema: {
      type: 'object',
      properties: {
        cancelImmediately: {
          type: 'boolean',
          description: 'Cancel immediately or at period end',
          default: false,
        },
      },
    },
  })
  @ApiResponse({
    status: 200,
    description: 'Subscription cancelled successfully',
  })
  async cancelSubscription(
    @Body() body: { cancelImmediately?: boolean },
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User ID is required');
    }

    await this.billingService.cancelSubscription(userId, body.cancelImmediately || false);

    return createSuccessResponse(null, 'Subscription cancelled successfully');
  }

  /**
   * Dodo Payments webhook endpoint
   * This endpoint should NOT use SupabaseAuthGuard
   * It uses Dodo webhook signature verification instead
   * 
   * IMPORTANT: Configure webhook URL in Dodo Dashboard as:
   * https://your-domain.com/api/billing/webhook
   */
  @Post('webhook')
  @ApiOperation({
    summary: 'Dodo Payments webhook handler',
    description: 'Handle Dodo Payments webhook events for subscription and payment updates',
  })
  @ApiResponse({
    status: 200,
    description: 'Webhook processed successfully',
  })
  async handleWebhook(@Request() req: ExpressRequestType) {
    console.log('📥 Dodo webhook received');
    console.log('Headers:', {
      'webhook-id': req.headers['webhook-id'],
      'webhook-timestamp': req.headers['webhook-timestamp'],
      'webhook-signature': req.headers['webhook-signature'] ? 'present' : 'missing',
      'x-dodo-signature': req.headers['x-dodo-signature'] ? 'present' : 'missing',
    });

    // Dodo sends signature in 'webhook-signature' header (not 'x-dodo-signature')
    // Format: "v1,<signature>" or just "<signature>"
    const signatureHeader = 
      (req.headers['webhook-signature'] as string) ||
      (req.headers['x-dodo-signature'] as string); // Fallback for compatibility

    // Get webhook metadata headers
    const webhookId = req.headers['webhook-id'] as string;
    const webhookTimestamp = req.headers['webhook-timestamp'] as string;

    // Get raw body for signature verification
    // With bodyParser.raw middleware in main.ts, req.body is a Buffer
    // Convert to string for signature verification
    const rawBody = (req as any).rawBody 
      ? (req as any).rawBody.toString('utf8')
      : Buffer.isBuffer(req.body)
        ? req.body.toString('utf8')
        : JSON.stringify(req.body);

    // If signature header is missing, allow in dev mode (for testing)
    const skipVerification = process.env.DODO_SKIP_WEBHOOK_SIGNATURE === 'true';
    if (!signatureHeader && !skipVerification) {
      console.error('❌ Dodo webhook signature header missing. Available headers:', Object.keys(req.headers));
      throw new Error('Dodo signature header is missing');
    }

    // Extract signature from "v1,<signature>" format if present
    const signature = signatureHeader && signatureHeader.includes(',') 
      ? signatureHeader.split(',')[1]?.trim() 
      : signatureHeader || '';

    try {
      await this.billingService.handleWebhookEvent(
        rawBody, 
        signature,
        webhookId,
        webhookTimestamp
      );
      console.log('✅ Webhook processed successfully');
      return { received: true };
    } catch (error) {
      console.error('❌ Webhook processing failed:', error);
      // Return 200 to prevent Dodo from retrying on our errors
      // But log the error for debugging
      return { received: false, error: error instanceof Error ? error.message : 'Unknown error' };
    }
  }
}
