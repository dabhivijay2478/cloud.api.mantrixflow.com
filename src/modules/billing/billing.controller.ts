/**
 * Billing Controller
 * REST API endpoints for billing information and Stripe integration
 */

import { Body, Controller, Get, Post, Query, Request, UseGuards } from '@nestjs/common';
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
import { RequiredUUIDPipe } from '../activity-logs/pipes/required-uuid.pipe';
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
   */
  @Get('overview')
  @UseGuards(SupabaseAuthGuard)
  @ApiBearerAuth('JWT-auth')
  @ApiOperation({
    summary: 'Get billing overview',
    description: 'Get billing overview for an organization including current plan, status, and next billing date',
  })
  @ApiQuery({
    name: 'organizationId',
    required: true,
    description: 'Organization ID',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Billing overview retrieved successfully',
    type: BillingOverviewDto,
  })
  @ApiResponse({
    status: 403,
    description: 'Forbidden - Only owners and admins can access billing',
  })
  @ApiResponse({
    status: 404,
    description: 'Organization not found',
  })
  async getBillingOverview(
    @Query('organizationId', RequiredUUIDPipe) organizationId: string,
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User ID is required');
    }

    const overview = await this.billingService.getBillingOverview(organizationId, userId);
    return createSuccessResponse(overview, 'Billing overview retrieved successfully');
  }

  /**
   * Get billing usage
   */
  @Get('usage')
  @UseGuards(SupabaseAuthGuard)
  @ApiBearerAuth('JWT-auth')
  @ApiOperation({
    summary: 'Get billing usage',
    description: 'Get usage statistics for an organization including pipelines, migrations, and data sources',
  })
  @ApiQuery({
    name: 'organizationId',
    required: true,
    description: 'Organization ID',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Billing usage retrieved successfully',
    type: BillingUsageDto,
  })
  @ApiResponse({
    status: 403,
    description: 'Forbidden - Only owners and admins can access billing',
  })
  @ApiResponse({
    status: 404,
    description: 'Organization not found',
  })
  async getBillingUsage(
    @Query('organizationId', RequiredUUIDPipe) organizationId: string,
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User ID is required');
    }

    const usage = await this.billingService.getBillingUsage(organizationId, userId);
    return createSuccessResponse(usage, 'Billing usage retrieved successfully');
  }

  /**
   * Get billing invoices
   */
  @Get('invoices')
  @UseGuards(SupabaseAuthGuard)
  @ApiBearerAuth('JWT-auth')
  @ApiOperation({
    summary: 'Get billing invoices',
    description: 'Get list of invoices for an organization',
  })
  @ApiQuery({
    name: 'organizationId',
    required: true,
    description: 'Organization ID',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Billing invoices retrieved successfully',
    type: [BillingInvoiceDto],
  })
  @ApiResponse({
    status: 403,
    description: 'Forbidden - Only owners and admins can access billing',
  })
  @ApiResponse({
    status: 404,
    description: 'Organization not found',
  })
  async getBillingInvoices(
    @Query('organizationId', RequiredUUIDPipe) organizationId: string,
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User ID is required');
    }

    const invoices = await this.billingService.getBillingInvoices(organizationId, userId);
    return createListResponse(invoices, 'Billing invoices retrieved successfully');
  }

  /**
   * Create Stripe Customer Portal session
   */
  @Post('create-portal-session')
  @UseGuards(SupabaseAuthGuard)
  @ApiBearerAuth('JWT-auth')
  @ApiOperation({
    summary: 'Create Stripe Customer Portal session',
    description: 'Create a Stripe Customer Portal session URL for managing billing',
  })
  @ApiBody({
    schema: {
      type: 'object',
      properties: {
        organizationId: {
          type: 'string',
          description: 'Organization ID',
        },
        returnUrl: {
          type: 'string',
          description: 'URL to return to after portal session',
        },
      },
      required: ['organizationId', 'returnUrl'],
    },
  })
  @ApiResponse({
    status: 200,
    description: 'Portal session URL created successfully',
    schema: {
      type: 'object',
      properties: {
        url: {
          type: 'string',
          description: 'Stripe Customer Portal URL',
        },
      },
    },
  })
  @ApiResponse({
    status: 403,
    description: 'Forbidden - Only owners and admins can access billing',
  })
  async createPortalSession(
    @Body() body: { organizationId: string; returnUrl: string },
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User ID is required');
    }

    const url = await this.billingService.createPortalSession(
      body.organizationId,
      userId,
      body.returnUrl,
    );

    return createSuccessResponse({ url }, 'Portal session created successfully');
  }

  /**
   * Create Stripe Checkout session
   */
  @Post('create-checkout-session')
  @UseGuards(SupabaseAuthGuard)
  @ApiBearerAuth('JWT-auth')
  @ApiOperation({
    summary: 'Create Stripe Checkout session',
    description: 'Create a Stripe Checkout session URL for subscribing to a plan',
  })
  @ApiBody({
    schema: {
      type: 'object',
      properties: {
        organizationId: {
          type: 'string',
          description: 'Organization ID',
        },
        planId: {
          type: 'string',
          description: 'Plan ID (e.g., pro, enterprise)',
        },
        successUrl: {
          type: 'string',
          description: 'URL to redirect to after successful checkout',
        },
        cancelUrl: {
          type: 'string',
          description: 'URL to redirect to after canceled checkout',
        },
      },
      required: ['organizationId', 'planId', 'successUrl', 'cancelUrl'],
    },
  })
  @ApiResponse({
    status: 200,
    description: 'Checkout session URL created successfully',
    schema: {
      type: 'object',
      properties: {
        url: {
          type: 'string',
          description: 'Stripe Checkout URL',
        },
      },
    },
  })
  @ApiResponse({
    status: 403,
    description: 'Forbidden - Only owners and admins can access billing',
  })
  async createCheckoutSession(
    @Body()
    body: {
      organizationId: string;
      planId: string;
      successUrl: string;
      cancelUrl: string;
    },
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User ID is required');
    }

    const url = await this.billingService.createCheckoutSession(
      body.organizationId,
      userId,
      body.planId,
      body.successUrl,
      body.cancelUrl,
    );

    return createSuccessResponse({ url }, 'Checkout session created successfully');
  }

  /**
   * Stripe webhook endpoint
   * This endpoint should NOT use SupabaseAuthGuard
   * It uses Stripe webhook signature verification instead
   * 
   * IMPORTANT: Configure NestJS to preserve raw body for webhook signature verification
   * You may need to use a custom body parser middleware for this route
   */
  @Post('webhook')
  @ApiOperation({
    summary: 'Stripe webhook handler',
    description: 'Handle Stripe webhook events for subscription and invoice updates',
  })
  @ApiResponse({
    status: 200,
    description: 'Webhook processed successfully',
  })
  async handleWebhook(@Request() req: ExpressRequestType) {
    const sig = req.headers['stripe-signature'] as string;
    const webhookSecret = process.env.STRIPE_WEBHOOK_SECRET;

    if (!webhookSecret) {
      throw new Error('STRIPE_WEBHOOK_SECRET is required');
    }

    if (!sig) {
      throw new Error('Stripe signature header is missing');
    }

    let event;
    try {
      // Get raw body for Stripe signature verification
      // Note: You may need to configure NestJS body parser to preserve raw body
      // For Stripe webhooks, the raw body is required for signature verification
      const rawBody = (req as any).rawBody || req.body;
      event = await this.billingService.verifyWebhookSignature(rawBody, sig, webhookSecret);
    } catch (err) {
      console.error('Webhook signature verification failed:', err);
      throw new Error('Webhook signature verification failed');
    }

    await this.billingService.handleWebhookEvent(event);

    return { received: true };
  }
}
