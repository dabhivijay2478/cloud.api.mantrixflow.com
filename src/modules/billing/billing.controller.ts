/**
 * Billing Controller
 * REST API endpoints for billing information and Dodo Payments integration
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
        organizationId: {
          type: 'string',
          description: 'Organization ID',
        },
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
      required: ['organizationId', 'planId', 'interval', 'returnUrl', 'cancelUrl'],
    },
  })
  @ApiResponse({
    status: 200,
    description: 'Checkout session created successfully',
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
      body.organizationId,
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
   */
  @Get('portal')
  @UseGuards(SupabaseAuthGuard)
  @ApiBearerAuth('JWT-auth')
  @ApiOperation({
    summary: 'Get customer portal URL',
    description: 'Get Dodo-hosted billing portal URL for managing subscription',
  })
  @ApiQuery({
    name: 'organizationId',
    required: true,
    description: 'Organization ID',
    type: String,
  })
  @ApiResponse({
    status: 200,
    description: 'Portal URL retrieved successfully',
  })
  @ApiResponse({
    status: 403,
    description: 'Forbidden - Only owners and admins can access billing',
  })
  async getCustomerPortal(
    @Query('organizationId', RequiredUUIDPipe) organizationId: string,
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User ID is required');
    }

    const portalUrl = await this.billingService.getCustomerPortalUrl(organizationId, userId);
    return createSuccessResponse({ url: portalUrl }, 'Portal URL retrieved successfully');
  }

  /**
   * Cancel subscription
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
        organizationId: {
          type: 'string',
          description: 'Organization ID',
        },
        cancelImmediately: {
          type: 'boolean',
          description: 'Cancel immediately or at period end',
          default: false,
        },
      },
      required: ['organizationId'],
    },
  })
  @ApiResponse({
    status: 200,
    description: 'Subscription cancelled successfully',
  })
  async cancelSubscription(
    @Body() body: { organizationId: string; cancelImmediately?: boolean },
    @Request() req: ExpressRequestType,
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User ID is required');
    }

    await this.billingService.cancelSubscription(
      body.organizationId,
      userId,
      body.cancelImmediately || false,
    );

    return createSuccessResponse(null, 'Subscription cancelled successfully');
  }

  /**
   * Dodo Payments webhook endpoint
   * This endpoint should NOT use SupabaseAuthGuard
   * It uses Dodo webhook signature verification instead
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
    const signature = req.headers['x-dodo-signature'] as string;

    if (!signature) {
      throw new Error('Dodo signature header is missing');
    }

    // Get raw body for signature verification
    const rawBody = (req as any).rawBody || JSON.stringify(req.body);

    try {
      await this.billingService.handleWebhookEvent(rawBody, signature);
      return { received: true };
    } catch (error) {
      console.error('Webhook processing failed:', error);
      throw error;
    }
  }
}
