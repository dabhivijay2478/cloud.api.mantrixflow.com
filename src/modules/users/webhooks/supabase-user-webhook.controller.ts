/**
 * Supabase User Webhook Controller
 * Handles automatic user creation after Supabase auth events
 */

import {
  Body,
  Controller,
  Headers,
  HttpCode,
  HttpStatus,
  Post,
  UnauthorizedException,
} from '@nestjs/common';
import { ApiOperation, ApiResponse, ApiTags } from '@nestjs/swagger';
import { createSuccessResponse } from '../../../common/dto/api-response.dto';
import { UserService } from '../user.service';

@ApiTags('webhooks')
@Controller('api/webhooks/supabase')
export class SupabaseUserWebhookController {
  constructor(private readonly userService: UserService) {}

  /**
   * Handle Supabase auth webhook
   * This endpoint should be called by Supabase when a user signs up or confirms email
   */
  @Post('user')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Supabase user webhook',
    description: 'Handle user creation/update from Supabase auth events',
  })
  @ApiResponse({
    status: 200,
    description: 'User synced successfully',
  })
  async handleSupabaseUser(
    @Body() body: {
      type: 'INSERT' | 'UPDATE';
      table: string;
      record: {
        id: string;
        email: string;
        email_confirmed_at?: string | null;
        user_metadata?: Record<string, unknown>;
        app_metadata?: Record<string, unknown>;
      };
    },
    @Headers('authorization') authHeader?: string,
  ) {
    // Verify webhook secret (optional but recommended)
    const webhookSecret = process.env.SUPABASE_WEBHOOK_SECRET;
    if (webhookSecret && authHeader !== `Bearer ${webhookSecret}`) {
      throw new UnauthorizedException('Invalid webhook secret');
    }

    // Only process auth.users table events
    if (body.table !== 'auth.users') {
      return createSuccessResponse(null, 'Event ignored');
    }

    if (body.type === 'INSERT' || body.type === 'UPDATE') {
      const result = await this.userService.createOrUpdateFromSupabase(body.record);
      return createSuccessResponse(
        result.user,
        result.created ? 'User created successfully' : 'User updated successfully',
      );
    }

    return createSuccessResponse(null, 'Event processed');
  }
}
