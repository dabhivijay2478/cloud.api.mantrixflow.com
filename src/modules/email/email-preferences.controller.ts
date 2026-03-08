/**
 * Email Preferences Controller
 * User-facing endpoints for managing email notification preferences
 * Routes: GET/PATCH /api/email/preferences
 */

import { Body, Controller, Get, Patch, Request, UseGuards } from '@nestjs/common';
import type { Request as ExpressRequest } from 'express';

import {
  ApiBearerAuth,
  ApiBody,
  ApiOperation,
  ApiResponse,
  ApiTags,
} from '@nestjs/swagger';
import { createSuccessResponse } from '../../common/dto/api-response.dto';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import { EmailRepository } from './repositories/email-repository';

type ExpressRequestType = ExpressRequest;

@ApiTags('email')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard)
@Controller('email')
export class EmailPreferencesController {
  constructor(private readonly emailRepository: EmailRepository) {}

  @Get('preferences')
  @ApiOperation({ summary: 'Get email preferences' })
  @ApiResponse({ status: 200, description: 'Preferences retrieved successfully' })
  async getPreferences(@Request() req: ExpressRequestType) {
    const userId = req.user?.id;
    if (!userId) throw new Error('User not authenticated');
    const prefs = await this.emailRepository.getPreferences(userId);
    return createSuccessResponse(prefs, 'Preferences retrieved successfully');
  }

  @Patch('preferences')
  @ApiOperation({ summary: 'Update email preferences' })
  @ApiBody({
    schema: {
      type: 'object',
      properties: {
        weeklyDigestEnabled: { type: 'boolean' },
        pipelineFailureEmails: { type: 'boolean' },
        marketingEmails: { type: 'boolean' },
      },
    },
  })
  @ApiResponse({ status: 200, description: 'Preferences updated successfully' })
  async updatePreferences(
    @Request() req: ExpressRequestType,
    @Body() data: { weeklyDigestEnabled?: boolean; pipelineFailureEmails?: boolean; marketingEmails?: boolean },
  ) {
    const userId = req.user?.id;
    if (!userId) throw new Error('User not authenticated');
    await this.emailRepository.upsertPreferences(userId, data);
    const prefs = await this.emailRepository.getPreferences(userId);
    return createSuccessResponse(prefs, 'Preferences updated successfully');
  }
}
