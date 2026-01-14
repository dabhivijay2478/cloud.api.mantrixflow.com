/**
 * Onboarding Controller
 * REST API endpoints for onboarding flow
 */

import { Body, Controller, Get, Patch, Post, Request, UseGuards } from '@nestjs/common';
// Type declarations are imported via tsconfig
import type { Request as ExpressRequest } from 'express';

type ExpressRequestType = ExpressRequest;

import { ApiBearerAuth, ApiBody, ApiOperation, ApiResponse, ApiTags } from '@nestjs/swagger';
import { createSuccessResponse } from '../../common/dto/api-response.dto';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import { OrganizationService } from '../organizations/organization.service';
import { UserService } from '../users/user.service';

@ApiTags('onboarding')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard)
@Controller('onboarding')
export class OnboardingController {
  constructor(
    private readonly userService: UserService,
    readonly _organizationService: OrganizationService,
  ) {}

  /**
   * Get onboarding status
   */
  @Get('status')
  @ApiOperation({
    summary: 'Get onboarding status',
    description: 'Get the current user onboarding status',
  })
  @ApiResponse({
    status: 200,
    description: 'Onboarding status retrieved successfully',
  })
  async getOnboardingStatus(@Request() req: ExpressRequestType) {
    const userId = req.user?.id;
    const userEmail = req.user?.email;

    if (!userId) {
      throw new Error('User not authenticated');
    }

    try {
      const user = await this.userService.getUserById(userId);
      return createSuccessResponse(
        {
          completed: user.onboardingCompleted,
          step: user.onboardingStep || 'welcome',
          currentOrgId: user.currentOrgId,
        },
        'Onboarding status retrieved successfully',
      );
    } catch (error) {
      // If user not found, try to create from Supabase auth data
      if (error instanceof Error && error.message.includes('not found')) {
        // User doesn't exist in database, create it from auth data
        const newUser = await this.userService.createOrUpdateFromSupabase({
          id: userId,
          email: userEmail || '',
          email_confirmed_at: null,
          user_metadata: {},
          app_metadata: {},
        });

        return createSuccessResponse(
          {
            completed: newUser.user.onboardingCompleted,
            step: newUser.user.onboardingStep || 'welcome',
            currentOrgId: newUser.user.currentOrgId,
          },
          'Onboarding status retrieved successfully',
        );
      }
      throw error;
    }
  }

  /**
   * Update onboarding step
   */
  @Patch('step')
  @ApiOperation({
    summary: 'Update onboarding step',
    description: 'Update the current onboarding step',
  })
  @ApiBody({
    schema: {
      type: 'object',
      properties: {
        step: {
          type: 'string',
          enum: [
            'welcome',
            'plans',
            'organization',
            'data-source',
            'connect',
            'select',
            'importing',
            'complete',
          ],
        },
      },
    },
  })
  @ApiResponse({
    status: 200,
    description: 'Onboarding step updated successfully',
  })
  async updateOnboardingStep(@Request() req: ExpressRequestType, @Body() data: { step: string }) {
    const userId = req.user?.id;
    const userEmail = req.user?.email;

    if (!userId) {
      throw new Error('User not authenticated');
    }

    // Ensure user exists in database
    try {
      await this.userService.getUserById(userId);
    } catch (error) {
      // If user not found, create it from Supabase auth data
      if (error instanceof Error && error.message.includes('not found')) {
        await this.userService.createOrUpdateFromSupabase({
          id: userId,
          email: userEmail || '',
          email_confirmed_at: null,
          user_metadata: {},
          app_metadata: {},
        });
      } else {
        throw error;
      }
    }

    const user = await this.userService.updateOnboarding(
      userId,
      data.step === 'complete',
      data.step,
    );
    return createSuccessResponse(user, 'Onboarding step updated successfully');
  }

  /**
   * Create organization after plan selection
   * This is called after user selects a plan and completes payment
   */
  @Post('create-org')
  @ApiOperation({
    summary: 'Create organization after plan selection',
    description:
      'Create organization and set user as owner (skip role validation for first-time users)',
  })
  @ApiBody({
    schema: {
      type: 'object',
      properties: {
        name: {
          type: 'string',
        },
      },
    },
  })
  @ApiResponse({
    status: 200,
    description: 'Organization created successfully',
  })
  async createOrg(@Request() req: ExpressRequestType, @Body() data: { name: string }) {
    const userId = req.user?.id;
    const userEmail = req.user?.email;

    if (!userId) {
      throw new Error('User not authenticated');
    }

    // Ensure user exists in database
    let user: Awaited<ReturnType<typeof this.userService.getUserById>>;
    try {
      user = await this.userService.getUserById(userId);
    } catch (error) {
      // If user not found, create it from Supabase auth data
      if (error instanceof Error && error.message.includes('not found')) {
        const newUser = await this.userService.createOrUpdateFromSupabase({
          id: userId,
          email: userEmail || '',
          email_confirmed_at: null,
          user_metadata: {},
          app_metadata: {},
        });
        user = newUser.user;
      } else {
        throw error;
      }
    }

    // Check if user already has an organization
    if (user.currentOrgId) {
      try {
        const existingOrg = await this._organizationService.getOrganization(user.currentOrgId);
        // Mark onboarding as completed if not already
        if (!user.onboardingCompleted) {
          await this.userService.updateOnboarding(userId, true, 'complete');
        }
        return createSuccessResponse(existingOrg, 'Organization already exists');
      } catch (_error) {
        // If org doesn't exist, continue with creation
        console.warn('Existing org ID found but org does not exist, creating new one');
      }
    }

    // Create organization with user as owner
    // Skip role validation for first-time users
    try {
      const organization = await this._organizationService.createOrganization(
        userId,
        { name: data.name },
        { skipRoleValidation: true }, // Skip validation for first-time users
      );

      // Set user's current org (the createOrganization already does this, but ensure it's set)
      await this.userService.setCurrentOrganization(userId, organization.id);

      // Mark onboarding as completed
      await this.userService.updateOnboarding(userId, true, 'complete');

      return createSuccessResponse(organization, 'Organization created successfully');
    } catch (error) {
      // Handle duplicate slug error gracefully
      if (
        error instanceof Error &&
        (error.message.includes('duplicate') || error.message.includes('already exists'))
      ) {
        // Try again with a slightly modified name
        const modifiedName = `${data.name} ${Date.now().toString().slice(-4)}`;
        const organization = await this._organizationService.createOrganization(
          userId,
          { name: modifiedName },
          { skipRoleValidation: true },
        );

        await this.userService.setCurrentOrganization(userId, organization.id);
        await this.userService.updateOnboarding(userId, true, 'complete');

        return createSuccessResponse(organization, 'Organization created successfully');
      }
      throw error;
    }
  }

  /**
   * Complete onboarding
   */
  @Post('complete')
  @ApiOperation({
    summary: 'Complete onboarding',
    description: 'Mark onboarding as completed',
  })
  @ApiResponse({
    status: 200,
    description: 'Onboarding completed successfully',
  })
  async completeOnboarding(@Request() req: ExpressRequestType) {
    const userId = req.user?.id;
    const userEmail = req.user?.email;

    if (!userId) {
      throw new Error('User not authenticated');
    }

    // Ensure user exists in database
    try {
      await this.userService.getUserById(userId);
    } catch (error) {
      // If user not found, create it from Supabase auth data
      if (error instanceof Error && error.message.includes('not found')) {
        await this.userService.createOrUpdateFromSupabase({
          id: userId,
          email: userEmail || '',
          email_confirmed_at: null,
          user_metadata: {},
          app_metadata: {},
        });
      } else {
        throw error;
      }
    }

    const user = await this.userService.updateOnboarding(userId, true, 'complete');
    return createSuccessResponse(user, 'Onboarding completed successfully');
  }
}
