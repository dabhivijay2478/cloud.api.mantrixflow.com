/**
 * User Controller
 * REST API endpoints for user management
 */

import { Body, Controller, Get, Param, Patch, Post, Request, UseGuards } from '@nestjs/common';
// Type declarations are imported via tsconfig
import type { Request as ExpressRequest } from 'express';

type ExpressRequestType = ExpressRequest;

import {
  ApiBearerAuth,
  ApiBody,
  ApiOperation,
  ApiParam,
  ApiResponse,
  ApiTags,
} from '@nestjs/swagger';
import { createSuccessResponse } from '../../common/dto/api-response.dto';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import { CreateUserDto } from './dto/create-user.dto';
import { UserService } from './user.service';

@ApiTags('users')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard)
@Controller('users')
export class UserController {
  constructor(private readonly userService: UserService) {}

  /**
   * Create or sync user from Supabase
   */
  @Post('sync')
  @ApiOperation({
    summary: 'Sync user from Supabase',
    description: 'Create or update user from Supabase auth data',
  })
  @ApiBody({ type: CreateUserDto })
  @ApiResponse({
    status: 200,
    description: 'User synced successfully',
  })
  async syncUser(@Body() dto: CreateUserDto) {
    const result = await this.userService.createOrUpdateFromSupabase({
      id: dto.supabaseUserId,
      email: dto.email,
      email_confirmed_at: null,
      user_metadata: {
        first_name: dto.firstName,
        last_name: dto.lastName,
        full_name: dto.fullName,
        avatar_url: dto.avatarUrl,
        ...dto.metadata,
      },
    });
    return createSuccessResponse(
      result.user,
      result.created ? 'User created successfully' : 'User updated successfully',
    );
  }

  /**
   * Get current user
   */
  @Get('me')
  @ApiOperation({
    summary: 'Get current user',
    description: 'Get the currently authenticated user',
  })
  @ApiResponse({
    status: 200,
    description: 'User retrieved successfully',
  })
  async getCurrentUser(@Request() req: ExpressRequestType) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }
    const user = await this.userService.getUserById(userId);
    return createSuccessResponse(user, 'User retrieved successfully');
  }

  /**
   * Get user by ID
   */
  @Get(':id')
  @ApiOperation({
    summary: 'Get user',
    description: 'Get user by ID',
  })
  @ApiParam({ name: 'id', description: 'User ID' })
  @ApiResponse({
    status: 200,
    description: 'User retrieved successfully',
  })
  async getUser(@Param('id') id: string) {
    const user = await this.userService.getUserById(id);
    return createSuccessResponse(user, 'User retrieved successfully');
  }

  /**
   * Update user
   */
  @Patch('me')
  @ApiOperation({
    summary: 'Update current user',
    description: 'Update the currently authenticated user',
  })
  @ApiResponse({
    status: 200,
    description: 'User updated successfully',
  })
  async updateCurrentUser(
    @Request() req: ExpressRequestType,
    @Body() data: {
      firstName?: string;
      lastName?: string;
      fullName?: string;
      avatarUrl?: string;
      metadata?: Record<string, unknown>;
    },
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }
    const user = await this.userService.updateUser(userId, data);
    return createSuccessResponse(user, 'User updated successfully');
  }

  /**
   * Update onboarding status
   */
  @Patch('me/onboarding')
  @ApiOperation({
    summary: 'Update onboarding status',
    description: 'Update user onboarding completion status',
  })
  @ApiResponse({
    status: 200,
    description: 'Onboarding status updated successfully',
  })
  async updateOnboarding(
    @Request() req: ExpressRequestType,
    @Body() data: { completed: boolean; step?: string },
  ) {
    const userId = req.user?.id;
    if (!userId) {
      throw new Error('User not authenticated');
    }
    const user = await this.userService.updateOnboarding(userId, data.completed, data.step);
    return createSuccessResponse(user, 'Onboarding status updated successfully');
  }

}
