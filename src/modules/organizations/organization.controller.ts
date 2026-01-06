/**
 * Organization Controller
 * REST API endpoints for organization management
 */

import {
  Body,
  Controller,
  Delete,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  Patch,
  Post,
  Request,
} from '@nestjs/common';
// Type declarations are imported via tsconfig
import type { Request as ExpressRequest } from 'express';

type ExpressRequestType = ExpressRequest;

import { UseGuards } from '@nestjs/common';
import {
  ApiBearerAuth,
  ApiBody,
  ApiOperation,
  ApiParam,
  ApiResponse,
  ApiTags,
} from '@nestjs/swagger';
import {
  createDeleteResponse,
  createListResponse,
  createSuccessResponse,
} from '../../common/dto/api-response.dto';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import { CreateOrganizationDto } from './dto/create-organization.dto';
import { UpdateOrganizationDto } from './dto/update-organization.dto';
import { OrganizationService } from './organization.service';

@ApiTags('organizations')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard)
@Controller('api/organizations')
export class OrganizationController {
  constructor(private readonly organizationService: OrganizationService) {}

  /**
   * Create organization
   */
  @Post()
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: 'Create organization',
    description: 'Create a new organization',
  })
  @ApiBody({ type: CreateOrganizationDto })
  @ApiResponse({
    status: 201,
    description: 'Organization created successfully',
  })
  async createOrganization(@Body() dto: CreateOrganizationDto, @Request() req: ExpressRequestType) {
    const userId = req.user?.id || 'default-user-id';
    const organization = await this.organizationService.createOrganization(userId, dto);
    return createSuccessResponse(organization, 'Organization created successfully', 201);
  }

  /**
   * List organizations
   */
  @Get()
  @ApiOperation({
    summary: 'List organizations',
    description: 'Get all organizations',
  })
  @ApiResponse({
    status: 200,
    description: 'Organizations retrieved successfully',
  })
  async listOrganizations(@Request() req: ExpressRequestType) {
    const userId = req.user?.id;
    if (!userId) {
      return createListResponse([], 'Organizations retrieved successfully');
    }
    const organizations = await this.organizationService.listOrganizations(userId);
    return createListResponse(organizations, 'Organizations retrieved successfully');
  }

  /**
   * Get current organization
   */
  @Get('current')
  @ApiOperation({
    summary: 'Get current organization',
    description: 'Get the currently active organization',
  })
  @ApiResponse({
    status: 200,
    description: 'Current organization retrieved successfully',
  })
  @ApiResponse({
    status: 404,
    description: 'No current organization set',
  })
  async getCurrentOrganization(@Request() req: ExpressRequestType) {
    const userId = req.user?.id;
    if (!userId) {
      return createSuccessResponse(null, 'No current organization set', 200);
    }
    const organization = await this.organizationService.getCurrentOrganization(userId);
    if (!organization) {
      return createSuccessResponse(null, 'No current organization set', 200);
    }
    return createSuccessResponse(organization, 'Current organization retrieved successfully');
  }

  /**
   * Get organization by ID
   */
  @Get(':id')
  @ApiOperation({
    summary: 'Get organization',
    description: 'Get organization by ID',
  })
  @ApiParam({ name: 'id', description: 'Organization ID' })
  @ApiResponse({
    status: 200,
    description: 'Organization retrieved successfully',
  })
  @ApiResponse({
    status: 404,
    description: 'Organization not found',
  })
  async getOrganization(@Param('id') id: string) {
    const organization = await this.organizationService.getOrganization(id);
    return createSuccessResponse(organization, 'Organization retrieved successfully');
  }

  /**
   * Set current organization
   */
  @Post(':id/set-current')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: 'Set current organization',
    description: 'Set the active organization',
  })
  @ApiParam({ name: 'id', description: 'Organization ID' })
  @ApiResponse({
    status: 200,
    description: 'Current organization set successfully',
  })
  async setCurrentOrganization(@Param('id') id: string, @Request() req: ExpressRequestType) {
    const userId = req.user?.id || 'default-user-id';
    const organization = await this.organizationService.setCurrentOrganization(userId, id);
    return createSuccessResponse(organization, 'Current organization set successfully');
  }

  /**
   * Update organization
   */
  @Patch(':id')
  @ApiOperation({
    summary: 'Update organization',
    description: 'Update organization details',
  })
  @ApiParam({ name: 'id', description: 'Organization ID' })
  @ApiBody({ type: UpdateOrganizationDto })
  @ApiResponse({
    status: 200,
    description: 'Organization updated successfully',
  })
  @ApiResponse({
    status: 404,
    description: 'Organization not found',
  })
  async updateOrganization(@Param('id') id: string, @Body() dto: UpdateOrganizationDto) {
    const organization = await this.organizationService.updateOrganization(id, dto);
    return createSuccessResponse(organization, 'Organization updated successfully');
  }

  /**
   * Delete organization
   */
  @Delete(':id')
  @ApiOperation({
    summary: 'Delete organization',
    description: 'Delete an organization',
  })
  @ApiParam({ name: 'id', description: 'Organization ID' })
  @ApiResponse({
    status: 200,
    description: 'Organization deleted successfully',
  })
  @ApiResponse({
    status: 404,
    description: 'Organization not found',
  })
  async deleteOrganization(@Param('id') id: string) {
    await this.organizationService.deleteOrganization(id);
    return createDeleteResponse(id, 'Organization deleted successfully');
  }
}
