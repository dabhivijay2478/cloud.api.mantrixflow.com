/**
 * Organization Controller
 * REST API endpoints for organization management
 */

import {
  Controller,
  Post,
  Get,
  Patch,
  Delete,
  Body,
  Param,
  HttpCode,
  HttpStatus,
  Request,
} from '@nestjs/common';
// Type declarations are imported via tsconfig
import type { Request as ExpressRequest } from 'express';
type Request = ExpressRequest;
import {
  ApiTags,
  ApiOperation,
  ApiResponse,
  ApiParam,
  ApiBody,
  ApiBearerAuth,
} from '@nestjs/swagger';
import { UseGuards } from '@nestjs/common';
import { OrganizationService } from './organization.service';
import { CreateOrganizationDto } from './dto/create-organization.dto';
import { UpdateOrganizationDto } from './dto/update-organization.dto';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import {
  ApiSuccessResponse,
  ApiListResponse,
  ApiDeleteResponse,
  createSuccessResponse,
  createListResponse,
  createDeleteResponse,
} from '../../common/dto/api-response.dto';

@ApiTags('organizations')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard)
@Controller('api/organizations')
export class OrganizationController {
  constructor(
    private readonly organizationService: OrganizationService,
  ) {}

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
  async createOrganization(
    @Body() dto: CreateOrganizationDto,
    @Request() req: Request,
  ) {
    const userId = req.user?.id || 'default-user-id';
    const organization = await this.organizationService.createOrganization(
      userId,
      dto,
    );
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
  async listOrganizations() {
    const organizations = await this.organizationService.listOrganizations();
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
  async getCurrentOrganization(@Request() req: Request) {
    const userId = req.user?.id;
    const organization = await this.organizationService.getCurrentOrganization();
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
  async setCurrentOrganization(
    @Param('id') id: string,
    @Request() req: Request,
  ) {
    const userId = req.user?.id || 'default-user-id';
    const organization = await this.organizationService.setCurrentOrganization(
      userId,
      id,
    );
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
  async updateOrganization(
    @Param('id') id: string,
    @Body() dto: UpdateOrganizationDto,
  ) {
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
