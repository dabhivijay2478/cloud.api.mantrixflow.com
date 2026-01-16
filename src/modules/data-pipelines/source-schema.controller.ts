/**
 * Source Schema Controller
 * REST API endpoints for pipeline source schema management
 */

import {
  Body,
  Controller,
  Delete,
  Get,
  HttpCode,
  HttpException,
  HttpStatus,
  Logger,
  NotFoundException,
  Param,
  Patch,
  Post,
  Request,
  UseGuards,
} from '@nestjs/common';
import type { Request as ExpressRequest } from 'express';
import { ApiBearerAuth, ApiOperation, ApiParam, ApiResponse, ApiTags } from '@nestjs/swagger';
import {
  createDeleteResponse,
  createListResponse,
  createSuccessResponse,
} from '../../common/dto/api-response.dto';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import { SourceSchemaService } from './services/source-schema.service';
import type {
  CreateSourceSchemaDto,
  UpdateSourceSchemaDto,
} from './services/source-schema.service';

type ExpressRequestType = ExpressRequest;

@ApiTags('source-schemas')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard)
@Controller('organizations/:organizationId/source-schemas')
export class SourceSchemaController {
  private readonly logger = new Logger(SourceSchemaController.name);

  constructor(private readonly sourceSchemaService: SourceSchemaService) {}

  /**
   * Create source schema
   */
  @Post()
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: 'Create source schema',
    description: 'Create a new source schema configuration.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiResponse({ status: 201, description: 'Source schema created successfully' })
  async createSourceSchema(
    @Param('organizationId') organizationId: string,
    @Body() dto: Omit<CreateSourceSchemaDto, 'organizationId'>,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = req.user?.id;
      if (!userId) {
        throw new Error('User not authenticated');
      }

      const schema = await this.sourceSchemaService.create({ ...dto, organizationId }, userId);

      return createSuccessResponse(
        schema,
        'Source schema created successfully',
        HttpStatus.CREATED,
      );
    } catch (error) {
      this.logger.error(
        `Failed to create source schema: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to create source schema',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
    }
  }

  /**
   * List source schemas
   */
  @Get()
  @ApiOperation({
    summary: 'List source schemas',
    description: 'Get all source schemas for the organization.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiResponse({ status: 200, description: 'List of source schemas' })
  async listSourceSchemas(@Param('organizationId') organizationId: string) {
    try {
      const schemas = await this.sourceSchemaService.findByOrganization(organizationId);

      return createListResponse(schemas, `Found ${schemas.length} source schema(s)`, {
        total: schemas.length,
        limit: schemas.length,
        offset: 0,
        hasMore: false,
      });
    } catch (error) {
      this.logger.error(
        `Failed to list source schemas: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw new HttpException(
        {
          success: false,
          error: error instanceof Error ? error.message : 'Failed to list source schemas',
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get source schema by ID
   */
  @Get(':id')
  @ApiOperation({
    summary: 'Get source schema',
    description: 'Get source schema details.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Source schema details' })
  async getSourceSchema(@Param('organizationId') _organizationId: string, @Param('id') id: string) {
    try {
      const schema = await this.sourceSchemaService.findById(id);

      if (!schema) {
        throw new NotFoundException(`Source schema ${id} not found`);
      }

      return createSuccessResponse(schema, 'Source schema retrieved successfully');
    } catch (error) {
      this.logger.error(
        `Failed to get source schema: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to get source schema',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
    }
  }

  /**
   * Update source schema
   */
  @Patch(':id')
  @ApiOperation({
    summary: 'Update source schema',
    description: 'Update source schema configuration.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Source schema updated successfully' })
  async updateSourceSchema(
    @Param('organizationId') _organizationId: string,
    @Param('id') id: string,
    @Body() updates: UpdateSourceSchemaDto,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = req.user?.id;
      if (!userId) {
        throw new Error('User not authenticated');
      }

      const updated = await this.sourceSchemaService.update(id, updates, userId);

      return createSuccessResponse(updated, 'Source schema updated successfully');
    } catch (error) {
      this.logger.error(
        `Failed to update source schema: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to update source schema',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
    }
  }

  /**
   * Discover source schema
   */
  @Post(':id/discover')
  @ApiOperation({
    summary: 'Discover source schema',
    description: 'Discover columns and structure from source.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Source schema discovered' })
  async discoverSourceSchema(
    @Param('organizationId') _organizationId: string,
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = req.user?.id;
      if (!userId) {
        throw new Error('User not authenticated');
      }

      const schema = await this.sourceSchemaService.discoverSchema(id, userId);

      return createSuccessResponse(schema, 'Source schema discovered successfully');
    } catch (error) {
      this.logger.error(
        `Failed to discover source schema: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to discover source schema',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
    }
  }

  /**
   * Delete source schema
   */
  @Delete(':id')
  @ApiOperation({
    summary: 'Delete source schema',
    description: 'Delete a source schema (soft delete).',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Source schema deleted successfully' })
  async deleteSourceSchema(
    @Param('organizationId') _organizationId: string,
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = req.user?.id;
      if (!userId) {
        throw new Error('User not authenticated');
      }

      await this.sourceSchemaService.delete(id, userId);

      return createDeleteResponse(id, 'Source schema deleted successfully');
    } catch (error) {
      this.logger.error(
        `Failed to delete source schema: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to delete source schema',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
    }
  }
}
