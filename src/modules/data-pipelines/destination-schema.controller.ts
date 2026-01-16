/**
 * Destination Schema Controller
 * REST API endpoints for pipeline destination schema management
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
import { DestinationSchemaService } from './services/destination-schema.service';
import type {
  CreateDestinationSchemaDto,
  UpdateDestinationSchemaDto,
} from './services/destination-schema.service';

type ExpressRequestType = ExpressRequest;

@ApiTags('destination-schemas')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard)
@Controller('organizations/:organizationId/destination-schemas')
export class DestinationSchemaController {
  private readonly logger = new Logger(DestinationSchemaController.name);

  constructor(private readonly destinationSchemaService: DestinationSchemaService) {}

  /**
   * Create destination schema
   */
  @Post()
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: 'Create destination schema',
    description: 'Create a new destination schema configuration.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiResponse({ status: 201, description: 'Destination schema created successfully' })
  async createDestinationSchema(
    @Param('organizationId') organizationId: string,
    @Body() dto: Omit<CreateDestinationSchemaDto, 'organizationId'>,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = req.user?.id;
      if (!userId) {
        throw new Error('User not authenticated');
      }

      const schema = await this.destinationSchemaService.create({ ...dto, organizationId }, userId);

      return createSuccessResponse(
        schema,
        'Destination schema created successfully',
        HttpStatus.CREATED,
      );
    } catch (error) {
      this.logger.error(
        `Failed to create destination schema: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to create destination schema',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
    }
  }

  /**
   * List destination schemas
   */
  @Get()
  @ApiOperation({
    summary: 'List destination schemas',
    description: 'Get all destination schemas for the organization.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiResponse({ status: 200, description: 'List of destination schemas' })
  async listDestinationSchemas(@Param('organizationId') organizationId: string) {
    try {
      const schemas = await this.destinationSchemaService.findByOrganization(organizationId);

      return createListResponse(schemas, `Found ${schemas.length} destination schema(s)`, {
        total: schemas.length,
        limit: schemas.length,
        offset: 0,
        hasMore: false,
      });
    } catch (error) {
      this.logger.error(
        `Failed to list destination schemas: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw new HttpException(
        {
          success: false,
          error: error instanceof Error ? error.message : 'Failed to list destination schemas',
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  /**
   * Get destination schema by ID
   */
  @Get(':id')
  @ApiOperation({
    summary: 'Get destination schema',
    description: 'Get destination schema details.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Destination schema details' })
  async getDestinationSchema(
    @Param('organizationId') _organizationId: string,
    @Param('id') id: string,
  ) {
    try {
      const schema = await this.destinationSchemaService.findById(id);

      if (!schema) {
        throw new NotFoundException(`Destination schema ${id} not found`);
      }

      return createSuccessResponse(schema, 'Destination schema retrieved successfully');
    } catch (error) {
      this.logger.error(
        `Failed to get destination schema: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to get destination schema',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
    }
  }

  /**
   * Update destination schema
   */
  @Patch(':id')
  @ApiOperation({
    summary: 'Update destination schema',
    description: 'Update destination schema configuration.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Destination schema updated successfully' })
  async updateDestinationSchema(
    @Param('organizationId') _organizationId: string,
    @Param('id') id: string,
    @Body() updates: UpdateDestinationSchemaDto,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = req.user?.id;
      if (!userId) {
        throw new Error('User not authenticated');
      }

      const updated = await this.destinationSchemaService.update(id, updates, userId);

      return createSuccessResponse(updated, 'Destination schema updated successfully');
    } catch (error) {
      this.logger.error(
        `Failed to update destination schema: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to update destination schema',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
    }
  }

  /**
   * Validate destination schema
   */
  @Post(':id/validate')
  @ApiOperation({
    summary: 'Validate destination schema',
    description: 'Validate destination schema against actual destination.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Destination schema validated' })
  async validateDestinationSchema(
    @Param('organizationId') _organizationId: string,
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = req.user?.id;
      if (!userId) {
        throw new Error('User not authenticated');
      }

      const result = await this.destinationSchemaService.validateSchema(id, userId);

      return createSuccessResponse(
        result,
        result.valid ? 'Destination schema is valid' : 'Destination schema has errors',
      );
    } catch (error) {
      this.logger.error(
        `Failed to validate destination schema: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error:
                error instanceof Error ? error.message : 'Failed to validate destination schema',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
    }
  }

  /**
   * Delete destination schema
   */
  @Delete(':id')
  @ApiOperation({
    summary: 'Delete destination schema',
    description: 'Delete a destination schema (soft delete).',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Destination schema deleted successfully' })
  async deleteDestinationSchema(
    @Param('organizationId') _organizationId: string,
    @Param('id') id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = req.user?.id;
      if (!userId) {
        throw new Error('User not authenticated');
      }

      await this.destinationSchemaService.delete(id, userId);

      return createDeleteResponse(id, 'Destination schema deleted successfully');
    } catch (error) {
      this.logger.error(
        `Failed to delete destination schema: ${error instanceof Error ? error.message : String(error)}`,
      );
      throw error instanceof HttpException
        ? error
        : new HttpException(
            {
              success: false,
              error: error instanceof Error ? error.message : 'Failed to delete destination schema',
            },
            HttpStatus.INTERNAL_SERVER_ERROR,
          );
    }
  }
}
