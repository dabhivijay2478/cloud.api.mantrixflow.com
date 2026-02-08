/**
 * Destination Schema Controller
 * REST API endpoints for pipeline destination schema management
 */

import {
  BadRequestException,
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
  Query,
  Request,
  UseGuards,
  UsePipes,
  ValidationPipe,
} from '@nestjs/common';
import type { Request as ExpressRequest } from 'express';
import {
  ApiBearerAuth,
  ApiBody,
  ApiOperation,
  ApiParam,
  ApiQuery,
  ApiResponse,
  ApiTags,
} from '@nestjs/swagger';
import {
  createDeleteResponse,
  createListResponse,
  createSuccessResponse,
} from '../../common/dto/api-response.dto';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';
import { RequiredUUIDPipe } from '../activity-logs/pipes/required-uuid.pipe';
import { DestinationSchemaService } from './services/destination-schema.service';
import { CreateDestinationSchemaDto, UpdateDestinationSchemaDto } from './dto';

type ExpressRequestType = ExpressRequest;

@ApiTags('pipeline-destination-schemas')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard)
@Controller('organizations/:organizationId/pipeline-destination-schemas')
@UsePipes(new ValidationPipe({ transform: true, whitelist: true }))
export class DestinationSchemaController {
  private readonly logger = new Logger(DestinationSchemaController.name);

  constructor(private readonly destinationSchemaService: DestinationSchemaService) {}

  /**
   * Create a new destination schema
   */
  @Post()
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: 'Create destination schema',
    description: 'Create a new destination schema definition for a pipeline.',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization UUID' })
  @ApiBody({ type: CreateDestinationSchemaDto })
  @ApiResponse({ status: 201, description: 'Destination schema created successfully' })
  @ApiResponse({ status: 400, description: 'Invalid input' })
  async createDestinationSchema(
    @Param('organizationId', RequiredUUIDPipe) organizationId: string,
    @Body() dto: CreateDestinationSchemaDto,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);

      const schema = await this.destinationSchemaService.create({ ...dto, organizationId }, userId);

      return createSuccessResponse(
        schema,
        'Destination schema created successfully',
        HttpStatus.CREATED,
      );
    } catch (error) {
      this.handleError('create destination schema', error);
    }
  }

  /**
   * List all destination schemas for organization
   */
  @Get()
  @ApiOperation({
    summary: 'List destination schemas',
    description: 'Get all destination schemas for the organization with pagination.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiQuery({ name: 'limit', required: false, type: Number, description: 'Items per page (default: 20, max: 100)' })
  @ApiQuery({ name: 'offset', required: false, type: Number, description: 'Items to skip (default: 0)' })
  @ApiResponse({ status: 200, description: 'List of destination schemas' })
  async listDestinationSchemas(
    @Param('organizationId', RequiredUUIDPipe) organizationId: string,
    @Request() req: ExpressRequestType,
    @Query('limit') limit?: string,
    @Query('offset') offset?: string,
  ) {
    try {
      const userId = this.extractUserId(req);
      const limitNum = Math.min(Math.max(parseInt(limit || '20', 10) || 20, 1), 100);
      const offsetNum = Math.max(parseInt(offset || '0', 10) || 0, 0);

      const result = await this.destinationSchemaService.findByOrganizationPaginated(
        organizationId,
        userId,
        limitNum,
        offsetNum,
      );

      return createListResponse(result.data, `Found ${result.total} destination schema(s)`, {
        total: result.total,
        limit: limitNum,
        offset: offsetNum,
        hasMore: offsetNum + limitNum < result.total,
      });
    } catch (error) {
      this.handleError('list destination schemas', error);
    }
  }

  /**
   * Get destination schema by ID
   */
  @Get(':id')
  @ApiOperation({
    summary: 'Get destination schema',
    description: 'Retrieve a specific destination schema.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string', description: 'Destination schema UUID' })
  @ApiResponse({ status: 200, description: 'Destination schema details' })
  @ApiResponse({ status: 404, description: 'Destination schema not found' })
  async getDestinationSchema(
    @Param('organizationId', RequiredUUIDPipe) organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      const schema = await this.destinationSchemaService.findById(id, organizationId, userId);

      if (!schema) {
        throw new NotFoundException(`Destination schema ${id} not found`);
      }

      return createSuccessResponse(schema, 'Destination schema retrieved successfully');
    } catch (error) {
      this.handleError('get destination schema', error);
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
  @ApiBody({ type: UpdateDestinationSchemaDto })
  @ApiResponse({ status: 200, description: 'Destination schema updated successfully' })
  async updateDestinationSchema(
    @Param('organizationId', RequiredUUIDPipe) _organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Body() updates: UpdateDestinationSchemaDto,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      const updated = await this.destinationSchemaService.update(id, updates, userId);

      return createSuccessResponse(updated, 'Destination schema updated successfully');
    } catch (error) {
      this.handleError('update destination schema', error);
    }
  }

  /**
   * Validate destination schema against database
   */
  @Post(':id/validate')
  @ApiOperation({
    summary: 'Validate destination schema',
    description: 'Validate the destination schema against the actual database.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Validation result' })
  async validateDestinationSchema(
    @Param('organizationId', RequiredUUIDPipe) _organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      const result = await this.destinationSchemaService.validateSchema(id, userId);

      return createSuccessResponse(
        result,
        result.valid ? 'Destination schema is valid' : 'Destination schema has errors',
      );
    } catch (error) {
      this.handleError('validate destination schema', error);
    }
  }

  /**
   * Check if destination table exists
   */
  @Get(':id/table-exists')
  @ApiOperation({
    summary: 'Check table exists',
    description: 'Check if the destination table already exists in the database.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Table existence status' })
  async checkTableExists(
    @Param('organizationId', RequiredUUIDPipe) _organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      const exists = await this.destinationSchemaService.checkTableExists(id, userId);

      return createSuccessResponse(
        { exists },
        exists ? 'Destination table exists' : 'Destination table does not exist',
      );
    } catch (error) {
      this.handleError('check table exists', error);
    }
  }

  /**
   * Create destination table
   */
  @Post(':id/create-table')
  @ApiOperation({
    summary: 'Create destination table',
    description: 'Create the destination table based on column mappings.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Table creation result' })
  async createTable(
    @Param('organizationId', RequiredUUIDPipe) _organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      const result = await this.destinationSchemaService.createTable(id, userId);

      return createSuccessResponse(
        result,
        result.created ? `Table ${result.tableName} created successfully` : 'Table already exists',
      );
    } catch (error) {
      this.handleError('create table', error);
    }
  }

  /**
   * Validate configuration
   */
  @Post(':id/validate-config')
  @ApiOperation({
    summary: 'Validate configuration',
    description: 'Validate the destination schema configuration without querying the database.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Configuration validation result' })
  async validateConfiguration(
    @Param('organizationId', RequiredUUIDPipe) _organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      const result = await this.destinationSchemaService.validateConfiguration(id, userId);

      return createSuccessResponse(
        result,
        result.valid ? 'Configuration is valid' : 'Configuration has errors',
      );
    } catch (error) {
      this.handleError('validate configuration', error);
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
    @Param('organizationId', RequiredUUIDPipe) _organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      await this.destinationSchemaService.delete(id, userId);

      return createDeleteResponse(id, 'Destination schema deleted successfully');
    } catch (error) {
      this.handleError('delete destination schema', error);
    }
  }

  // ============================================================================
  // HELPER METHODS
  // ============================================================================

  private extractUserId(req: ExpressRequestType): string {
    const userId = req.user?.id;
    if (!userId) {
      throw new BadRequestException('User ID is required');
    }
    return userId;
  }

  private handleError(operation: string, error: unknown): never {
    const message = error instanceof Error ? error.message : String(error);
    this.logger.error(`Failed to ${operation}: ${message}`);

    if (error instanceof HttpException) {
      throw error;
    }

    throw new HttpException({ success: false, error: message }, HttpStatus.INTERNAL_SERVER_ERROR);
  }
}
