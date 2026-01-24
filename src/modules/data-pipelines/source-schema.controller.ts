/**
 * Source Schema Controller
 * REST API endpoints for pipeline source schema management
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
import { SourceSchemaService } from './services/source-schema.service';
import { CreateSourceSchemaDto, UpdateSourceSchemaDto } from './dto';

type ExpressRequestType = ExpressRequest;

@ApiTags('pipeline-source-schemas')
@ApiBearerAuth('JWT-auth')
@UseGuards(SupabaseAuthGuard)
@Controller('organizations/:organizationId/pipeline-source-schemas')
@UsePipes(new ValidationPipe({ transform: true, whitelist: true }))
export class SourceSchemaController {
  private readonly logger = new Logger(SourceSchemaController.name);

  constructor(private readonly sourceSchemaService: SourceSchemaService) {}

  /**
   * Create a new source schema
   */
  @Post()
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: 'Create source schema',
    description: 'Create a new source schema definition for a pipeline.',
  })
  @ApiParam({ name: 'organizationId', type: 'string', description: 'Organization UUID' })
  @ApiBody({ type: CreateSourceSchemaDto })
  @ApiResponse({ status: 201, description: 'Source schema created successfully' })
  @ApiResponse({ status: 400, description: 'Invalid input' })
  async createSourceSchema(
    @Param('organizationId', RequiredUUIDPipe) organizationId: string,
    @Body() dto: CreateSourceSchemaDto,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);

      const schema = await this.sourceSchemaService.create({ ...dto, organizationId }, userId);

      return createSuccessResponse(
        schema,
        'Source schema created successfully',
        HttpStatus.CREATED,
      );
    } catch (error) {
      this.handleError('create source schema', error);
    }
  }

  /**
   * List all source schemas for organization
   */
  @Get()
  @ApiOperation({
    summary: 'List source schemas',
    description: 'Get all source schemas for the organization.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiResponse({ status: 200, description: 'List of source schemas' })
  async listSourceSchemas(
    @Param('organizationId', RequiredUUIDPipe) organizationId: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      const schemas = await this.sourceSchemaService.findByOrganization(organizationId, userId);

      return createListResponse(schemas, `Found ${schemas.length} source schema(s)`, {
        total: schemas.length,
        limit: schemas.length,
        offset: 0,
        hasMore: false,
      });
    } catch (error) {
      this.handleError('list source schemas', error);
    }
  }

  /**
   * Get source schema by ID
   */
  @Get(':id')
  @ApiOperation({
    summary: 'Get source schema',
    description: 'Retrieve a specific source schema.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string', description: 'Source schema UUID' })
  @ApiResponse({ status: 200, description: 'Source schema details' })
  @ApiResponse({ status: 404, description: 'Source schema not found' })
  async getSourceSchema(
    @Param('organizationId', RequiredUUIDPipe) organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      const schema = await this.sourceSchemaService.findById(id, organizationId, userId);

      if (!schema) {
        throw new NotFoundException(`Source schema ${id} not found`);
      }

      return createSuccessResponse(schema, 'Source schema retrieved successfully');
    } catch (error) {
      this.handleError('get source schema', error);
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
  @ApiBody({ type: UpdateSourceSchemaDto })
  @ApiResponse({ status: 200, description: 'Source schema updated successfully' })
  async updateSourceSchema(
    @Param('organizationId', RequiredUUIDPipe) _organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Body() updates: UpdateSourceSchemaDto,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      const updated = await this.sourceSchemaService.update(id, updates, userId);

      return createSuccessResponse(updated, 'Source schema updated successfully');
    } catch (error) {
      this.handleError('update source schema', error);
    }
  }

  /**
   * Validate source schema
   */
  @Post(':id/validate')
  @ApiOperation({
    summary: 'Validate source schema',
    description: 'Validate the source schema configuration.',
  })
  @ApiParam({ name: 'organizationId', type: 'string' })
  @ApiParam({ name: 'id', type: 'string' })
  @ApiResponse({ status: 200, description: 'Validation result' })
  async validateSourceSchema(
    @Param('organizationId', RequiredUUIDPipe) _organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      const result = await this.sourceSchemaService.validateSchema(id, userId);

      return createSuccessResponse(
        result,
        result.valid ? 'Source schema is valid' : 'Source schema has errors',
      );
    } catch (error) {
      this.handleError('validate source schema', error);
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
    @Param('organizationId', RequiredUUIDPipe) _organizationId: string,
    @Param('id', RequiredUUIDPipe) id: string,
    @Request() req: ExpressRequestType,
  ) {
    try {
      const userId = this.extractUserId(req);
      await this.sourceSchemaService.delete(id, userId);

      return createDeleteResponse(id, 'Source schema deleted successfully');
    } catch (error) {
      this.handleError('delete source schema', error);
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
