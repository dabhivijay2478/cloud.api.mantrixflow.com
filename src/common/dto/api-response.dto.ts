/**
 * Common API Response DTOs
 * Reusable response wrappers for all CRUD operations across all connectors
 */

import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

/**
 * Standard API Response Metadata
 */
export class ApiResponseMetadata {
  @ApiProperty({
    description: 'Response status code',
    example: 200,
  })
  statusCode: number;

  @ApiProperty({
    description: 'Response message',
    example: 'Operation completed successfully',
  })
  message: string;

  @ApiProperty({
    description: 'Operation status',
    enum: ['success', 'error', 'warning'],
    example: 'success',
  })
  status: 'success' | 'error' | 'warning';

  @ApiProperty({
    description: 'Timestamp of the response',
    example: '2024-01-15T10:30:00.000Z',
  })
  timestamp: string;

  @ApiPropertyOptional({
    description: 'Request ID for tracking',
    example: 'req-123e4567-e89b-12d3-a456-426614174000',
  })
  requestId?: string;

  @ApiPropertyOptional({
    description: 'Additional metadata',
    type: Object,
    additionalProperties: true,
  })
  metadata?: Record<string, unknown>;
}

/**
 * Standard Success Response Wrapper
 */
export class ApiSuccessResponse<T = unknown> {
  @ApiProperty({
    description: 'Response metadata',
    type: ApiResponseMetadata,
  })
  meta: ApiResponseMetadata;

  @ApiProperty({
    description: 'Response data',
  })
  data: T;

  constructor(
    data: T,
    message: string = 'Operation completed successfully',
    statusCode: number = 200,
    metadata?: Record<string, unknown>,
  ) {
    this.meta = {
      statusCode,
      message,
      status: 'success',
      timestamp: new Date().toISOString(),
      metadata,
    };
    this.data = data;
  }
}

/**
 * Standard Error Response Wrapper
 */
export class ApiErrorResponse {
  @ApiProperty({
    description: 'Response metadata',
    type: ApiResponseMetadata,
  })
  meta: ApiResponseMetadata;

  @ApiProperty({
    description: 'Error details',
    type: Object,
    additionalProperties: true,
  })
  error: {
    code: string;
    message: string;
    details?: unknown;
    suggestion?: string;
  };

  constructor(
    code: string,
    message: string,
    statusCode: number = 500,
    details?: unknown,
    suggestion?: string,
  ) {
    this.meta = {
      statusCode,
      message,
      status: 'error',
      timestamp: new Date().toISOString(),
    };
    this.error = {
      code,
      message,
      details,
      suggestion,
    };
  }
}

/**
 * Standard List Response Wrapper (for paginated results)
 */
export class ApiListResponse<T = unknown> {
  @ApiProperty({
    description: 'Response metadata',
    type: ApiResponseMetadata,
  })
  meta: ApiResponseMetadata;

  @ApiProperty({
    description: 'List of items',
    type: Array,
  })
  data: T[];

  @ApiPropertyOptional({
    description: 'Pagination information',
    type: Object,
    additionalProperties: true,
  })
  pagination?: {
    total: number;
    limit: number;
    offset: number;
    hasMore: boolean;
  };

  constructor(
    data: T[],
    message: string = 'Items retrieved successfully',
    statusCode: number = 200,
    pagination?: {
      total: number;
      limit: number;
      offset: number;
      hasMore: boolean;
    },
  ) {
    this.meta = {
      statusCode,
      message,
      status: 'success',
      timestamp: new Date().toISOString(),
    };
    this.data = data;
    if (pagination) {
      this.pagination = pagination;
    }
  }
}

/**
 * Standard Delete Response
 */
export class ApiDeleteResponse {
  @ApiProperty({
    description: 'Response metadata',
    type: ApiResponseMetadata,
  })
  meta: ApiResponseMetadata;

  @ApiProperty({
    description: 'Deleted resource ID',
    example: '123e4567-e89b-12d3-a456-426614174000',
  })
  deletedId: string;

  constructor(
    deletedId: string,
    message: string = 'Resource deleted successfully',
    statusCode: number = 200,
  ) {
    this.meta = {
      statusCode,
      message,
      status: 'success',
      timestamp: new Date().toISOString(),
    };
    this.deletedId = deletedId;
  }
}

/**
 * Helper function to create success response
 */
export function createSuccessResponse<T>(
  data: T,
  message?: string,
  statusCode?: number,
  metadata?: Record<string, unknown>,
): ApiSuccessResponse<T> {
  return new ApiSuccessResponse(data, message, statusCode, metadata);
}

/**
 * Helper function to create list response
 */
export function createListResponse<T>(
  data: T[],
  message?: string,
  pagination?: {
    total: number;
    limit: number;
    offset: number;
    hasMore: boolean;
  },
): ApiListResponse<T> {
  return new ApiListResponse(data, message, 200, pagination);
}

/**
 * Helper function to create delete response
 */
export function createDeleteResponse(deletedId: string, message?: string): ApiDeleteResponse {
  return new ApiDeleteResponse(deletedId, message, 200);
}

/**
 * Helper function to create error response
 */
export function createErrorResponseWrapper(
  code: string,
  message: string,
  statusCode: number = 500,
  details?: unknown,
  suggestion?: string,
): ApiErrorResponse {
  return new ApiErrorResponse(code, message, statusCode, details, suggestion);
}
