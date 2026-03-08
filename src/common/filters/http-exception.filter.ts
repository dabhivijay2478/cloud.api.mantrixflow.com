/**
 * Global HTTP Exception Filter
 * Catches all exceptions and returns consistent ApiErrorResponse format
 */

import {
  ExceptionFilter,
  Catch,
  ArgumentsHost,
  HttpException,
  HttpStatus,
  Logger,
} from '@nestjs/common';
import { Request, Response } from 'express';
import { createErrorResponseWrapper } from '../dto/api-response.dto';
import { ERROR_CODES } from '../constants/error-codes.constants';

@Catch()
export class HttpExceptionFilter implements ExceptionFilter {
  private readonly logger = new Logger(HttpExceptionFilter.name);

  catch(exception: unknown, host: ArgumentsHost): void {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse<Response>();
    const request = ctx.getRequest<Request>();

    const requestId =
      (request.headers['x-request-id'] as string) ??
      `req-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;

    let statusCode: number;
    let code: string;
    let message: string;
    let details: unknown;

    if (exception instanceof HttpException) {
      statusCode = exception.getStatus();
      const exceptionResponse = exception.getResponse();

      if (typeof exceptionResponse === 'object' && exceptionResponse !== null) {
        const res = exceptionResponse as Record<string, unknown>;
        code = (res.code as string) ?? this.statusToCode(statusCode);
        message =
          (res.message as string) ??
          (typeof res.error === 'string' ? res.error : undefined) ??
          exception.message;
        details = res.details ?? (Array.isArray(res.message) ? res.message : undefined);
      } else {
        code = this.statusToCode(statusCode);
        message = typeof exceptionResponse === 'string' ? exceptionResponse : exception.message;
        details = undefined;
      }
    } else {
      statusCode = HttpStatus.INTERNAL_SERVER_ERROR;
      code = ERROR_CODES.INTERNAL_ERROR;
      message = exception instanceof Error ? exception.message : 'An unexpected error occurred';
      details = exception instanceof Error ? { stack: exception.stack } : undefined;

      this.logger.error(
        `Unhandled exception: ${message}`,
        exception instanceof Error ? exception.stack : String(exception),
      );
    }

    this.logger.warn(`Request ${requestId} failed: ${code} ${statusCode} - ${message}`);

    const errorResponse = createErrorResponseWrapper(code, message, statusCode, details);
    errorResponse.meta.requestId = requestId;

    response.status(statusCode).json(errorResponse);
  }

  private statusToCode(status: number): string {
    switch (status) {
      case HttpStatus.NOT_FOUND:
        return ERROR_CODES.NOT_FOUND;
      case HttpStatus.BAD_REQUEST:
        return ERROR_CODES.BAD_REQUEST;
      case HttpStatus.UNAUTHORIZED:
        return ERROR_CODES.UNAUTHORIZED;
      case HttpStatus.FORBIDDEN:
        return ERROR_CODES.FORBIDDEN;
      case HttpStatus.CONFLICT:
        return ERROR_CODES.CONFLICT;
      case HttpStatus.SERVICE_UNAVAILABLE:
        return ERROR_CODES.SERVICE_UNAVAILABLE;
      default:
        return status >= 500 ? ERROR_CODES.INTERNAL_ERROR : ERROR_CODES.VALIDATION_ERROR;
    }
  }
}
