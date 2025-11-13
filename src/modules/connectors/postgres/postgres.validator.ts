/**
 * PostgreSQL Connector Validator
 * Validates connection configurations and inputs
 */

import { Injectable } from '@nestjs/common';
import { z } from 'zod';
import {
  PostgresConnectionConfigSchema,
  TestConnectionSchema,
  ExecuteQuerySchema,
  CreateSyncJobSchema,
  UpdateSyncScheduleSchema,
} from './postgres.types';
import {
  CONNECTION_DEFAULTS,
  MAX_CONNECTIONS_PER_ORG,
} from './constants/postgres.constants';
import { PostgresErrorCode } from './constants/error-codes.constants';

@Injectable()
export class PostgresValidator {
  /**
   * Validate connection configuration
   */
  validateConnectionConfig(config: unknown): {
    isValid: boolean;
    error?: string;
    errorCode?: PostgresErrorCode;
    data?: z.infer<typeof PostgresConnectionConfigSchema>;
  } {
    try {
      const result = PostgresConnectionConfigSchema.safeParse(config);
      if (!result.success) {
        const firstError = result.error.errors[0];
        return {
          isValid: false,
          error: firstError.message,
          errorCode: this.mapValidationErrorToCode(
            firstError.path[0] as string,
          ),
        };
      }

      // Additional validation
      if (
        result.data.poolSize &&
        result.data.poolSize > CONNECTION_DEFAULTS.MAX_POOL_SIZE
      ) {
        return {
          isValid: false,
          error: `Pool size cannot exceed ${CONNECTION_DEFAULTS.MAX_POOL_SIZE}`,
          errorCode: PostgresErrorCode.INVALID_POOL_SIZE,
        };
      }

      return {
        isValid: true,
        data: result.data,
      };
    } catch (error) {
      return {
        isValid: false,
        error: error instanceof Error ? error.message : 'Validation failed',
        errorCode: PostgresErrorCode.UNKNOWN_ERROR,
      };
    }
  }

  /**
   * Validate test connection request
   */
  validateTestConnection(config: unknown): {
    isValid: boolean;
    error?: string;
    errorCode?: PostgresErrorCode;
    data?: z.infer<typeof TestConnectionSchema>;
  } {
    try {
      const result = TestConnectionSchema.safeParse(config);
      if (!result.success) {
        const firstError = result.error.errors[0];
        return {
          isValid: false,
          error: firstError.message,
          errorCode: this.mapValidationErrorToCode(
            firstError.path[0] as string,
          ),
        };
      }

      return {
        isValid: true,
        data: result.data,
      };
    } catch (error) {
      return {
        isValid: false,
        error: error instanceof Error ? error.message : 'Validation failed',
        errorCode: PostgresErrorCode.UNKNOWN_ERROR,
      };
    }
  }

  /**
   * Validate query execution request
   */
  validateQuery(query: unknown): {
    isValid: boolean;
    error?: string;
    errorCode?: PostgresErrorCode;
    data?: z.infer<typeof ExecuteQuerySchema>;
  } {
    try {
      const result = ExecuteQuerySchema.safeParse(query);
      if (!result.success) {
        const firstError = result.error.errors[0];
        return {
          isValid: false,
          error: firstError.message,
          errorCode: PostgresErrorCode.QUERY_SYNTAX_ERROR,
        };
      }

      return {
        isValid: true,
        data: result.data,
      };
    } catch (error) {
      return {
        isValid: false,
        error: error instanceof Error ? error.message : 'Validation failed',
        errorCode: PostgresErrorCode.UNKNOWN_ERROR,
      };
    }
  }

  /**
   * Validate sync job creation request
   */
  validateSyncJob(config: unknown): {
    isValid: boolean;
    error?: string;
    errorCode?: PostgresErrorCode;
    data?: z.infer<typeof CreateSyncJobSchema>;
  } {
    try {
      const result = CreateSyncJobSchema.safeParse(config);
      if (!result.success) {
        const firstError = result.error.errors[0];
        return {
          isValid: false,
          error: firstError.message,
          errorCode: PostgresErrorCode.UNKNOWN_ERROR,
        };
      }

      // Additional validation for incremental sync
      if (
        result.data.syncMode === 'incremental' &&
        !result.data.incrementalColumn
      ) {
        return {
          isValid: false,
          error: 'Incremental sync requires an incremental column',
          errorCode: PostgresErrorCode.SYNC_INVALID_INCREMENTAL_COLUMN,
        };
      }

      return {
        isValid: true,
        data: result.data,
      };
    } catch (error) {
      return {
        isValid: false,
        error: error instanceof Error ? error.message : 'Validation failed',
        errorCode: PostgresErrorCode.UNKNOWN_ERROR,
      };
    }
  }

  /**
   * Validate sync schedule update
   */
  validateSyncSchedule(config: unknown): {
    isValid: boolean;
    error?: string;
    errorCode?: PostgresErrorCode;
    data?: z.infer<typeof UpdateSyncScheduleSchema>;
  } {
    try {
      const result = UpdateSyncScheduleSchema.safeParse(config);
      if (!result.success) {
        const firstError = result.error.errors[0];
        return {
          isValid: false,
          error: firstError.message,
          errorCode: PostgresErrorCode.UNKNOWN_ERROR,
        };
      }

      return {
        isValid: true,
        data: result.data,
      };
    } catch (error) {
      return {
        isValid: false,
        error: error instanceof Error ? error.message : 'Validation failed',
        errorCode: PostgresErrorCode.UNKNOWN_ERROR,
      };
    }
  }

  /**
   * Validate connection count per organization
   */
  validateConnectionCount(currentCount: number): {
    isValid: boolean;
    error?: string;
    errorCode?: PostgresErrorCode;
  } {
    if (currentCount >= MAX_CONNECTIONS_PER_ORG) {
      return {
        isValid: false,
        error: `Maximum ${MAX_CONNECTIONS_PER_ORG} connections per organization exceeded`,
        errorCode: PostgresErrorCode.MAX_CONNECTIONS_EXCEEDED,
      };
    }

    return { isValid: true };
  }

  /**
   * Map validation error field to error code
   */
  private mapValidationErrorToCode(field: string): PostgresErrorCode {
    const fieldMap: Record<string, PostgresErrorCode> = {
      host: PostgresErrorCode.INVALID_HOST,
      port: PostgresErrorCode.INVALID_PORT,
      database: PostgresErrorCode.INVALID_DATABASE,
      username: PostgresErrorCode.INVALID_USERNAME,
      password: PostgresErrorCode.INVALID_PASSWORD,
      ssl: PostgresErrorCode.INVALID_SSL_CONFIG,
      sshTunnel: PostgresErrorCode.INVALID_SSH_CONFIG,
      poolSize: PostgresErrorCode.INVALID_POOL_SIZE,
      queryTimeout: PostgresErrorCode.INVALID_QUERY_TIMEOUT,
    };

    return fieldMap[field] || PostgresErrorCode.UNKNOWN_ERROR;
  }
}
