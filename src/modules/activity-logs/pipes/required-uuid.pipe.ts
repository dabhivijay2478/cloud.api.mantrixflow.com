/**
 * Required UUID Pipe
 * Validates that a UUID string is provided
 */

import { PipeTransform, Injectable, BadRequestException } from '@nestjs/common';

@Injectable()
export class RequiredUUIDPipe implements PipeTransform {
  transform(value: any): string {
    // Handle undefined, null, or empty string
    if (value === undefined || value === null) {
      throw new BadRequestException('organizationId is required and must be a valid UUID');
    }

    // Convert to string - handle all possible types
    let stringValue: string;
    if (Array.isArray(value)) {
      // Query params can be arrays
      if (value.length === 0) {
        throw new BadRequestException('organizationId is required and must be a valid UUID');
      }
      stringValue = String(value[0]);
    } else if (typeof value === 'string') {
      stringValue = value;
    } else {
      // Convert any other type to string
      stringValue = String(value);
    }

    // Check for empty string after conversion
    if (!stringValue || stringValue.trim() === '') {
      throw new BadRequestException('organizationId is required and must be a valid UUID');
    }

    // UUID v4 regex pattern (more lenient - accepts v1-v5)
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

    if (!uuidRegex.test(stringValue)) {
      throw new BadRequestException(`Invalid UUID format: ${stringValue}. Must be a valid UUID.`);
    }

    return stringValue;
  }
}
