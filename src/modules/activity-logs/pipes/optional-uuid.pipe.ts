/**
 * Optional UUID Pipe
 * Validates UUID only if value is provided, otherwise allows undefined/null
 */

import { PipeTransform, Injectable, BadRequestException } from '@nestjs/common';

@Injectable()
export class OptionalUUIDPipe implements PipeTransform {
  transform(value: any): string | undefined {
    if (value === undefined || value === null || value === '') {
      return undefined;
    }

    // Handle array case (query params can be arrays)
    const stringValue = Array.isArray(value) ? value[0] : String(value);

    // UUID v4 regex pattern
    const uuidRegex =
      /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

    if (!uuidRegex.test(stringValue)) {
      throw new BadRequestException(
        `Invalid UUID format: ${stringValue}. Must be a valid UUID.`,
      );
    }

    return stringValue;
  }
}
