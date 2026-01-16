/**
 * Transformer Service
 * Generic service for transforming data between source and destination
 * Works with all data source types
 */

import { Injectable, Logger } from '@nestjs/common';
import type { ColumnMapping, Transformation, ValidationResult } from '../types/common.types';

@Injectable()
export class TransformerService {
  private readonly logger = new Logger(TransformerService.name);

  /**
   * Transform a batch of rows
   */
  async transform(
    rows: any[],
    mappings: ColumnMapping[],
    transformations?: Transformation[],
  ): Promise<any[]> {
    if (!mappings || mappings.length === 0) {
      this.logger.warn('No column mappings provided, returning rows as-is');
      return rows;
    }

    return rows.map((row) => {
      const transformed: any = {};

      // Apply column mappings
      for (const mapping of mappings) {
        const sourceValue = row[mapping.sourceColumn];
        transformed[mapping.destinationColumn] = sourceValue;
      }

      // Apply transformations if provided
      if (transformations && transformations.length > 0) {
        for (const transformation of transformations) {
          if (transformation.sourceColumn in row) {
            const transformedValue = this.applyTransformation(
              row[transformation.sourceColumn],
              transformation,
            );
            transformed[transformation.destinationColumn] = transformedValue;
          }
        }
      }

      return transformed;
    });
  }

  /**
   * Validate transformation configuration
   */
  validate(mappings: ColumnMapping[], transformations?: Transformation[]): ValidationResult {
    const errors: string[] = [];
    const warnings: string[] = [];

    // Validate mappings
    if (!mappings || mappings.length === 0) {
      errors.push('At least one column mapping is required');
    }

    for (const mapping of mappings) {
      if (!mapping.sourceColumn) {
        errors.push('Column mapping missing sourceColumn');
      }
      if (!mapping.destinationColumn) {
        errors.push('Column mapping missing destinationColumn');
      }
      if (!mapping.dataType) {
        errors.push(`Column mapping for ${mapping.destinationColumn} missing dataType`);
      }
    }

    // Validate transformations
    if (transformations) {
      for (const transformation of transformations) {
        if (!transformation.sourceColumn) {
          errors.push('Transformation missing sourceColumn');
        }
        if (!transformation.destinationColumn) {
          errors.push('Transformation missing destinationColumn');
        }
        if (!transformation.transformType) {
          errors.push(
            `Transformation for ${transformation.destinationColumn} missing transformType`,
          );
        }
      }
    }

    return {
      valid: errors.length === 0,
      errors,
      warnings,
    };
  }

  /**
   * Apply a single transformation
   */
  private applyTransformation(value: any, transformation: Transformation): any {
    switch (transformation.transformType) {
      case 'rename':
        // Already handled by column mapping
        return value;

      case 'cast':
        return this.castValue(value, transformation.transformConfig?.targetType || 'string');

      case 'concat': {
        const fields = transformation.transformConfig?.fields || [];
        return fields
          .map((f: string) => value[f] || '')
          .join(transformation.transformConfig?.separator || '');
      }

      case 'split': {
        const separator = transformation.transformConfig?.separator || ',';
        const index = transformation.transformConfig?.index || 0;
        const parts = String(value).split(separator);
        return parts[index] || null;
      }

      case 'custom':
        // Custom transformation - apply transformConfig function
        if (typeof transformation.transformConfig?.transform === 'function') {
          return transformation.transformConfig.transform(value);
        }
        return value;

      default:
        this.logger.warn(`Unknown transformation type: ${transformation.transformType}`);
        return value;
    }
  }

  /**
   * Cast value to target type
   */
  private castValue(value: any, targetType: string): any {
    try {
      switch (targetType.toLowerCase()) {
        case 'string':
          return String(value);
        case 'number':
        case 'integer':
          return Number(value);
        case 'boolean':
          return Boolean(value);
        case 'date':
        case 'timestamp':
          return new Date(value);
        case 'json':
          return typeof value === 'string' ? JSON.parse(value) : value;
        default:
          return value;
      }
    } catch (error) {
      this.logger.warn(`Failed to cast value to ${targetType}: ${error}`);
      return value;
    }
  }
}
