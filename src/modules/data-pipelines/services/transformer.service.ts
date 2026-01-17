/**
 * Transformer Service
 * Generic service for transforming data between source and destination
 * Works with all data source types
 *
 * Supports transformations:
 * - rename: Rename columns
 * - cast: Type conversion
 * - concat: Combine multiple fields
 * - split: Split a field into parts
 * - filter: Filter rows based on conditions
 * - mask: Mask sensitive data
 * - hash: Hash values for anonymization
 * - custom: Custom transformation logic
 */

import { Injectable, Logger } from '@nestjs/common';
import * as crypto from 'crypto';
import type {
  ColumnMapping,
  Transformation,
  TransformConfig,
  ValidationResult,
  PipelineError,
} from '../types/common.types';

@Injectable()
export class TransformerService {
  private readonly logger = new Logger(TransformerService.name);

  /**
   * Transform a batch of rows
   * Applies column mappings and transformations
   */
  async transform(
    rows: any[],
    mappings: ColumnMapping[],
    transformations?: Transformation[],
  ): Promise<any[]> {
    if (!rows || rows.length === 0) {
      return [];
    }

    if (!mappings || mappings.length === 0) {
      this.logger.warn('No column mappings provided, returning rows as-is');
      return rows;
    }

    // Debug logging for troubleshooting
    this.logger.log(`Transforming ${rows.length} rows with ${mappings.length} column mappings`);
    if (rows.length > 0) {
      const sampleRow = rows[0];
      const rowKeys = Object.keys(sampleRow);
      this.logger.log(`Sample row keys: ${rowKeys.join(', ')}`);
      this.logger.log(`Column mappings: ${mappings.map(m => `${m.sourceColumn} -> ${m.destinationColumn}`).join(', ')}`);
    }

    const transformedRows: any[] = [];
    const errors: PipelineError[] = [];

    for (let i = 0; i < rows.length; i++) {
      try {
        const row = rows[i];
        const transformed: any = {};

        // STEP 1: Apply column mappings
        for (const mapping of mappings) {
          const sourceValue = this.getNestedValue(row, mapping.sourceColumn);
          let transformedValue = sourceValue;

          // Apply type conversion based on destination dataType
          if (mapping.dataType) {
            transformedValue = this.castValue(sourceValue, mapping.dataType);
          }

          // Handle null values with defaults
          if ((transformedValue === null || transformedValue === undefined) && mapping.defaultValue) {
            transformedValue = mapping.defaultValue;
          }

          // Check nullable constraints
          if (!mapping.nullable && (transformedValue === null || transformedValue === undefined)) {
            throw new Error(
              `Column ${mapping.destinationColumn} cannot be null (source: ${mapping.sourceColumn})`,
            );
          }

          transformed[mapping.destinationColumn] = transformedValue;
        }

        // STEP 2: Apply explicit transformations
        if (transformations && transformations.length > 0) {
          for (const transformation of transformations) {
            try {
              const sourceValue = this.getNestedValue(row, transformation.sourceColumn);
              const transformedValue = this.applyTransformation(
                sourceValue,
                transformation,
                row, // Pass full row for concat operations
              );
              transformed[transformation.destinationColumn] = transformedValue;
            } catch (error) {
              this.logger.warn(
                `Transformation failed for column ${transformation.sourceColumn}: ${error}`,
              );
              // Keep original value if transformation fails
              transformed[transformation.destinationColumn] = this.getNestedValue(
                row,
                transformation.sourceColumn,
              );
            }
          }
        }

        transformedRows.push(transformed);
      } catch (error) {
        errors.push({
          row: i,
          message: error instanceof Error ? error.message : String(error),
        });
        this.logger.error(`Row ${i} transformation failed: ${error}`);
      }
    }

    if (errors.length > 0) {
      this.logger.warn(`${errors.length} rows had transformation errors`);
    }

    return transformedRows;
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
    } else {
      const destinationColumns = new Set<string>();

      for (let i = 0; i < mappings.length; i++) {
        const mapping = mappings[i];

        if (!mapping.sourceColumn) {
          errors.push(`Mapping ${i + 1}: sourceColumn is required`);
        }
        if (!mapping.destinationColumn) {
          errors.push(`Mapping ${i + 1}: destinationColumn is required`);
        }
        if (!mapping.dataType) {
          warnings.push(`Mapping ${i + 1}: dataType not specified, will infer from source`);
        }

        // Check for duplicate destination columns
        if (destinationColumns.has(mapping.destinationColumn)) {
          errors.push(`Mapping ${i + 1}: duplicate destinationColumn '${mapping.destinationColumn}'`);
        }
        destinationColumns.add(mapping.destinationColumn);

        // Validate supported data types
        const supportedTypes = [
          'string',
          'text',
          'varchar',
          'number',
          'integer',
          'bigint',
          'float',
          'double',
          'boolean',
          'date',
          'timestamp',
          'datetime',
          'json',
          'object',
          'array',
          'uuid',
        ];
        if (mapping.dataType && !supportedTypes.includes(mapping.dataType.toLowerCase())) {
          warnings.push(`Mapping ${i + 1}: unknown dataType '${mapping.dataType}', will treat as string`);
        }
      }
    }

    // Validate transformations
    if (transformations) {
      const validTransformTypes = ['rename', 'cast', 'concat', 'split', 'filter', 'mask', 'hash', 'custom'];

      for (let i = 0; i < transformations.length; i++) {
        const transformation = transformations[i];

        if (!transformation.sourceColumn) {
          errors.push(`Transformation ${i + 1}: sourceColumn is required`);
        }
        if (!transformation.destinationColumn) {
          errors.push(`Transformation ${i + 1}: destinationColumn is required`);
        }
        if (!transformation.transformType) {
          errors.push(`Transformation ${i + 1}: transformType is required`);
        } else if (!validTransformTypes.includes(transformation.transformType)) {
          errors.push(
            `Transformation ${i + 1}: invalid transformType '${transformation.transformType}'. Valid types: ${validTransformTypes.join(', ')}`,
          );
        }

        // Validate transform-specific config
        if (transformation.transformType === 'cast' && !transformation.transformConfig?.targetType) {
          errors.push(`Transformation ${i + 1}: cast requires transformConfig.targetType`);
        }
        if (transformation.transformType === 'concat' && !transformation.transformConfig?.fields) {
          errors.push(`Transformation ${i + 1}: concat requires transformConfig.fields array`);
        }
        if (transformation.transformType === 'split' && transformation.transformConfig?.separator === undefined) {
          warnings.push(`Transformation ${i + 1}: split defaults to comma separator`);
        }
      }
    }

    return {
      valid: errors.length === 0,
      errors,
      warnings: warnings.length > 0 ? warnings : undefined,
    };
  }

  /**
   * Apply a single transformation
   */
  private applyTransformation(value: any, transformation: Transformation, fullRow: any): any {
    const config = transformation.transformConfig || {};

    switch (transformation.transformType) {
      case 'rename':
        // Rename is handled by column mapping, just return value
        return value;

      case 'cast':
        return this.castValue(value, config.targetType || 'string');

      case 'concat':
        return this.concatFields(fullRow, config.fields || [], config.separator || '');

      case 'split':
        return this.splitValue(value, config.separator || ',', config.index || 0);

      case 'filter':
        return this.filterValue(value, config);

      case 'mask':
        return this.maskValue(value, config.maskChar || '*', config.visibleChars || 4);

      case 'hash':
        return this.hashValue(value, config.algorithm || 'sha256');

      case 'custom':
        return this.applyCustomTransformation(value, config, fullRow);

      default:
        this.logger.warn(`Unknown transformation type: ${transformation.transformType}`);
        return value;
    }
  }

  /**
   * Cast value to target type
   */
  private castValue(value: any, targetType: string): any {
    if (value === null || value === undefined) {
      return null;
    }

    try {
      switch (targetType.toLowerCase()) {
        case 'string':
        case 'text':
        case 'varchar':
          return String(value);

        case 'number':
        case 'float':
        case 'double':
          const numVal = Number(value);
          return isNaN(numVal) ? null : numVal;

        case 'integer':
        case 'int':
          const intVal = parseInt(String(value), 10);
          return isNaN(intVal) ? null : intVal;

        case 'bigint':
          try {
            return BigInt(value);
          } catch {
            return null;
          }

        case 'boolean':
        case 'bool':
          if (typeof value === 'boolean') return value;
          if (typeof value === 'string') {
            const lower = value.toLowerCase();
            if (['true', '1', 'yes', 'on'].includes(lower)) return true;
            if (['false', '0', 'no', 'off'].includes(lower)) return false;
          }
          return Boolean(value);

        case 'date':
          if (value instanceof Date) return value;
          const dateVal = new Date(value);
          return isNaN(dateVal.getTime()) ? null : dateVal;

        case 'timestamp':
        case 'datetime':
          if (value instanceof Date) return value;
          const tsVal = new Date(value);
          return isNaN(tsVal.getTime()) ? null : tsVal;

        case 'json':
        case 'object':
          if (typeof value === 'object') return value;
          if (typeof value === 'string') {
            try {
              return JSON.parse(value);
            } catch {
              return value;
            }
          }
          return value;

        case 'array':
          if (Array.isArray(value)) return value;
          if (typeof value === 'string') {
            try {
              const parsed = JSON.parse(value);
              return Array.isArray(parsed) ? parsed : [value];
            } catch {
              return value.split(',').map((v: string) => v.trim());
            }
          }
          return [value];

        case 'uuid':
          // Basic UUID validation
          const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
          const strVal = String(value);
          return uuidRegex.test(strVal) ? strVal : null;

        default:
          return value;
      }
    } catch (error) {
      this.logger.warn(`Failed to cast value to ${targetType}: ${error}`);
      return value;
    }
  }

  /**
   * Concatenate multiple fields
   */
  private concatFields(row: any, fields: string[], separator: string): string {
    const values = fields.map((field) => {
      const value = this.getNestedValue(row, field);
      return value !== null && value !== undefined ? String(value) : '';
    });
    return values.join(separator);
  }

  /**
   * Split a value and return part at index
   */
  private splitValue(value: any, separator: string, index: number): string | null {
    if (value === null || value === undefined) return null;
    const parts = String(value).split(separator);
    return parts[index] !== undefined ? parts[index].trim() : null;
  }

  /**
   * Filter value based on conditions
   */
  private filterValue(value: any, config: TransformConfig): any {
    if (!config.operator || config.value === undefined) {
      return value;
    }

    const compareValue = config.value;
    let passes = false;

    switch (config.operator) {
      case 'eq':
        passes = value === compareValue;
        break;
      case 'ne':
        passes = value !== compareValue;
        break;
      case 'gt':
        passes = value > compareValue;
        break;
      case 'lt':
        passes = value < compareValue;
        break;
      case 'gte':
        passes = value >= compareValue;
        break;
      case 'lte':
        passes = value <= compareValue;
        break;
      case 'contains':
        passes = String(value).includes(String(compareValue));
        break;
      case 'startsWith':
        passes = String(value).startsWith(String(compareValue));
        break;
      case 'endsWith':
        passes = String(value).endsWith(String(compareValue));
        break;
      default:
        passes = true;
    }

    return passes ? value : null;
  }

  /**
   * Mask sensitive data
   */
  private maskValue(value: any, maskChar: string, visibleChars: number): string {
    if (value === null || value === undefined) return '';
    const strValue = String(value);

    if (strValue.length <= visibleChars) {
      return maskChar.repeat(strValue.length);
    }

    const visible = strValue.slice(-visibleChars);
    const masked = maskChar.repeat(strValue.length - visibleChars);
    return masked + visible;
  }

  /**
   * Hash value for anonymization
   */
  private hashValue(value: any, algorithm: 'md5' | 'sha256' | 'sha512'): string {
    if (value === null || value === undefined) return '';
    return crypto.createHash(algorithm).update(String(value)).digest('hex');
  }

  /**
   * Apply custom transformation
   */
  private applyCustomTransformation(value: any, config: TransformConfig, fullRow: any): any {
    // If a transform function is provided (for programmatic use)
    if (typeof config.transform === 'function') {
      return config.transform(value);
    }

    // If an expression is provided (for configuration-based transforms)
    if (config.expression) {
      try {
        // Safe evaluation using Function constructor (limited scope)
        // Note: In production, consider using a sandboxed evaluator
        const fn = new Function('value', 'row', `return ${config.expression}`);
        return fn(value, fullRow);
      } catch (error) {
        this.logger.error(`Custom expression failed: ${error}`);
        return value;
      }
    }

    return value;
  }

  /**
   * Get nested value from object using dot notation
   * Handles both direct column names and table.column format from UI
   */
  private getNestedValue(obj: any, path: string): any {
    if (!obj || !path) return undefined;

    // Handle simple case (no nesting) - direct column name
    if (!path.includes('.') && !path.includes('[')) {
      return obj[path];
    }

    // First, try to find the value using the full path
    // This handles nested paths like "address.city" or "items[0].name"
    const parts = path.split(/\.|\[|\]/).filter(Boolean);
    let current = obj;

    for (const part of parts) {
      if (current === null || current === undefined) {
        break;
      }
      current = current[part];
    }

    // If we found a value, return it
    if (current !== undefined) {
      return current;
    }

    // Special handling for table.column format from UI (e.g., "users.email")
    // The actual row data has columns directly by name, not nested under table
    // Try looking up just the last part (column name)
    if (parts.length >= 2) {
      const columnName = parts[parts.length - 1];
      if (obj[columnName] !== undefined) {
        return obj[columnName];
      }
    }

    // Not found
    return undefined;
  }

  /**
   * Infer data type from sample values
   */
  inferDataType(values: any[]): string {
    if (!values || values.length === 0) {
      return 'string';
    }

    // Filter out null/undefined values
    const nonNullValues = values.filter((v) => v !== null && v !== undefined);
    if (nonNullValues.length === 0) {
      return 'string';
    }

    // Count types
    const typeCounts: Record<string, number> = {};

    for (const value of nonNullValues) {
      let type: string;

      if (typeof value === 'boolean') {
        type = 'boolean';
      } else if (typeof value === 'number') {
        type = Number.isInteger(value) ? 'integer' : 'float';
      } else if (typeof value === 'bigint') {
        type = 'bigint';
      } else if (value instanceof Date) {
        type = 'timestamp';
      } else if (Array.isArray(value)) {
        type = 'array';
      } else if (typeof value === 'object') {
        type = 'object';
      } else if (typeof value === 'string') {
        // Try to detect special string types
        if (this.isISODate(value)) {
          type = 'timestamp';
        } else if (this.isUUID(value)) {
          type = 'uuid';
        } else if (this.isJSON(value)) {
          type = 'json';
        } else {
          type = 'string';
        }
      } else {
        type = 'string';
      }

      typeCounts[type] = (typeCounts[type] || 0) + 1;
    }

    // Return the most common type
    return Object.entries(typeCounts).sort((a, b) => b[1] - a[1])[0]?.[0] || 'string';
  }

  private isISODate(value: string): boolean {
    const isoDateRegex = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d{3})?(Z|[+-]\d{2}:\d{2})?)?$/;
    return isoDateRegex.test(value);
  }

  private isUUID(value: string): boolean {
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    return uuidRegex.test(value);
  }

  private isJSON(value: string): boolean {
    try {
      const parsed = JSON.parse(value);
      return typeof parsed === 'object';
    } catch {
      return false;
    }
  }
}
