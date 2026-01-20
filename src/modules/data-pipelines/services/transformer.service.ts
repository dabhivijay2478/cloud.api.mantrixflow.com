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
 * 
 * NoSQL ↔ SQL Support:
 * - flattenObject: Flatten nested objects for SQL (MongoDB → PostgreSQL)
 * - embedObject: Embed into nested structure (PostgreSQL → MongoDB)
 * - Array unwinding and embedding
 */

import { Injectable, Logger } from '@nestjs/common';
import * as crypto from 'node:crypto';
import * as _ from 'lodash';
import * as flat from 'flat';
import type {
  ColumnMapping,
  Transformation,
  TransformConfig,
  ValidationResult,
  PipelineError,
  ColumnTransformationType,
} from '../types/common.types';
import type { SchemaInfo } from '../types/source-handler.types';

@Injectable()
export class TransformerService {
  private readonly logger = new Logger(TransformerService.name);

  /**
   * Enhance column mappings with auto-detected transformations
   * Auto-detects MongoDB ObjectId -> UUID conversion
   */
  enhanceColumnMappings(
    mappings: ColumnMapping[],
    sampleRows?: any[],
  ): ColumnMapping[] {
    if (!mappings || mappings.length === 0) {
      return mappings;
    }

    // Auto-detect and apply objectIdToUuid transformation for MongoDB _id fields
    return mappings.map((mapping) => {
      // Check if this is a MongoDB _id mapping that needs UUID conversion
      if (
        mapping.sourceColumn === '_id' &&
        !mapping.transformation &&
        mapping.dataType !== 'uuid'
      ) {
        // Check if the value looks like a MongoDB ObjectId (24 hex characters)
        // We'll verify this in sample rows if available
        if (sampleRows && sampleRows.length > 0) {
          const firstRow = sampleRows[0];
          if (firstRow && firstRow._id) {
            const objectIdValue = firstRow._id?.toString?.() || String(firstRow._id);
            // MongoDB ObjectId is 24 hex characters
            if (/^[0-9a-fA-F]{24}$/.test(objectIdValue)) {
              this.logger.log(
                `Auto-detected MongoDB ObjectId for ${mapping.sourceColumn} -> ${mapping.destinationColumn}, applying objectIdToUuid transformation`,
              );
              return {
                ...mapping,
                transformation: 'objectIdToUuid' as ColumnTransformationType,
                dataType: 'uuid', // Set dataType to uuid for PostgreSQL compatibility
              };
            }
          }
        } else {
          // If no sample rows, assume it's an ObjectId if sourceColumn is _id
          // This is a safe assumption for MongoDB
          this.logger.log(
            `Auto-applying objectIdToUuid transformation for ${mapping.sourceColumn} -> ${mapping.destinationColumn} (MongoDB _id detected)`,
          );
          return {
            ...mapping,
            transformation: 'objectIdToUuid' as ColumnTransformationType,
            dataType: 'uuid', // Set dataType to uuid for PostgreSQL compatibility
          };
        }
      }
      return mapping;
    });
  }

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

    // Auto-detect and apply objectIdToUuid transformation for MongoDB _id fields
    // This ensures MongoDB ObjectIds are automatically converted to UUIDs
    const enhancedMappings = this.enhanceColumnMappings(mappings, rows);

    // Debug logging for troubleshooting
    this.logger.log(`Transforming ${rows.length} rows with ${enhancedMappings.length} column mappings`);
    if (rows.length > 0) {
      const sampleRow = rows[0];
      const rowKeys = Object.keys(sampleRow);
      this.logger.log(`Sample row keys: ${rowKeys.join(', ')}`);
      this.logger.log(
        `Column mappings: ${enhancedMappings.map((m) => `${m.sourceColumn} -> ${m.destinationColumn}${m.transformation ? ` (${m.transformation})` : ''}`).join(', ')}`,
      );
    }

    const transformedRows: any[] = [];
    const errors: PipelineError[] = [];

    for (let i = 0; i < rows.length; i++) {
      try {
        const row = rows[i];
        const transformed: any = {};

        // STEP 1: Apply column mappings (use enhanced mappings with auto-detected transformations)
        for (const mapping of enhancedMappings) {
          const sourceValue = this.getNestedValue(row, mapping.sourceColumn);
          let transformedValue = sourceValue;

          // Apply transformation FIRST (if specified)
          if (mapping.transformation && mapping.transformation !== 'none') {
            transformedValue = this.applyColumnTransformation(
              sourceValue,
              mapping.transformation,
              row,
            );
          }

          // Apply type conversion based on destination dataType
          if (mapping.dataType) {
            transformedValue = this.castValue(transformedValue, mapping.dataType);
          }

          // Handle null values with defaults
          if (
            (transformedValue === null || transformedValue === undefined) &&
            mapping.defaultValue
          ) {
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

    // Log sample transformed data for visibility
    if (transformedRows.length > 0) {
      this.logger.log(`Sample transformed row (first): ${JSON.stringify(transformedRows[0], null, 2)}`);
    }

    return transformedRows;
  }

  /**
   * Get list of mapped fields for visibility
   * Returns array of { sourcePath, destPath } pairs
   */
  getMappedFieldsList(mappings: ColumnMapping[]): Array<{ sourcePath: string; destPath: string }> {
    if (!mappings || mappings.length === 0) {
      return [];
    }

    return mappings.map((mapping) => ({
      sourcePath: mapping.sourcePath || mapping.sourceColumn,
      destPath: mapping.destPath || mapping.destinationColumn,
    }));
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
          errors.push(
            `Mapping ${i + 1}: duplicate destinationColumn '${mapping.destinationColumn}'`,
          );
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
          warnings.push(
            `Mapping ${i + 1}: unknown dataType '${mapping.dataType}', will treat as string`,
          );
        }
      }
    }

    // Validate transformations
    if (transformations) {
      const validTransformTypes = [
        'rename',
        'cast',
        'concat',
        'split',
        'filter',
        'mask',
        'hash',
        'custom',
      ];

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
        if (
          transformation.transformType === 'cast' &&
          !transformation.transformConfig?.targetType
        ) {
          errors.push(`Transformation ${i + 1}: cast requires transformConfig.targetType`);
        }
        if (transformation.transformType === 'concat' && !transformation.transformConfig?.fields) {
          errors.push(`Transformation ${i + 1}: concat requires transformConfig.fields array`);
        }
        if (
          transformation.transformType === 'split' &&
          transformation.transformConfig?.separator === undefined
        ) {
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
        case 'double': {
          const numVal = Number(value);
          return Number.isNaN(numVal) ? null : numVal;
        }

        case 'integer':
        case 'int': {
          const intVal = parseInt(String(value), 10);
          return Number.isNaN(intVal) ? null : intVal;
        }

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

        case 'date': {
          if (value instanceof Date) return value;
          const dateVal = new Date(value);
          return Number.isNaN(dateVal.getTime()) ? null : dateVal;
        }

        case 'timestamp':
        case 'datetime': {
          if (value instanceof Date) return value;
          const tsVal = new Date(value);
          return Number.isNaN(tsVal.getTime()) ? null : tsVal;
        }

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

        case 'uuid': {
          // Basic UUID validation
          const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
          const strVal = String(value);
          return uuidRegex.test(strVal) ? strVal : null;
        }

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
   * Enhanced to handle S3 CSV rows, API JSON responses, BigQuery/Snowflake query results
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

    // Use lodash get as fallback for complex nested paths
    const lodashValue = _.get(obj, path);
    if (lodashValue !== undefined) {
      return lodashValue;
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

  // ===========================================================================
  // NoSQL ↔ SQL BIDIRECTIONAL TRANSFORMATION METHODS
  // ===========================================================================

  /**
   * Transform data between NoSQL and SQL formats
   * Handles flattening (NoSQL→SQL) and embedding (SQL→NoSQL)
   * 
   * @returns Record<string, any[]> - Data organized by destination entity
   */
  async transformBidirectional(
    data: any[],
    mappings: ColumnMapping[],
    sourceSchema: SchemaInfo,
    destSchema: SchemaInfo,
  ): Promise<Record<string, any[]>> {
    const transformedData: Record<string, any[]> = {};
    const isNoSqlToSql = !sourceSchema.isRelational && destSchema.isRelational;
    const isSqlToNoSql = sourceSchema.isRelational && !destSchema.isRelational;

    this.logger.log(
      `Bidirectional transform: ${sourceSchema.sourceType || 'unknown'} → ${destSchema.sourceType || 'unknown'}, ` +
      `${data.length} rows, ${mappings.length} mappings`
    );

    try {
      for (let rowIndex = 0; rowIndex < data.length; rowIndex++) {
        const item = data[rowIndex];

        for (const mapping of mappings) {
          const destEntity = mapping.destEntity || mapping.destinationColumn.split('.')[0] || 'default';
          
          // Get source value using path or column name
          const sourcePath = mapping.sourcePath || mapping.sourceColumn;
          let value = _.get(item, sourcePath);

          // Apply column transformation if specified
          if (mapping.transformation) {
            value = this.applyColumnTransformation(value, mapping.transformation, item);
          }

          // Handle NoSQL → SQL: Flatten nested objects
          if (isNoSqlToSql && value !== null && value !== undefined) {
            if (mapping.isArray && Array.isArray(value)) {
              // Unwind array into separate rows
              this.unwindArray(
                transformedData,
                destEntity,
                mapping.destPath || mapping.destinationColumn,
                value,
                mapping.foreignKey,
                item,
                rowIndex,
              );
            } else if (mapping.transformation === 'flattenObject' && _.isPlainObject(value)) {
              // Flatten nested object
              const flattened = flat.flatten(value, { delimiter: '_' }) as Record<string, any>;
              const destPath = mapping.destPath || mapping.destinationColumn;
              for (const [key, val] of Object.entries(flattened)) {
                this.addToTransformed(
                  transformedData,
                  destEntity,
                  { [`${destPath}_${key}`]: val },
                  mapping.foreignKey,
                  item,
                  rowIndex,
                );
              }
            } else {
              // Simple value transfer
              const destPath = mapping.destPath || mapping.destinationColumn;
              this.addToTransformed(
                transformedData,
                destEntity,
                { [destPath]: value },
                mapping.foreignKey,
                item,
                rowIndex,
              );
            }
          }
          // Handle SQL → NoSQL: Embed into nested structure
          else if (isSqlToNoSql) {
            const destPath = mapping.destPath || mapping.destinationColumn;
            
            if (mapping.transformation === 'embedObject') {
              // Set nested path
              this.setNestedValue(
                transformedData,
                destEntity,
                destPath,
                value,
                rowIndex,
              );
            } else if (mapping.transformation === 'embedArray') {
              // Add to array at path
              this.appendToNestedArray(
                transformedData,
                destEntity,
                destPath,
                value,
                rowIndex,
              );
            } else {
              // Simple set with deep path support
              this.setNestedValue(
                transformedData,
                destEntity,
                destPath,
                value,
                rowIndex,
              );
            }
          }
          // Default: Simple mapping (same schema type)
          else {
            const destPath = mapping.destPath || mapping.destinationColumn;
            this.addToTransformed(
              transformedData,
              destEntity,
              { [destPath]: value },
              mapping.foreignKey,
              item,
              rowIndex,
            );
          }
        }
      }

      // Log summary
      for (const [entity, rows] of Object.entries(transformedData)) {
        this.logger.log(`Transformed entity '${entity}': ${rows.length} rows`);
      }

      return transformedData;
    } catch (error) {
      this.logger.error(`Bidirectional transformation failed: ${error}`);
      throw error;
    }
  }

  /**
   * Apply column-level transformation based on transformation type
   */
  private applyColumnTransformation(
    value: any,
    transformation: ColumnTransformationType,
    fullRow: any,
  ): any {
    if (value === null || value === undefined) {
      return value;
    }

    switch (transformation) {
      case 'none':
        return value;

      case 'flattenObject':
        if (_.isPlainObject(value)) {
          return flat.flatten(value, { delimiter: '_' });
        }
        return value;

      case 'flattenArray':
        return Array.isArray(value) ? value.flat() : value;

      case 'embedObject':
      case 'embedArray':
        // These are handled at the structural level, not value level
        return value;

      case 'jsonStringify':
        return JSON.stringify(value);

      case 'jsonParse':
        if (typeof value === 'string') {
          try {
            return JSON.parse(value);
          } catch {
            return value;
          }
        }
        return value;

      case 'toISODate':
        if (value instanceof Date) {
          return value.toISOString();
        }
        if (typeof value === 'string' || typeof value === 'number') {
          return new Date(value).toISOString();
        }
        return value;

      case 'toTimestamp':
        if (value instanceof Date) {
          return value.getTime();
        }
        if (typeof value === 'string') {
          return new Date(value).getTime();
        }
        return value;

      case 'objectIdToUuid':
        // MongoDB ObjectId to UUID
        // Generate a deterministic UUID from ObjectId using v5 UUID (name-based)
        if (!value) return null;
        
        const objectIdString = value?.toString?.() || String(value);
        
        // Use ObjectId hex string as namespace and generate UUID v5
        // This ensures the same ObjectId always generates the same UUID
        const namespace = '6ba7b810-9dad-11d1-80b4-00c04fd430c8'; // DNS namespace UUID
        const name = `mongodb-objectid-${objectIdString}`;
        
        // Generate UUID v5 (deterministic)
        const hash = crypto.createHash('sha1');
        hash.update(Buffer.from(namespace.replace(/-/g, ''), 'hex'));
        hash.update(name);
        const hashBytes = hash.digest();
        
        // Set version (5) and variant bits
        hashBytes[6] = (hashBytes[6] & 0x0f) | 0x50; // Version 5
        hashBytes[8] = (hashBytes[8] & 0x3f) | 0x80; // Variant 10
        
        // Format as UUID string
        const uuidString = [
          hashBytes.slice(0, 4).toString('hex'),
          hashBytes.slice(4, 6).toString('hex'),
          hashBytes.slice(6, 8).toString('hex'),
          hashBytes.slice(8, 10).toString('hex'),
          hashBytes.slice(10, 16).toString('hex'),
        ].join('-');
        
        return uuidString;

      case 'uuidToObjectId':
        // Just return string, MongoDB driver will handle
        return String(value).replace(/-/g, '').slice(0, 24);

      case 'toNumber':
        if (typeof value === 'number') return value;
        const num = Number(value);
        return isNaN(num) ? null : num;

      case 'toString':
        return String(value);

      case 'toBoolean':
        if (typeof value === 'boolean') return value;
        if (typeof value === 'string') {
          return ['true', '1', 'yes', 'on'].includes(value.toLowerCase());
        }
        return Boolean(value);

      case 'trim':
        return typeof value === 'string' ? value.trim() : value;

      case 'lowercase':
        return typeof value === 'string' ? value.toLowerCase() : value;

      case 'uppercase':
        return typeof value === 'string' ? value.toUpperCase() : value;

      default:
        return value;
    }
  }

  /**
   * Unwind an array into separate rows (MongoDB → PostgreSQL)
   */
  private unwindArray(
    transformedData: Record<string, any[]>,
    entity: string,
    destPath: string,
    arrayValue: any[],
    foreignKey: string | undefined,
    sourceItem: any,
    rowIndex: number,
  ): void {
    if (!transformedData[entity]) {
      transformedData[entity] = [];
    }

    for (let i = 0; i < arrayValue.length; i++) {
      const arrayItem = arrayValue[i];
      const row: any = {};

      // Handle array of primitives vs objects
      if (_.isPlainObject(arrayItem)) {
        // Flatten object properties
        const flattened = flat.flatten(arrayItem, { delimiter: '_' }) as Record<string, any>;
        Object.assign(row, flattened);
      } else {
        row[destPath] = arrayItem;
      }

      // Add foreign key reference
      if (foreignKey) {
        row[foreignKey] = _.get(sourceItem, '_id') || _.get(sourceItem, 'id') || rowIndex;
      }

      // Add array index
      row['_array_index'] = i;

      transformedData[entity].push(row);
    }
  }

  /**
   * Add row to transformed data with optional foreign key
   */
  private addToTransformed(
    transformedData: Record<string, any[]>,
    entity: string,
    data: Record<string, any>,
    foreignKey: string | undefined,
    sourceItem: any,
    rowIndex: number,
  ): void {
    if (!transformedData[entity]) {
      transformedData[entity] = [];
    }

    // Find existing row for this source item or create new
    let row = transformedData[entity][rowIndex];
    if (!row) {
      row = {};
      transformedData[entity][rowIndex] = row;
    }

    // Merge data
    Object.assign(row, data);

    // Add foreign key if specified
    if (foreignKey && !row[foreignKey]) {
      row[foreignKey] = _.get(sourceItem, '_id') || _.get(sourceItem, 'id') || rowIndex;
    }
  }

  /**
   * Set nested value in transformed data (SQL → NoSQL embedding)
   */
  private setNestedValue(
    transformedData: Record<string, any[]>,
    entity: string,
    path: string,
    value: any,
    rowIndex: number,
  ): void {
    if (!transformedData[entity]) {
      transformedData[entity] = [];
    }

    // Ensure row exists
    if (!transformedData[entity][rowIndex]) {
      transformedData[entity][rowIndex] = {};
    }

    // Use lodash set for deep path
    _.set(transformedData[entity][rowIndex], path, value);
  }

  /**
   * Append value to nested array (SQL → NoSQL embedding)
   */
  private appendToNestedArray(
    transformedData: Record<string, any[]>,
    entity: string,
    path: string,
    value: any,
    rowIndex: number,
  ): void {
    if (!transformedData[entity]) {
      transformedData[entity] = [];
    }

    if (!transformedData[entity][rowIndex]) {
      transformedData[entity][rowIndex] = {};
    }

    const existingArray = _.get(transformedData[entity][rowIndex], path) || [];
    existingArray.push(value);
    _.set(transformedData[entity][rowIndex], path, existingArray);
  }

  /**
   * Validate bidirectional mappings against source and destination schemas
   */
  validateBidirectional(
    mappings: ColumnMapping[],
    sourceSchema: SchemaInfo,
    destSchema: SchemaInfo,
  ): ValidationResult {
    const errors: string[] = [];
    const warnings: string[] = [];

    if (!mappings || mappings.length === 0) {
      errors.push('At least one column mapping is required');
      return { valid: false, errors };
    }

    const sourceColumns = new Set(sourceSchema.columns.map(c => c.name));
    const destColumns = new Set(destSchema.columns.map(c => c.name));
    const isNoSqlToSql = !sourceSchema.isRelational && destSchema.isRelational;
    const isSqlToNoSql = sourceSchema.isRelational && !destSchema.isRelational;

    for (let i = 0; i < mappings.length; i++) {
      const mapping = mappings[i];
      const sourcePath = mapping.sourcePath || mapping.sourceColumn;
      const destPath = mapping.destPath || mapping.destinationColumn;

      // Check source path exists (root level for simple paths)
      const sourceRoot = sourcePath.split('.')[0].replace(/\[\d*\]/, '');
      if (!sourceColumns.has(sourceRoot) && sourceColumns.size > 0) {
        warnings.push(`Mapping ${i + 1}: source path '${sourcePath}' root not found in schema`);
      }

      // Check destination compatibility
      const destRoot = destPath.split('.')[0];
      if (!destColumns.has(destRoot) && destColumns.size > 0) {
        warnings.push(`Mapping ${i + 1}: destination '${destPath}' not found in schema (will be created)`);
      }

      // Validate NoSQL → SQL specific
      if (isNoSqlToSql) {
        if (mapping.isArray && !mapping.foreignKey) {
          warnings.push(`Mapping ${i + 1}: array unwinding without foreignKey - rows won't be linked`);
        }
        if (sourcePath.includes('.') && mapping.transformation !== 'flattenObject') {
          warnings.push(`Mapping ${i + 1}: nested path without flattenObject transformation`);
        }
      }

      // Validate SQL → NoSQL specific
      if (isSqlToNoSql) {
        if (destPath.includes('.') && !mapping.transformation) {
          warnings.push(`Mapping ${i + 1}: nested destination without embed transformation`);
        }
      }

      // Check required fields
      if (mapping.required) {
        // Will be validated during actual transform
      }
    }

    return {
      valid: errors.length === 0,
      errors,
      warnings: warnings.length > 0 ? warnings : undefined,
    };
  }

  /**
   * Helper to determine if data source type is relational
   */
  isRelationalType(sourceType: string): boolean {
    const relationalTypes = ['postgres', 'postgresql', 'mysql', 'mariadb', 'sqlite', 'mssql', 'oracle', 'bigquery', 'snowflake', 'redshift'];
    return relationalTypes.includes(sourceType?.toLowerCase());
  }

  /**
   * Helper to determine if data source type is NoSQL/document-based
   */
  isNoSqlType(sourceType: string): boolean {
    const noSqlTypes = ['mongodb', 'documentdb', 'dynamodb', 'couchdb', 'firestore'];
    return noSqlTypes.includes(sourceType?.toLowerCase());
  }

  /**
   * Create auto-mappings from source to destination schema
   * Useful for initial setup or simple migrations
   */
  createAutoMappings(
    sourceSchema: SchemaInfo,
    destSchema: SchemaInfo,
  ): ColumnMapping[] {
    const mappings: ColumnMapping[] = [];
    const isNoSqlToSql = !sourceSchema.isRelational && destSchema.isRelational;

    for (const sourceCol of sourceSchema.columns) {
      // Find matching dest column by name
      const destCol = destSchema.columns.find(
        c => c.name.toLowerCase() === sourceCol.name.toLowerCase()
      );

      const mapping: ColumnMapping = {
        sourceColumn: sourceCol.name,
        destinationColumn: destCol?.name || sourceCol.name,
        sourceEntity: sourceSchema.entityName,
        destEntity: destSchema.entityName,
        sourcePath: sourceCol.name,
        destPath: destCol?.name || sourceCol.name,
        dataType: destCol?.dataType || sourceCol.dataType,
        nullable: destCol?.nullable ?? sourceCol.nullable,
        isPrimaryKey: sourceCol.isPrimaryKey,
      };

      // Add transformation based on schema types
      if (isNoSqlToSql && sourceCol.dataType === 'object') {
        mapping.transformation = 'flattenObject';
      } else if (isNoSqlToSql && sourceCol.dataType === 'array') {
        mapping.transformation = 'flattenArray';
        mapping.isArray = true;
      }

      mappings.push(mapping);
    }

    return mappings;
  }
}
