/**
 * PostgreSQL Schema Mapper Service
 * Auto-map source schema to destination and handle type inference
 */

import { Injectable, Logger } from '@nestjs/common';
import type {
  ColumnInfo,
  ColumnMapping,
  TypeInferenceResult,
  ValidationError,
} from '../../data-sources/postgres/postgres.types';

@Injectable()
export class PostgresSchemaMapperService {
  private readonly logger = new Logger(PostgresSchemaMapperService.name);

  /**
   * Auto-generate column mappings from source to destination
   * Uses intelligent name matching + type inference
   */
  async autoMapColumns(
    sourceColumns: ColumnInfo[],
    destinationColumns?: ColumnInfo[],
  ): Promise<ColumnMapping[]> {
    this.logger.log(`Auto-mapping ${sourceColumns.length} source columns to destination`);

    const mappings: ColumnMapping[] = [];

    for (const sourceCol of sourceColumns) {
      // Try to find matching destination column
      let destColName = sourceCol.name;

      if (destinationColumns) {
        // Find exact match or similar name
        const match = this.findBestMatch(sourceCol.name, destinationColumns);
        if (match) {
          destColName = match.name;
        }
      }

      // Map source type to PostgreSQL type
      const pgType = this.mapSourceTypeToPostgres(sourceCol.dataType, sourceCol);

      mappings.push({
        sourceColumn: sourceCol.name,
        destinationColumn: destColName,
        dataType: pgType,
        nullable: sourceCol.isNullable,
        defaultValue: sourceCol.defaultValue,
        isPrimaryKey: sourceCol.isPrimaryKey,
        maxLength: sourceCol.maxLength,
      });
    }

    this.logger.log(`Generated ${mappings.length} column mappings`);
    return mappings;
  }

  /**
   * Suggest destination data types based on source data
   */
  async inferDestinationTypes(
    sourceColumns: ColumnInfo[],
    sampleData: any[],
  ): Promise<TypeInferenceResult[]> {
    const results: TypeInferenceResult[] = [];

    for (const column of sourceColumns) {
      const values = sampleData
        .map((row) => row[column.name])
        .filter((v) => v !== null && v !== undefined);

      const inferred = this.inferTypeFromValues(column.name, values);
      results.push(inferred);
    }

    return results;
  }

  /**
   * Map external source types (Stripe, Salesforce) to PostgreSQL types
   */
  mapExternalTypeToPostgres(sourceType: string, sourcePlatform: string): string {
    const platformMappings: Record<string, Record<string, string>> = {
      stripe: {
        string: 'TEXT',
        number: 'NUMERIC',
        integer: 'INTEGER',
        boolean: 'BOOLEAN',
        timestamp: 'TIMESTAMPTZ',
        object: 'JSONB',
        array: 'JSONB',
      },
      salesforce: {
        string: 'VARCHAR(255)',
        picklist: 'VARCHAR(255)',
        multipicklist: 'TEXT',
        textarea: 'TEXT',
        email: 'VARCHAR(255)',
        phone: 'VARCHAR(50)',
        url: 'TEXT',
        datetime: 'TIMESTAMPTZ',
        date: 'DATE',
        time: 'TIME',
        boolean: 'BOOLEAN',
        int: 'INTEGER',
        double: 'NUMERIC',
        currency: 'NUMERIC(19,4)',
        percent: 'NUMERIC(5,2)',
        id: 'VARCHAR(18)',
        reference: 'VARCHAR(18)',
      },
      google_sheets: {
        string: 'TEXT',
        number: 'NUMERIC',
        boolean: 'BOOLEAN',
        date: 'DATE',
        datetime: 'TIMESTAMPTZ',
      },
    };

    const mapping = platformMappings[sourcePlatform.toLowerCase()]?.[sourceType.toLowerCase()];
    return mapping || 'TEXT'; // Default to TEXT for unknown types
  }

  /**
   * Validate that mapping is safe (no data loss)
   */
  validateMapping(mapping: ColumnMapping): ValidationError[] {
    const errors: ValidationError[] = [];

    // Check for potential data loss
    if (mapping.dataType === 'TEXT' && mapping.maxLength) {
      errors.push({
        column: mapping.destinationColumn,
        error: `Using TEXT type for column with max length ${mapping.maxLength}. Consider using VARCHAR(${mapping.maxLength}) instead.`,
        severity: 'warning',
      });
    }

    // Check nullable constraints
    if (!mapping.nullable && !mapping.defaultValue) {
      errors.push({
        column: mapping.destinationColumn,
        error: 'Column is NOT NULL but has no default value. Ensure source data is always present.',
        severity: 'warning',
      });
    }

    return errors;
  }

  /**
   * Generate SQL CREATE TABLE statement from mappings
   */
  generateCreateTableSQL(schema: string, table: string, mappings: ColumnMapping[]): string {
    const columnDefs = mappings.map((mapping) => {
      let def = `"${mapping.destinationColumn}" ${mapping.dataType}`;

      if (!mapping.nullable) {
        def += ' NOT NULL';
      }

      if (mapping.defaultValue) {
        def += ` DEFAULT ${mapping.defaultValue}`;
      }

      return def;
    });

    // Add primary key constraint
    const primaryKeys = mappings
      .filter((m) => m.isPrimaryKey)
      .map((m) => `"${m.destinationColumn}"`);

    if (primaryKeys.length > 0) {
      columnDefs.push(`PRIMARY KEY (${primaryKeys.join(', ')})`);
    }

    return `
CREATE TABLE IF NOT EXISTS "${schema}"."${table}" (
  ${columnDefs.join(',\n  ')}
);
    `.trim();
  }

  /**
   * Generate SQL INSERT statement with column mappings
   */
  generateInsertSQL(
    schema: string,
    table: string,
    mappings: ColumnMapping[],
    writeMode: 'append' | 'upsert',
  ): string {
    const columns = mappings.map((m) => `"${m.destinationColumn}"`);
    const placeholders = mappings.map((_, i) => `$${i + 1}`);

    let sql = `
INSERT INTO "${schema}"."${table}" (${columns.join(', ')})
VALUES (${placeholders.join(', ')})
    `.trim();

    if (writeMode === 'upsert') {
      const primaryKeys = mappings
        .filter((m) => m.isPrimaryKey)
        .map((m) => `"${m.destinationColumn}"`);

      if (primaryKeys.length > 0) {
        const updateCols = mappings
          .filter((m) => !m.isPrimaryKey)
          .map((m) => `"${m.destinationColumn}" = EXCLUDED."${m.destinationColumn}"`)
          .join(', ');

        sql += `
ON CONFLICT (${primaryKeys.join(', ')})
DO UPDATE SET ${updateCols}`;
      }
    }

    return sql;
  }

  /**
   * Find best matching column name
   */
  private findBestMatch(sourceName: string, destinationColumns: ColumnInfo[]): ColumnInfo | null {
    // Exact match
    let match = destinationColumns.find((col) => col.name === sourceName);
    if (match) return match;

    // Case-insensitive match
    match = destinationColumns.find((col) => col.name.toLowerCase() === sourceName.toLowerCase());
    if (match) return match;

    // Snake_case conversion (firstName -> first_name)
    const snakeCaseName = this.toSnakeCase(sourceName);
    match = destinationColumns.find((col) => col.name === snakeCaseName);
    if (match) return match;

    // CamelCase conversion (first_name -> firstName)
    const camelCaseName = this.toCamelCase(sourceName);
    match = destinationColumns.find((col) => col.name === camelCaseName);
    if (match) return match;

    return null;
  }

  /**
   * Map source PostgreSQL type to destination PostgreSQL type
   */
  private mapSourceTypeToPostgres(sourceType: string, sourceCol: ColumnInfo): string {
    const typeUpper = sourceType.toUpperCase();

    // Direct PostgreSQL type mappings
    const directMappings: Record<string, string> = {
      INTEGER: 'INTEGER',
      BIGINT: 'BIGINT',
      SMALLINT: 'SMALLINT',
      SERIAL: 'INTEGER',
      BIGSERIAL: 'BIGINT',
      NUMERIC: 'NUMERIC',
      DECIMAL: 'NUMERIC',
      REAL: 'REAL',
      'DOUBLE PRECISION': 'DOUBLE PRECISION',
      MONEY: 'NUMERIC(19,4)',
      TEXT: 'TEXT',
      VARCHAR: sourceCol.maxLength ? `VARCHAR(${sourceCol.maxLength})` : 'VARCHAR(255)',
      CHAR: sourceCol.maxLength ? `CHAR(${sourceCol.maxLength})` : 'CHAR(1)',
      BOOLEAN: 'BOOLEAN',
      DATE: 'DATE',
      TIME: 'TIME',
      TIMESTAMP: 'TIMESTAMP',
      TIMESTAMPTZ: 'TIMESTAMPTZ',
      INTERVAL: 'INTERVAL',
      UUID: 'UUID',
      JSON: 'JSON',
      JSONB: 'JSONB',
      BYTEA: 'BYTEA',
      ARRAY: 'JSONB', // Convert arrays to JSONB for simplicity
    };

    // Check for array types
    if (sourceCol.isArray) {
      return 'JSONB';
    }

    // Check for JSONB
    if (sourceCol.isJsonb) {
      return 'JSONB';
    }

    // Check for enum
    if (sourceCol.isEnum) {
      return 'VARCHAR(255)'; // Convert enums to VARCHAR
    }

    return directMappings[typeUpper] || 'TEXT';
  }

  /**
   * Infer type from sample values
   */
  private inferTypeFromValues(columnName: string, values: any[]): TypeInferenceResult {
    if (values.length === 0) {
      return {
        column: columnName,
        inferredType: 'TEXT',
        confidence: 0.5,
        alternatives: ['VARCHAR(255)', 'JSONB'],
      };
    }

    // Check if all values are numbers
    if (values.every((v) => typeof v === 'number')) {
      const hasDecimals = values.some((v) => v % 1 !== 0);
      return {
        column: columnName,
        inferredType: hasDecimals ? 'NUMERIC' : 'INTEGER',
        confidence: 1.0,
        alternatives: hasDecimals ? ['REAL', 'DOUBLE PRECISION'] : ['BIGINT'],
      };
    }

    // Check if all values are booleans
    if (values.every((v) => typeof v === 'boolean')) {
      return {
        column: columnName,
        inferredType: 'BOOLEAN',
        confidence: 1.0,
        alternatives: [],
      };
    }

    // Check if all values are dates
    if (values.every((v) => v instanceof Date || this.isDateString(v))) {
      return {
        column: columnName,
        inferredType: 'TIMESTAMPTZ',
        confidence: 0.9,
        alternatives: ['TIMESTAMP', 'DATE'],
      };
    }

    // Check if all values are objects/arrays
    if (values.every((v) => typeof v === 'object')) {
      return {
        column: columnName,
        inferredType: 'JSONB',
        confidence: 1.0,
        alternatives: ['JSON', 'TEXT'],
      };
    }

    // Default to TEXT
    const maxLength = Math.max(...values.map((v) => String(v).length));
    return {
      column: columnName,
      inferredType: maxLength > 255 ? 'TEXT' : `VARCHAR(${maxLength})`,
      confidence: 0.8,
      alternatives: ['TEXT', 'VARCHAR(255)'],
    };
  }

  /**
   * Convert to snake_case
   */
  private toSnakeCase(str: string): string {
    return str
      .replace(/([A-Z])/g, '_$1')
      .toLowerCase()
      .replace(/^_/, '');
  }

  /**
   * Convert to camelCase
   */
  private toCamelCase(str: string): string {
    return str.replace(/_([a-z])/g, (_, letter) => letter.toUpperCase());
  }

  /**
   * Check if string is a date
   */
  private isDateString(value: any): boolean {
    if (typeof value !== 'string') return false;
    const date = new Date(value);
    return !Number.isNaN(date.getTime());
  }
}
