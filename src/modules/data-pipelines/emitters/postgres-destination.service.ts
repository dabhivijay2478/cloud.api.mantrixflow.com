/**
 * PostgreSQL Destination Service
 * Handles write operations to PostgreSQL destinations
 */

import { Injectable, Logger, BadRequestException } from '@nestjs/common';
import { PoolClient } from 'pg';
import {
    ColumnMapping,
    WriteResult,
    WriteError,
    SchemaValidationResult,
    TypeMismatch,
    TableStats,
} from '../../data-sources/postgres/postgres.types';

@Injectable()
export class PostgresDestinationService {
    private readonly logger = new Logger(PostgresDestinationService.name);
    private readonly BATCH_SIZE = 1000;
    private readonly MAX_RETRIES = 3;
    private readonly RETRY_DELAY_MS = 1000;

    /**
     * Create destination table if not exists
     * Auto-generates schema from source columns
     */
    async createDestinationTable(
        client: PoolClient,
        schema: string,
        table: string,
        columns: ColumnMapping[],
    ): Promise<void> {
        this.logger.log(
            `Creating destination table ${schema}.${table} with ${columns.length} columns`,
        );

        const sql = this.generateCreateTableSQL(schema, table, columns);

        try {
            await client.query(sql);
            this.logger.log(`Successfully created table ${schema}.${table}`);
        } catch (error) {
            this.logger.error(
                `Failed to create table ${schema}.${table}: ${error instanceof Error ? error.message : 'Unknown error'}`,
            );
            throw error;
        }
    }

    /**
     * Check if destination table exists
     */
    async tableExists(
        client: PoolClient,
        schema: string,
        table: string,
    ): Promise<boolean> {
        const query = `
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = $1 
        AND table_name = $2
      ) as exists
    `;

        const result = await client.query(query, [schema, table]);
        return result.rows[0]?.exists || false;
    }

    /**
     * Write data to destination (batch insert)
     * @param writeMode - 'append', 'upsert', 'replace'
     */
    async writeData(
        client: PoolClient,
        schema: string,
        table: string,
        data: any[],
        writeMode: 'append' | 'upsert' | 'replace',
        upsertKey?: string[],
    ): Promise<WriteResult> {
        this.logger.log(
            `Writing ${data.length} rows to ${schema}.${table} in ${writeMode} mode`,
        );

        if (data.length === 0) {
            return {
                rowsWritten: 0,
                rowsSkipped: 0,
                rowsFailed: 0,
                errors: [],
            };
        }

        try {
            let rowsWritten = 0;

            switch (writeMode) {
                case 'append':
                    rowsWritten = await this.appendData(client, schema, table, data);
                    break;
                case 'upsert':
                    if (!upsertKey || upsertKey.length === 0) {
                        throw new BadRequestException(
                            'Upsert key is required for upsert mode',
                        );
                    }
                    rowsWritten = await this.upsertData(
                        client,
                        schema,
                        table,
                        data,
                        upsertKey,
                    );
                    break;
                case 'replace':
                    rowsWritten = await this.replaceData(client, schema, table, data);
                    break;
                default:
                    throw new BadRequestException(`Invalid write mode: ${writeMode}`);
            }

            return {
                rowsWritten,
                rowsSkipped: 0,
                rowsFailed: 0,
                errors: [],
            };
        } catch (error) {
            this.logger.error(
                `Failed to write data: ${error instanceof Error ? error.message : 'Unknown error'}`,
            );
            throw error;
        }
    }

    /**
     * Append mode: Simple INSERT
     */
    private async appendData(
        client: PoolClient,
        schema: string,
        table: string,
        data: any[],
    ): Promise<number> {
        let totalRowsWritten = 0;

        // Process in batches
        for (let i = 0; i < data.length; i += this.BATCH_SIZE) {
            const batch = data.slice(i, i + this.BATCH_SIZE);
            const rowsWritten = await this.insertBatch(client, schema, table, batch);
            totalRowsWritten += rowsWritten;

            this.logger.debug(
                `Inserted batch ${Math.floor(i / this.BATCH_SIZE) + 1}: ${rowsWritten} rows`,
            );
        }

        return totalRowsWritten;
    }

    /**
     * Upsert mode: INSERT ... ON CONFLICT DO UPDATE
     */
    private async upsertData(
        client: PoolClient,
        schema: string,
        table: string,
        data: any[],
        upsertKey: string[],
    ): Promise<number> {
        let totalRowsWritten = 0;

        // Process in batches
        for (let i = 0; i < data.length; i += this.BATCH_SIZE) {
            const batch = data.slice(i, i + this.BATCH_SIZE);
            const rowsWritten = await this.upsertBatch(
                client,
                schema,
                table,
                batch,
                upsertKey,
            );
            totalRowsWritten += rowsWritten;

            this.logger.debug(
                `Upserted batch ${Math.floor(i / this.BATCH_SIZE) + 1}: ${rowsWritten} rows`,
            );
        }

        return totalRowsWritten;
    }

    /**
     * Replace mode: TRUNCATE + INSERT
     */
    private async replaceData(
        client: PoolClient,
        schema: string,
        table: string,
        data: any[],
    ): Promise<number> {
        // Truncate table first
        await client.query(
            `TRUNCATE TABLE ${this.quoteIdentifier(schema)}.${this.quoteIdentifier(table)}`,
        );
        this.logger.log(`Truncated table ${schema}.${table}`);

        // Then insert all data
        return await this.appendData(client, schema, table, data);
    }

    /**
     * Insert batch of rows
     */
    private async insertBatch(
        client: PoolClient,
        schema: string,
        table: string,
        batch: any[],
    ): Promise<number> {
        if (batch.length === 0) return 0;

        const columns = Object.keys(batch[0]);
        const values: any[] = [];
        const placeholders: string[] = [];

        batch.forEach((row, rowIndex) => {
            const rowPlaceholders: string[] = [];
            columns.forEach((col, colIndex) => {
                const paramIndex = rowIndex * columns.length + colIndex + 1;
                rowPlaceholders.push(`$${paramIndex}`);
                values.push(row[col]);
            });
            placeholders.push(`(${rowPlaceholders.join(', ')})`);
        });

        const sql = `
      INSERT INTO ${this.quoteIdentifier(schema)}.${this.quoteIdentifier(table)}
      (${columns.map((c) => this.quoteIdentifier(c)).join(', ')})
      VALUES ${placeholders.join(', ')}
    `;

        await this.executeWithRetry(client, sql, values);
        return batch.length;
    }

    /**
     * Upsert batch of rows
     */
    private async upsertBatch(
        client: PoolClient,
        schema: string,
        table: string,
        batch: any[],
        upsertKey: string[],
    ): Promise<number> {
        if (batch.length === 0) return 0;

        const columns = Object.keys(batch[0]);
        const values: any[] = [];
        const placeholders: string[] = [];

        batch.forEach((row, rowIndex) => {
            const rowPlaceholders: string[] = [];
            columns.forEach((col, colIndex) => {
                const paramIndex = rowIndex * columns.length + colIndex + 1;
                rowPlaceholders.push(`$${paramIndex}`);
                values.push(row[col]);
            });
            placeholders.push(`(${rowPlaceholders.join(', ')})`);
        });

        // Build UPDATE clause (exclude upsert key columns)
        const updateColumns = columns.filter((col) => !upsertKey.includes(col));
        const updateClause = updateColumns
            .map((col) => `${this.quoteIdentifier(col)} = EXCLUDED.${this.quoteIdentifier(col)}`)
            .join(', ');

        const sql = `
      INSERT INTO ${this.quoteIdentifier(schema)}.${this.quoteIdentifier(table)}
      (${columns.map((c) => this.quoteIdentifier(c)).join(', ')})
      VALUES ${placeholders.join(', ')}
      ON CONFLICT (${upsertKey.map((k) => this.quoteIdentifier(k)).join(', ')})
      DO UPDATE SET ${updateClause}
    `;

        await this.executeWithRetry(client, sql, values);
        return batch.length;
    }

    /**
     * Execute query with retry logic
     */
    private async executeWithRetry(
        client: PoolClient,
        sql: string,
        params: any[],
    ): Promise<void> {
        let lastError: Error | null = null;

        for (let attempt = 1; attempt <= this.MAX_RETRIES; attempt++) {
            try {
                await client.query(sql, params);
                return;
            } catch (error) {
                lastError = error instanceof Error ? error : new Error('Unknown error');
                this.logger.warn(
                    `Query failed (attempt ${attempt}/${this.MAX_RETRIES}): ${lastError.message}`,
                );

                if (attempt < this.MAX_RETRIES) {
                    await this.sleep(this.RETRY_DELAY_MS * attempt);
                }
            }
        }

        throw lastError;
    }

    /**
     * Validate destination schema matches source
     */
    async validateSchema(
        client: PoolClient,
        schema: string,
        table: string,
        expectedColumns: ColumnMapping[],
    ): Promise<SchemaValidationResult> {
        const query = `
      SELECT 
        column_name,
        data_type,
        is_nullable,
        character_maximum_length
      FROM information_schema.columns
      WHERE table_schema = $1 AND table_name = $2
      ORDER BY ordinal_position
    `;

        const result = await client.query(query, [schema, table]);
        const actualColumns = result.rows;

        const missingColumns: string[] = [];
        const typeMismatches: TypeMismatch[] = [];

        expectedColumns.forEach((expected) => {
            const actual = actualColumns.find(
                (col: any) => col.column_name === expected.destinationColumn,
            );

            if (!actual) {
                missingColumns.push(expected.destinationColumn);
            } else {
                // Check type compatibility
                const actualType = actual.data_type.toUpperCase();
                const expectedType = expected.dataType.toUpperCase();

                if (!this.areTypesCompatible(actualType, expectedType)) {
                    typeMismatches.push({
                        column: expected.destinationColumn,
                        expectedType: expectedType,
                        actualType: actualType,
                    });
                }
            }
        });

        return {
            valid: missingColumns.length === 0 && typeMismatches.length === 0,
            missingColumns,
            typeMismatches,
        };
    }

    /**
     * Add missing columns to destination table
     */
    async addMissingColumns(
        client: PoolClient,
        schema: string,
        table: string,
        columns: ColumnMapping[],
    ): Promise<void> {
        for (const column of columns) {
            const sql = `
        ALTER TABLE ${this.quoteIdentifier(schema)}.${this.quoteIdentifier(table)}
        ADD COLUMN ${this.quoteIdentifier(column.destinationColumn)} ${column.dataType}
        ${column.nullable ? 'NULL' : 'NOT NULL'}
        ${column.defaultValue ? `DEFAULT ${column.defaultValue}` : ''}
      `;

            await client.query(sql);
            this.logger.log(
                `Added column ${column.destinationColumn} to ${schema}.${table}`,
            );
        }
    }

    /**
     * Get destination table statistics
     */
    async getTableStats(
        client: PoolClient,
        schema: string,
        table: string,
    ): Promise<TableStats> {
        const countQuery = `
      SELECT COUNT(*) as count
      FROM ${this.quoteIdentifier(schema)}.${this.quoteIdentifier(table)}
    `;

        const sizeQuery = `
      SELECT pg_total_relation_size($1 || '.' || $2) as size
    `;

        const [countResult, sizeResult] = await Promise.all([
            client.query(countQuery),
            client.query(sizeQuery, [schema, table]),
        ]);

        return {
            rowCount: parseInt(countResult.rows[0]?.count || '0'),
            sizeBytes: parseInt(sizeResult.rows[0]?.size || '0'),
            lastUpdated: new Date(),
        };
    }

    /**
     * Generate CREATE TABLE SQL
     */
    private generateCreateTableSQL(
        schema: string,
        table: string,
        columns: ColumnMapping[],
    ): string {
        const columnDefs = columns.map((col) => {
            let def = `${this.quoteIdentifier(col.destinationColumn)} ${col.dataType}`;

            if (!col.nullable) {
                def += ' NOT NULL';
            }

            if (col.defaultValue) {
                def += ` DEFAULT ${col.defaultValue}`;
            }

            return def;
        });

        // Add primary key constraint if any
        const primaryKeys = columns
            .filter((col) => col.isPrimaryKey)
            .map((col) => col.destinationColumn);

        if (primaryKeys.length > 0) {
            columnDefs.push(
                `PRIMARY KEY (${primaryKeys.map((k) => this.quoteIdentifier(k)).join(', ')})`,
            );
        }

        return `
      CREATE TABLE IF NOT EXISTS ${this.quoteIdentifier(schema)}.${this.quoteIdentifier(table)} (
        ${columnDefs.join(',\n        ')}
      )
    `;
    }

    /**
     * Check if types are compatible
     */
    private areTypesCompatible(
        actualType: string,
        expectedType: string,
    ): boolean {
        // Exact match
        if (actualType === expectedType) return true;

        // Common compatible types
        const compatibleTypes: Record<string, string[]> = {
            TEXT: ['VARCHAR', 'CHARACTER VARYING', 'CHAR', 'CHARACTER'],
            VARCHAR: ['TEXT', 'CHARACTER VARYING', 'CHAR', 'CHARACTER'],
            INTEGER: ['BIGINT', 'SMALLINT', 'INT', 'INT4'],
            BIGINT: ['INTEGER', 'SMALLINT', 'INT', 'INT8'],
            NUMERIC: ['DECIMAL', 'REAL', 'DOUBLE PRECISION'],
            TIMESTAMP: ['TIMESTAMPTZ', 'TIMESTAMP WITH TIME ZONE'],
            TIMESTAMPTZ: ['TIMESTAMP', 'TIMESTAMP WITHOUT TIME ZONE'],
        };

        return compatibleTypes[expectedType]?.includes(actualType) || false;
    }

    /**
     * Quote identifier for SQL safety
     */
    private quoteIdentifier(identifier: string): string {
        return `"${identifier.replace(/"/g, '""')}"`;
    }

    /**
     * Sleep utility
     */
    private sleep(ms: number): Promise<void> {
        return new Promise((resolve) => setTimeout(resolve, ms));
    }
}
