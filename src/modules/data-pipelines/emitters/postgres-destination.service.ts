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
        preventDuplicates: boolean = true,
    ): Promise<WriteResult> {
        this.logger.log(
            `Writing ${data.length} rows to ${schema}.${table} in ${writeMode} mode${preventDuplicates ? ' (with duplicate prevention)' : ''}`,
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
            const totalRows = data.length;

            switch (writeMode) {
                case 'append':
                    rowsWritten = await this.appendData(client, schema, table, data, preventDuplicates);
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

            const rowsSkipped = totalRows - rowsWritten;

            return {
                rowsWritten,
                rowsSkipped: rowsSkipped > 0 ? rowsSkipped : 0,
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
     * Append mode: Simple INSERT with duplicate prevention
     */
    private async appendData(
        client: PoolClient,
        schema: string,
        table: string,
        data: any[],
        preventDuplicates: boolean = true,
    ): Promise<number> {
        let totalRowsWritten = 0;

        // Process in batches
        for (let i = 0; i < data.length; i += this.BATCH_SIZE) {
            const batch = data.slice(i, i + this.BATCH_SIZE);
            const rowsWritten = await this.insertBatch(client, schema, table, batch, preventDuplicates);
            totalRowsWritten += rowsWritten;

            this.logger.debug(
                `Inserted batch ${Math.floor(i / this.BATCH_SIZE) + 1}: ${rowsWritten} rows (${batch.length - rowsWritten} skipped as duplicates)`,
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
     * Get primary key columns for a table
     */
    private async getPrimaryKeys(
        client: PoolClient,
        schema: string,
        table: string,
    ): Promise<string[]> {
        const query = `
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE i.indisprimary
            AND n.nspname = $1
            AND c.relname = $2
            ORDER BY a.attnum
        `;

        const result = await client.query(query, [schema, table]);
        return result.rows.map((row) => row.attname);
    }

    /**
     * Check if a column has PRIMARY KEY or UNIQUE constraint
     * CRITICAL: Required for ON CONFLICT to work
     * Public method - called from pipeline service for validation
     */
    async hasUniqueConstraint(
        client: PoolClient,
        schema: string,
        table: string,
        column: string,
    ): Promise<boolean> {
        const query = `
            SELECT COUNT(*) as count
            FROM (
                -- Check PRIMARY KEY constraints
                SELECT 1
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE i.indisprimary
                AND n.nspname = $1
                AND c.relname = $2
                AND a.attname = $3
                
                UNION
                
                -- Check UNIQUE constraints
                SELECT 1
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE i.indisunique
                AND i.indisprimary = false
                AND n.nspname = $1
                AND c.relname = $2
                AND a.attname = $3
                AND array_length(i.indkey, 1) = 1  -- Single column constraint
            ) constraints
        `;

        const result = await client.query(query, [schema, table, column]);
        return parseInt(result.rows[0]?.count || '0', 10) > 0;
    }

    /**
     * Create UNIQUE constraint on a column if it doesn't exist
     * CRITICAL: Required for ON CONFLICT to work
     * Public method - called from pipeline service for validation
     */
    async ensureUniqueConstraint(
        client: PoolClient,
        schema: string,
        table: string,
        column: string,
    ): Promise<void> {
        // Check if constraint already exists
        const hasConstraint = await this.hasUniqueConstraint(client, schema, table, column);
        
        if (hasConstraint) {
            this.logger.log(
                `UNIQUE or PRIMARY KEY constraint already exists on ${schema}.${table}.${column}`,
            );
            return;
        }

        // Create UNIQUE constraint
        const constraintName = `${table}_${column}_unique`;
        const sql = `
            ALTER TABLE ${this.quoteIdentifier(schema)}.${this.quoteIdentifier(table)}
            ADD CONSTRAINT ${this.quoteIdentifier(constraintName)}
            UNIQUE (${this.quoteIdentifier(column)})
        `;

        try {
            await client.query(sql);
            this.logger.log(
                `Created UNIQUE constraint '${constraintName}' on ${schema}.${table}.${column} for upsert operations`,
            );
        } catch (error) {
            const errorMessage = error instanceof Error ? error.message : 'Unknown error';
            this.logger.error(
                `Failed to create UNIQUE constraint on ${schema}.${table}.${column}: ${errorMessage}`,
            );
            throw new Error(
                `Cannot create UNIQUE constraint on ${schema}.${table}.${column}: ${errorMessage}. ` +
                `This is required for upsert operations. Please ensure the column can have a unique constraint.`,
            );
        }
    }

    /**
     * Insert batch of rows with duplicate prevention
     */
    private async insertBatch(
        client: PoolClient,
        schema: string,
        table: string,
        batch: any[],
        preventDuplicates: boolean = true,
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

        let sql = `
      INSERT INTO ${this.quoteIdentifier(schema)}.${this.quoteIdentifier(table)}
      (${columns.map((c) => this.quoteIdentifier(c)).join(', ')})
      VALUES ${placeholders.join(', ')}
    `;

        // Add ON CONFLICT DO NOTHING to prevent duplicates if primary keys exist
        if (preventDuplicates) {
            try {
                const primaryKeys = await this.getPrimaryKeys(client, schema, table);
                if (primaryKeys.length > 0) {
                    // Use only the first primary key (there should be only one)
                    const primaryKey = primaryKeys[0];
                    sql += `
      ON CONFLICT (${this.quoteIdentifier(primaryKey)})
      DO NOTHING
    `;
                    this.logger.debug(
                        `Using primary key '${primaryKey}' for duplicate prevention on ${schema}.${table}`,
                    );
                } else {
                    this.logger.warn(
                        `No primary key found for ${schema}.${table} - duplicate prevention disabled. Please ensure a primary key is defined.`,
                    );
                }
            } catch (error) {
                // If we can't get primary keys, log warning but continue with regular insert
                this.logger.warn(
                    `Could not get primary keys for ${schema}.${table}: ${error instanceof Error ? error.message : 'Unknown error'}. Inserting without duplicate prevention.`,
                );
            }
        }

        const result = await this.executeWithRetry(client, sql, values);
        // Return the number of rows actually inserted (affected rows)
        // Note: With ON CONFLICT DO NOTHING, result.rowCount will be the number of rows inserted
        return (result as any).rowCount || batch.length;
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
     * Returns result with rowCount for duplicate detection
     * 
     * CRITICAL: Does NOT retry on transaction errors (25P02) - these indicate
     * the transaction is aborted and must be rolled back immediately
     */
    private async executeWithRetry(
        client: PoolClient,
        sql: string,
        params: any[],
    ): Promise<any> {
        let lastError: Error | null = null;

        for (let attempt = 1; attempt <= this.MAX_RETRIES; attempt++) {
            try {
                const result = await client.query(sql, params);
                return result;
            } catch (error) {
                lastError = error instanceof Error ? error : new Error('Unknown error');
                
                // CRITICAL: Check for transaction aborted error (25P02)
                // This means the transaction is in an aborted state and MUST be rolled back
                // Do NOT retry - immediately throw to trigger ROLLBACK
                const errorCode = (error as any)?.code;
                const errorMessage = lastError.message || '';
                
                if (errorCode === '25P02' || errorMessage.includes('current transaction is aborted')) {
                    this.logger.error(
                        `Transaction aborted (25P02) - cannot retry. Original error: ${errorMessage}. This indicates a previous query failed and the transaction must be rolled back.`,
                    );
                    // Immediately throw - do not retry
                    throw new Error(
                        `Transaction aborted: ${errorMessage}. Previous query in transaction failed. Transaction must be rolled back.`,
                    );
                }
                
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
        // Find the PRIMARY KEY column - ensure only ONE primary key
        const primaryKeyColumn = columns.find(col => col.isPrimaryKey);
        const hasIdColumn = columns.some(col => col.destinationColumn.toLowerCase() === 'id');
        
        const columnDefs: string[] = [];
        let primaryKeyName: string | null = null;
        
        // If no primary key is explicitly marked, use 'id' if it exists, or mark the first column
        if (!primaryKeyColumn) {
            if (hasIdColumn) {
                // Use 'id' as primary key
                primaryKeyName = 'id';
            } else {
                // Find first column that could be a primary key (id-like fields)
                const idLikeColumn = columns.find(col => 
                    col.destinationColumn.toLowerCase().endsWith('_id') || 
                    col.destinationColumn.toLowerCase() === 'id'
                );
                if (idLikeColumn) {
                    primaryKeyName = idLikeColumn.destinationColumn;
                } else if (columns.length > 0) {
                    // Use first column as primary key as last resort
                    primaryKeyName = columns[0].destinationColumn;
                }
            }
        } else {
            primaryKeyName = primaryKeyColumn.destinationColumn;
        }
        
        // Add all columns
        columns.forEach((col) => {
            let def = `${this.quoteIdentifier(col.destinationColumn)} ${col.dataType}`;

            if (!col.nullable) {
                def += ' NOT NULL';
            }

            if (col.defaultValue) {
                def += ` DEFAULT ${col.defaultValue}`;
            } else if (col.destinationColumn === primaryKeyName && col.dataType === 'UUID') {
                // Add default UUID generation for primary key UUID columns
                def += ' DEFAULT gen_random_uuid()';
            }

            // DO NOT add PRIMARY KEY in column definition - we'll add it as a constraint
            columnDefs.push(def);
        });

        // Add PRIMARY KEY constraint - ONLY ONE primary key
        if (primaryKeyName) {
            columnDefs.push(
                `PRIMARY KEY (${this.quoteIdentifier(primaryKeyName)})`,
            );
            this.logger.log(
                `Creating table ${schema}.${table} with primary key: ${primaryKeyName}`,
            );
        } else {
            this.logger.warn(
                `No primary key defined for table ${schema}.${table} - duplicate prevention may not work`,
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
