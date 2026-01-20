/**
 * Snowflake Source Handler
 * Handles data collection and schema discovery for Snowflake
 */

import { Logger } from '@nestjs/common';
import { ColumnInfo, DataSourceType } from '../../types/common.types';
import {
  BaseSourceHandler,
  CollectParams,
  CollectResult,
  ConnectionTestResult,
  PipelineSourceSchemaWithConfig,
  SchemaInfo,
} from '../../types/source-handler.types';

export class SnowflakeHandler extends BaseSourceHandler {
  readonly type = DataSourceType.SNOWFLAKE;
  private readonly logger = new Logger(SnowflakeHandler.name);

  /**
   * Test Snowflake connection
   */
  async testConnection(connectionConfig: any): Promise<ConnectionTestResult> {
    return new Promise((resolve, reject) => {
      import('snowflake-sdk').then((snowflake) => {
        const connection = snowflake.createConnection({
          account: connectionConfig.account,
          username: connectionConfig.username,
          password: connectionConfig.password,
          warehouse: connectionConfig.warehouse,
          database: connectionConfig.database,
          schema: connectionConfig.schema,
          role: connectionConfig.role,
        });

        connection.connect((err, conn) => {
          if (err) {
            resolve({
              success: false,
              message: `Connection failed: ${err.message}`,
            });
            return;
          }

          conn.execute({
            sqlText: 'SELECT CURRENT_VERSION() as version',
            complete: (err, _stmt, rows) => {
              connection.destroy(() => {});
              
              if (err) {
                resolve({
                  success: false,
                  message: `Connection test failed: ${err.message}`,
                });
                return;
              }

              const version = rows?.[0]?.VERSION || 'Unknown';
              resolve({
                success: true,
                message: 'Connection successful',
                details: {
                  serverInfo: { version },
                },
              });
            },
          });
        });
      }).catch((error) => {
        resolve({
          success: false,
          message: `Failed to load Snowflake SDK: ${error.message}`,
        });
      });
    });
  }

  /**
   * Discover Snowflake schema
   */
  async discoverSchema(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
  ): Promise<SchemaInfo> {
    return new Promise((resolve, reject) => {
      import('snowflake-sdk').then((snowflake) => {
        const connection = snowflake.createConnection({
          account: connectionConfig.account,
          username: connectionConfig.username,
          password: connectionConfig.password,
          warehouse: connectionConfig.warehouse,
          database: connectionConfig.database,
          schema: connectionConfig.schema,
          role: connectionConfig.role,
        });

        connection.connect((err, conn) => {
          if (err) {
            reject(err);
            return;
          }

          const schemaName = sourceSchema.config.schema || connectionConfig.schema || sourceSchema.sourceSchema;
          const tableName = sourceSchema.config.table || sourceSchema.config.tableName || sourceSchema.sourceTable;

          this.logger.log(`Discovering schema for ${schemaName}.${tableName}`);

          // Get columns
          const columnsQuery = `
            SELECT 
              COLUMN_NAME,
              DATA_TYPE,
              IS_NULLABLE,
              COLUMN_DEFAULT,
              CHARACTER_MAXIMUM_LENGTH,
              NUMERIC_PRECISION,
              NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
          `;

          conn.execute({
            sqlText: columnsQuery,
            binds: [schemaName, tableName],
            complete: (err, _stmt, rows) => {
              if (err) {
                connection.destroy(() => {});
                reject(err);
                return;
              }

              // Get primary keys
              const pkQuery = `
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                WHERE tc.TABLE_SCHEMA = ? 
                  AND tc.TABLE_NAME = ?
                  AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
              `;

              conn.execute({
                sqlText: pkQuery,
                binds: [schemaName, tableName],
                complete: (err2, _stmt2, pkRows) => {
                  if (err2) {
                    connection.destroy(() => {});
                    reject(err2);
                    return;
                  }

                  // Get row count
                  const countQuery = `SELECT COUNT(*) as COUNT FROM "${schemaName}"."${tableName}"`;
                  conn.execute({
                    sqlText: countQuery,
                    complete: (err3, _stmt3, countRows) => {
                      connection.destroy(() => {});

                      if (err3) {
                        reject(err3);
                        return;
                      }

                      const columns: ColumnInfo[] = (rows || []).map((row: any) => ({
                        name: row.COLUMN_NAME,
                        dataType: this.normalizeSnowflakeType(row.DATA_TYPE),
                        nullable: row.IS_NULLABLE === 'YES',
                        defaultValue: row.COLUMN_DEFAULT,
                        maxLength: row.CHARACTER_MAXIMUM_LENGTH,
                        precision: row.NUMERIC_PRECISION,
                        scale: row.NUMERIC_SCALE,
                        isPrimaryKey: (pkRows || []).some((pk: any) => pk.COLUMN_NAME === row.COLUMN_NAME),
                      }));

                      const primaryKeys = (pkRows || []).map((row: any) => row.COLUMN_NAME);
                      const estimatedRowCount = countRows?.[0]?.COUNT ? Number(countRows[0].COUNT) : undefined;

                      this.logger.log(`Found ${columns.length} columns, ${primaryKeys.length} primary keys`);

                      resolve({
                        columns,
                        primaryKeys,
                        estimatedRowCount,
                        isRelational: true,
                        sourceType: 'snowflake',
                        entityName: tableName || undefined,
                      });
                    },
                  });
                },
              });
            },
          });
        });
      }).catch(reject);
    });
  }

  /**
   * Collect data from Snowflake
   */
  async collect(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    params: CollectParams,
  ): Promise<CollectResult> {
    return new Promise((resolve, reject) => {
      import('snowflake-sdk').then((snowflake) => {
        const connection = snowflake.createConnection({
          account: connectionConfig.account,
          username: connectionConfig.username,
          password: connectionConfig.password,
          warehouse: connectionConfig.warehouse,
          database: connectionConfig.database,
          schema: connectionConfig.schema,
          role: connectionConfig.role,
        });

        connection.connect((err, conn) => {
          if (err) {
            reject(err);
            return;
          }

          const schemaName = sourceSchema.config.schema || connectionConfig.schema || sourceSchema.sourceSchema;
          const tableName = sourceSchema.config.table || sourceSchema.config.tableName || sourceSchema.sourceTable;

          let query = sourceSchema.sourceQuery;
          if (!query) {
            query = `SELECT * FROM "${schemaName}"."${tableName}"`;
            
            // Add incremental filter if provided
            if (params.incrementalColumn && params.lastSyncValue) {
              query += ` WHERE "${params.incrementalColumn}" > ?`;
            }
          }

          // Add pagination
          query += ` LIMIT ? OFFSET ?`;

          this.logger.log(`Executing Snowflake query with limit ${params.limit}, offset ${params.offset}`);

          const binds: any[] = [];
          if (params.incrementalColumn && params.lastSyncValue && !sourceSchema.sourceQuery) {
            binds.push(params.lastSyncValue);
          }
          binds.push(params.limit, params.offset);

          conn.execute({
            sqlText: query,
            binds,
            complete: (err, _stmt, rows) => {
              connection.destroy(() => {});

              if (err) {
                reject(err);
                return;
              }

              // Convert Snowflake rows to plain objects
              const plainRows = (rows || []).map((row: any) => {
                const obj: any = {};
                for (const [key, value] of Object.entries(row)) {
                  obj[key] = value;
                }
                return obj;
              });

              const hasMore = plainRows.length === params.limit;
              const nextCursor = hasMore ? String(params.offset + params.limit) : undefined;

              // Get total count if needed (for future use)
              let totalRows: number | undefined;
              // Note: includeTotal is not in CollectParams, but we can add it if needed

              resolve({
                rows: plainRows,
                totalRows,
                nextCursor,
                hasMore,
              });
            },
          });
        });
      }).catch(reject);
    });
  }

  /**
   * Stream data using async generator for large datasets
   * Note: Snowflake SDK is callback-based, so this is a simplified implementation
   */
  async *collectStream(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    params: CollectParams,
  ): AsyncIterable<any[]> {
    // For now, use regular collect in batches
    // A full async generator implementation would require wrapping callbacks
    const batchSize = params.batchSize || 1000;
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const result = await this.collect(sourceSchema, connectionConfig, {
        ...params,
        limit: batchSize,
        offset,
      });

      if (result.rows.length > 0) {
        yield result.rows;
        offset += result.rows.length;
      }

      hasMore = result.hasMore || false;
    }
  }

  /**
   * Legacy streaming implementation (not used, kept for reference)
   */
  private async *collectStreamLegacy(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    params: CollectParams,
  ): AsyncIterable<any[]> {
    const snowflake = await import('snowflake-sdk');
    const batchSize = params.batchSize || 1000;

    return new Promise<void>((resolve, reject) => {
      const connection = snowflake.createConnection({
        account: connectionConfig.account,
        username: connectionConfig.username,
        password: connectionConfig.password,
        warehouse: connectionConfig.warehouse,
        database: connectionConfig.database,
        schema: connectionConfig.schema,
        role: connectionConfig.role,
      });

      connection.connect((err, conn) => {
        if (err) {
          reject(err);
          return;
        }

        const schemaName = sourceSchema.config.schema || connectionConfig.schema || sourceSchema.sourceSchema;
        const tableName = sourceSchema.config.table || sourceSchema.config.tableName || sourceSchema.sourceTable;

        let offset = 0;
        let hasMore = true;

        const fetchBatch = () => {
          if (!hasMore) {
            connection.destroy(() => {});
            resolve();
            return;
          }

          let query = sourceSchema.sourceQuery;
          if (!query) {
            query = `SELECT * FROM "${schemaName}"."${tableName}"`;
          }
          query += ` LIMIT ${batchSize} OFFSET ${offset}`;

          conn.execute({
            sqlText: query,
            complete: (err, _stmt, rows) => {
              if (err) {
                connection.destroy(() => {});
                reject(err);
                return;
              }

              if (rows && rows.length > 0) {
                const plainRows = rows.map((row: any) => {
                  const obj: any = {};
                  for (const [key, value] of Object.entries(row)) {
                    obj[key] = value;
                  }
                  return obj;
                });
                
                // Yield batch (this is a simplified version - actual async generator would need different approach)
                offset += plainRows.length;
                hasMore = rows.length === batchSize;
                
                // Note: This is a limitation - Snowflake SDK is callback-based
                // For true streaming, would need to wrap in a different pattern
                fetchBatch();
              } else {
                hasMore = false;
                connection.destroy(() => {});
                resolve();
              }
            },
          });
        };

        fetchBatch();
      });
    });
  }

  /**
   * Get sample data for preview
   */
  async getSampleData(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    sampleSize: number = 10,
  ): Promise<any[]> {
    const result = await this.collect(sourceSchema, connectionConfig, {
      limit: sampleSize,
      offset: 0,
    });
    return result.rows;
  }

  /**
   * Normalize Snowflake type to standard type
   */
  private normalizeSnowflakeType(sfType: string): string {
    const typeMap: Record<string, string> = {
      'NUMBER': 'decimal',
      'DECIMAL': 'decimal',
      'NUMERIC': 'decimal',
      'INT': 'integer',
      'INTEGER': 'integer',
      'BIGINT': 'bigint',
      'SMALLINT': 'smallint',
      'TINYINT': 'smallint',
      'BYTEINT': 'smallint',
      'FLOAT': 'float',
      'FLOAT4': 'float',
      'FLOAT8': 'double',
      'DOUBLE': 'double',
      'DOUBLE PRECISION': 'double',
      'REAL': 'float',
      'VARCHAR': 'varchar',
      'CHAR': 'char',
      'CHARACTER': 'char',
      'STRING': 'string',
      'TEXT': 'text',
      'DATE': 'date',
      'TIME': 'time',
      'TIMESTAMP': 'timestamp',
      'TIMESTAMP_NTZ': 'timestamp',
      'TIMESTAMP_LTZ': 'timestamptz',
      'TIMESTAMP_TZ': 'timestamptz',
      'BOOLEAN': 'boolean',
      'BOOL': 'boolean',
      'BINARY': 'binary',
      'VARBINARY': 'binary',
      'VARIANT': 'json',
      'OBJECT': 'object',
      'ARRAY': 'array',
      'GEOGRAPHY': 'string',
      'GEOMETRY': 'string',
    };

    const upperType = sfType.toUpperCase().split('(')[0].trim();
    return typeMap[upperType] || 'string';
  }
}
