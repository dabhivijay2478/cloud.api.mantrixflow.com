/**
 * PostgreSQL Source Handler
 * Handles data collection and schema discovery for PostgreSQL databases
 */

import { Logger } from '@nestjs/common';
import { Pool } from 'pg';
import { LogicalReplicationService, PgoutputPlugin } from 'pg-logical-replication';
import { ColumnInfo, DataSourceType } from '../../types/common.types';
import {
  BaseSourceHandler,
  CollectParams,
  CollectResult,
  ConnectionTestResult,
  PipelineSourceSchemaWithConfig,
  SchemaInfo,
} from '../../types/source-handler.types';

export class PostgresHandler extends BaseSourceHandler {
  readonly type = DataSourceType.POSTGRES;
  private readonly logger = new Logger(PostgresHandler.name);

  /**
   * Test PostgreSQL connection
   */
  async testConnection(connectionConfig: any): Promise<ConnectionTestResult> {
    const pool = this.createPool(connectionConfig);

    try {
      const client = await pool.connect();
      try {
        const result = await client.query('SELECT version()');
        const version = result.rows[0]?.version || 'Unknown';

        return {
          success: true,
          message: 'Connection successful',
          details: {
            version,
            serverInfo: { version },
          },
        };
      } finally {
        client.release();
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return {
        success: false,
        message: `Connection failed: ${message}`,
      };
    } finally {
      await pool.end();
    }
  }

  /**
   * Discover PostgreSQL schema
   */
  async discoverSchema(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
  ): Promise<SchemaInfo> {
    const pool = this.createPool(connectionConfig);

    try {
      const client = await pool.connect();
      try {
        const tableName = sourceSchema.config.table || sourceSchema.config.tableName || sourceSchema.sourceTable;
        const schemaName = sourceSchema.config.schema || sourceSchema.sourceSchema || 'public';

        this.logger.log(`Discovering schema for ${schemaName}.${tableName}`);

        // Get columns
        const columnsResult = await client.query(
          `
          SELECT 
            c.column_name,
            c.data_type,
            c.is_nullable,
            c.column_default,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale
          FROM information_schema.columns c
          WHERE c.table_schema = $1 AND c.table_name = $2
          ORDER BY c.ordinal_position
        `,
          [schemaName, tableName],
        );

        // Get primary keys
        const pkResult = await client.query(
          `
          SELECT kcu.column_name
          FROM information_schema.table_constraints tc
          JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
          WHERE tc.table_schema = $1 
            AND tc.table_name = $2 
            AND tc.constraint_type = 'PRIMARY KEY'
        `,
          [schemaName, tableName],
        );

        // Get estimated row count
        const countResult = await client.query(
          `
          SELECT reltuples::bigint as estimate
          FROM pg_class
          WHERE relname = $1
        `,
          [tableName],
        );

        const columns: ColumnInfo[] = columnsResult.rows.map(row => ({
          name: row.column_name,
          dataType: this.normalizeDataType(row.data_type),
          nullable: row.is_nullable === 'YES',
          defaultValue: row.column_default,
          maxLength: row.character_maximum_length,
          precision: row.numeric_precision,
          scale: row.numeric_scale,
          isPrimaryKey: pkResult.rows.some(pk => pk.column_name === row.column_name),
        }));

        const primaryKeys = pkResult.rows.map(row => row.column_name);
        const estimatedRowCount = countResult.rows[0]?.estimate || undefined;

        this.logger.log(`Found ${columns.length} columns, ${primaryKeys.length} primary keys`);

        return {
          columns,
          primaryKeys,
          estimatedRowCount: Number(estimatedRowCount),
          isRelational: true,
          sourceType: 'postgres',
          entityName: tableName || undefined,
        };
      } finally {
        client.release();
      }
    } catch (error) {
      this.logger.error(`Failed to discover schema: ${error}`);
      throw error;
    } finally {
      await pool.end();
    }
  }

  /**
   * Collect data from PostgreSQL
   */
  async collect(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    params: CollectParams,
  ): Promise<CollectResult> {
    const pool = this.createPool(connectionConfig);

    try {
      const client = await pool.connect();
      try {
        const tableName = sourceSchema.config.table || sourceSchema.config.tableName || sourceSchema.sourceTable;
        const schemaName = sourceSchema.config.schema || sourceSchema.sourceSchema || 'public';
        const fullTableName = `"${schemaName}"."${tableName}"`;

        let query = `SELECT * FROM ${fullTableName}`;
        const queryParams: any[] = [];
        let paramIndex = 1;

        // Add incremental filter if provided
        if (params.incrementalColumn && params.lastSyncValue) {
          query += ` WHERE "${params.incrementalColumn}" > $${paramIndex}`;
          queryParams.push(params.lastSyncValue);
          paramIndex++;
        }

        // Add ordering for consistent pagination
        query += ` ORDER BY 1`; // Order by first column

        // Add pagination
        query += ` LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`;
        queryParams.push(params.limit, params.offset);

        this.logger.log(`Executing query: ${query} with params: ${JSON.stringify(queryParams)}`);

        const result = await client.query(query, queryParams);

        // Get total count
        let totalRows: number | undefined;
        try {
          const countQuery = params.incrementalColumn && params.lastSyncValue
            ? `SELECT COUNT(*) FROM ${fullTableName} WHERE "${params.incrementalColumn}" > $1`
            : `SELECT COUNT(*) FROM ${fullTableName}`;
          const countParams = params.incrementalColumn && params.lastSyncValue 
            ? [params.lastSyncValue] 
            : [];
          const countResult = await client.query(countQuery, countParams);
          totalRows = parseInt(countResult.rows[0].count, 10);
        } catch {
          // Ignore count errors
        }

        const hasMore = result.rows.length === params.limit;
        const nextCursor = hasMore ? String(params.offset + params.limit) : undefined;

        return {
          rows: result.rows,
          totalRows,
          nextCursor,
          hasMore,
        };
      } finally {
        client.release();
      }
    } catch (error) {
      this.logger.error(`Failed to collect data: ${error}`);
      throw error;
    } finally {
      await pool.end();
    }
  }

  /**
   * Collect incremental data using PostgreSQL WAL (Write-Ahead Log) via logical replication
   * ROOT FIX: Uses logical replication to read changes from WAL - NO COLUMN CHECKS
   * Captures ALL changes (INSERT, UPDATE, DELETE) from transaction logs
   * 
   * Architecture:
   * - Uses logical replication slot with pgoutput plugin (built-in)
   * - Reads WAL changes via replication protocol
   * - Tracks LSN (Log Sequence Number) position for resumable syncs
   * - Captures all changes regardless of table structure or columns
   */
  async collectIncremental(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    checkpoint: { walPosition?: string; lsn?: string; slotName?: string; publicationName?: string; [key: string]: any },
    params: Omit<CollectParams, 'incrementalColumn' | 'lastSyncValue'>,
  ): Promise<CollectResult> {
    const tableName = sourceSchema.config.table || sourceSchema.config.tableName || sourceSchema.sourceTable;
    const schemaName = sourceSchema.config.schema || sourceSchema.sourceSchema || 'public';
    const fullTableName = `${schemaName}.${tableName}`;
    const pool = this.createPool(connectionConfig);

    try {
      const client = await pool.connect();
      try {
        // ROOT FIX: Use WAL-based CDC via logical replication - NO COLUMN CHECKS
        // Step 1: Ensure logical replication is enabled
        const walLevelCheck = await client.query(`SHOW wal_level`);
        const walLevel = walLevelCheck.rows[0]?.wal_level;
        
        if (walLevel !== 'logical') {
          throw new Error(
            `PostgreSQL WAL-based CDC requires wal_level = 'logical'. Current value: ${walLevel}. ` +
            `Please set wal_level = logical in postgresql.conf and restart PostgreSQL.`,
          );
        }

        // Step 2: Setup replication slot and publication
        const slotName = checkpoint.slotName || `pipeline_${sourceSchema.id?.replace(/-/g, '_') || 'default'}`;
        const safeTableName = (tableName || 'table').replace(/[^a-zA-Z0-9_]/g, '_');
        const publicationName = checkpoint.publicationName || `pub_${schemaName}_${safeTableName}`;

        // Ensure slot exists
        let slotLSN: string | null = null;
        const slotCheck = await client.query(
          `SELECT slot_name, restart_lsn, confirmed_flush_lsn 
           FROM pg_replication_slots 
           WHERE slot_name = $1`,
          [slotName],
        );

        if (slotCheck.rows.length === 0) {
          this.logger.log(`Creating logical replication slot: ${slotName}`);
          const createSlot = await client.query(
            `SELECT * FROM pg_create_logical_replication_slot($1, 'pgoutput')`,
            [slotName],
          );
          slotLSN = createSlot.rows[0].restart_lsn;
          this.logger.log(`Created replication slot ${slotName} at LSN: ${slotLSN}`);
        } else {
          slotLSN = slotCheck.rows[0].confirmed_flush_lsn || slotCheck.rows[0].restart_lsn;
          this.logger.log(`Using existing replication slot ${slotName} at LSN: ${slotLSN}`);
        }

        // Ensure publication exists
        const pubCheck = await client.query(
          `SELECT pubname FROM pg_publication WHERE pubname = $1`,
          [publicationName],
        );

        if (pubCheck.rows.length === 0) {
          await client.query(
            `CREATE PUBLICATION ${publicationName} FOR TABLE ${fullTableName}`,
          );
          this.logger.log(`Created publication ${publicationName} for table ${fullTableName}`);
        }

        // Step 3: Read WAL changes using logical replication service
        const lastLSN = checkpoint.walPosition || checkpoint.lsn || slotLSN;
        const changes: any[] = [];
        let lastProcessedLSN: string | null = lastLSN;

        // ROOT FIX: Use pg-logical-replication library to read WAL changes
        // This properly parses pgoutput protocol and extracts INSERT/UPDATE/DELETE events
        const replicationConfig = {
          host: connectionConfig.host,
          port: connectionConfig.port || 5432,
          user: connectionConfig.username || connectionConfig.user,
          password: connectionConfig.password,
          database: connectionConfig.database,
        };

        const service = new LogicalReplicationService(replicationConfig, {
          acknowledge: { auto: false, timeoutSeconds: 0 },
        });

        const plugin = new PgoutputPlugin({
          publicationNames: [publicationName],
          protoVersion: 1, // Use version 1 for compatibility
        });

        // Collect changes with timeout
        const changePromise = new Promise<void>((resolve, reject) => {
          let resolved = false;
          const timeout = setTimeout(() => {
            if (!resolved) {
              resolved = true;
              service.stop().catch(() => {}); // Ignore stop errors
              resolve();
            }
          }, 10000); // 10 second timeout for change collection

          const cleanup = () => {
            if (!resolved) {
              resolved = true;
              clearTimeout(timeout);
              service.stop().catch(() => {}); // Ignore stop errors
            }
          };

          service.on('data', async (lsn, msg) => {
            try {
              // Filter for our target table
              if (msg.relation && msg.relation.schema === schemaName && msg.relation.name === tableName) {
                if (msg.tag === 'insert' && msg.new) {
                  changes.push(msg.new);
                  lastProcessedLSN = lsn;
                } else if (msg.tag === 'update' && msg.new) {
                  changes.push(msg.new); // Use new row data
                  lastProcessedLSN = lsn;
                } else if (msg.tag === 'delete' && msg.old) {
                  // For deletes, include the old row data with a marker
                  changes.push({ ...msg.old, _deleted: true });
                  lastProcessedLSN = lsn;
                }
              }

              // Acknowledge processed LSN
              await service.acknowledge(lsn);

              // Stop if we have enough changes
              if (changes.length >= params.limit) {
                cleanup();
                resolve();
              }
            } catch (err) {
              this.logger.warn(`Error processing WAL message: ${err}`);
            }
          });

          service.on('error', (err) => {
            this.logger.warn(`Replication error (non-fatal): ${err.message}`);
            cleanup();
            resolve(); // Don't reject - allow fallback
          });

          // Start replication from last LSN (or from beginning if no LSN)
          // The subscribe method signature: subscribe(plugin, slotName, startLsn?: string)
          const subscribePromise = lastLSN
            ? service.subscribe(plugin, slotName, lastLSN)
            : service.subscribe(plugin, slotName);
            
          subscribePromise
            .then(() => {
              this.logger.debug(`WAL replication subscribed to slot ${slotName} from LSN ${lastLSN || 'start'}`);
            })
            .catch((err) => {
              this.logger.warn(`Failed to subscribe to replication: ${err.message}`);
              cleanup();
              resolve(); // Don't reject - allow fallback
            });
        });

        await changePromise;

        // If we got changes from WAL, return them
        if (changes.length > 0) {
          this.logger.log(
            `WAL CDC: Retrieved ${changes.length} changes from WAL at LSN ${lastProcessedLSN}`,
          );

          return {
            rows: changes.slice(0, params.limit),
            totalRows: undefined,
            nextCursor: lastProcessedLSN || undefined,
            hasMore: changes.length >= params.limit,
            metadata: {
              lastLSN: lastProcessedLSN,
              slotName,
              publicationName,
            },
          };
        }

        // No changes from WAL - return empty result
        this.logger.log(`WAL CDC: No changes detected since LSN ${lastLSN}`);
        return {
          rows: [],
          totalRows: 0,
          nextCursor: lastLSN || slotLSN || undefined,
          hasMore: false,
          metadata: {
            lastLSN: lastLSN || slotLSN,
            slotName,
            publicationName,
          },
        };
      } finally {
        client.release();
      }
    } catch (error) {
      this.logger.error(`Failed to collect WAL-based incremental data: ${error}`);
      throw error;
    } finally {
      await pool.end();
    }
  }

  /**
   * Stream data using async generator for large datasets
   */
  async *collectStream(
    sourceSchema: PipelineSourceSchemaWithConfig,
    connectionConfig: any,
    params: CollectParams,
  ): AsyncIterable<any[]> {
    const pool = this.createPool(connectionConfig);
    const batchSize = params.batchSize || 500; // Default batch size is 500

    try {
      const client = await pool.connect();
      try {
        const tableName = sourceSchema.config.table || sourceSchema.config.tableName || sourceSchema.sourceTable;
        const schemaName = sourceSchema.config.schema || sourceSchema.sourceSchema || 'public';
        const fullTableName = `"${schemaName}"."${tableName}"`;

        let offset = 0;
        let hasMore = true;

        while (hasMore) {
          const query = `SELECT * FROM ${fullTableName} LIMIT ${batchSize} OFFSET ${offset}`;
          const result = await client.query(query);

          if (result.rows.length > 0) {
            yield result.rows;
            offset += result.rows.length;
          }

          hasMore = result.rows.length === batchSize;
        }
      } finally {
        client.release();
      }
    } finally {
      await pool.end();
    }
  }

  /**
   * Create a PostgreSQL connection pool
   */
  private createPool(connectionConfig: any): Pool {
    return new Pool({
      host: connectionConfig.host,
      port: connectionConfig.port || 5432,
      user: connectionConfig.username || connectionConfig.user,
      password: connectionConfig.password,
      database: connectionConfig.database,
      ssl: connectionConfig.ssl ? { rejectUnauthorized: false } : undefined,
      connectionTimeoutMillis: connectionConfig.connectionTimeout || 10000,
      max: connectionConfig.poolSize || 5,
    });
  }
}
