/**
 * PostgreSQL Connector Service
 * Main service that orchestrates all PostgreSQL connector functionality
 */

import * as crypto from 'node:crypto';
import { BadRequestException, Injectable, Logger, NotFoundException } from '@nestjs/common';
import type { NewPostgresConnection } from '../../../database/schemas/data-sources/connections/postgres-connections.schema';
import type {
  ConnectionHealth,
  ConnectionMetrics,
  PostgresConnection,
  PostgresConnectionConfig,
  QueryExecutionResult,
  SchemaDiscoveryResult,
  SchemaInfo,
  SyncMode,
  TableInfo,
} from './postgres.types';
import { PostgresValidator } from './postgres.validator';
import { PostgresConnectionRepository } from './repositories/postgres-connection.repository';
import { PostgresQueryLogRepository } from './repositories/postgres-query-log.repository';
import { PostgresSyncJobRepository } from './repositories/postgres-sync-job.repository';
import { PostgresConnectionPoolService } from './services/postgres-connection-pool.service';
import { PostgresHealthMonitorService } from './services/postgres-health-monitor.service';
import { PostgresQueryExecutorService } from './services/postgres-query-executor.service';
import { PostgresSchemaDiscoveryService } from './services/postgres-schema-discovery.service';
import { PostgresSyncService } from './services/postgres-sync.service';

@Injectable()
export class PostgresDataSourceService {
  private readonly logger = new Logger(PostgresDataSourceService.name);

  constructor(
    private readonly connectionRepository: PostgresConnectionRepository,
    private readonly syncJobRepository: PostgresSyncJobRepository,
    private readonly queryLogRepository: PostgresQueryLogRepository,
    private readonly connectionPoolService: PostgresConnectionPoolService,
    private readonly schemaDiscoveryService: PostgresSchemaDiscoveryService,
    private readonly queryExecutorService: PostgresQueryExecutorService,
    private readonly syncService: PostgresSyncService,
    private readonly healthMonitorService: PostgresHealthMonitorService,
    private readonly validator: PostgresValidator,
  ) {}

  /**
   * Test connection without saving
   */
  async testConnection(config: PostgresConnectionConfig): Promise<{
    success: boolean;
    error?: string;
    version?: string;
    responseTimeMs?: number;
  }> {
    const validation = this.validator.validateTestConnection(config);
    if (!validation.isValid) {
      throw new BadRequestException(validation.error);
    }

    return await this.connectionPoolService.testConnection(config);
  }

  /**
   * Create connection
   */
  async createConnection(
    orgId: string,
    userId: string,
    name: string,
    config: PostgresConnectionConfig,
  ): Promise<PostgresConnection> {
    // Validate UUIDs - throw error if invalid (don't generate new ones)
    const validOrgId = this.validateUUID(orgId, 'Organization ID');
    const validUserId = this.validateUUID(userId, 'User ID');

    this.logger.log(`Creating connection for organization ${validOrgId} by user ${validUserId}`);

    // Validate connection count
    const currentCount = await this.connectionRepository.countByOrgId(validOrgId);
    const countValidation = this.validator.validateConnectionCount(currentCount);
    if (!countValidation.isValid) {
      throw new BadRequestException(countValidation.error);
    }

    // Validate config
    const validation = this.validator.validateConnectionConfig(config);
    if (!validation.isValid) {
      throw new BadRequestException(validation.error);
    }

    // Detect Neon and Supabase connections and auto-enable SSL
    const isNeon = config.host.includes('.neon.tech');
    const isSupabase = config.host.includes('supabase.co') || config.host.includes('supabase.com');

    // Don't auto-enable SSL for localhost/127.0.0.1 (development)
    const isLocalhost =
      config.host === 'localhost' || config.host === '127.0.0.1' || config.host.startsWith('127.');

    const sslEnabled =
      !isLocalhost &&
      config.ssl?.enabled !== false &&
      (isNeon || isSupabase || config.ssl?.enabled === true);

    // For Neon databases, extract endpoint ID and add to options
    if (isNeon && !config.options) {
      const endpointId = config.host.split('.')[0];
      config.options = `endpoint%3D${encodeURIComponent(endpointId)}`;
    }

    // For localhost connections, ensure rejectUnauthorized is false if SSL is enabled
    if (isLocalhost && config.ssl?.enabled && config.ssl.rejectUnauthorized !== false) {
      config.ssl.rejectUnauthorized = false;
    }

    // Save connection FIRST - this is the critical operation
    // Do NOT test connection before saving - save immediately
    let connection: PostgresConnection;
    try {
      connection = await this.connectionRepository.create({
        orgId: validOrgId,
        userId: validUserId,
        name,
        host: config.host,
        port: config.port || 5432,
        database: config.database,
        username: config.username,
        password: config.password,
        sslEnabled,
        sslCaCert: config.ssl?.caCert,
        sshTunnelEnabled: config.sshTunnel?.enabled || false,
        sshHost: config.sshTunnel?.host,
        sshPort: config.sshTunnel?.port,
        sshUsername: config.sshTunnel?.username,
        sshPrivateKey: config.sshTunnel?.privateKey,
        connectionPoolSize: config.poolSize || 5,
        queryTimeoutSeconds: config.queryTimeout || 60,
        status: 'inactive', // Start as inactive, will be updated after test
        lastConnectedAt: null,
        lastError: null,
      });
    } catch (error) {
      // If database save fails, throw immediately - this is a real error
      this.logger.error(
        'Failed to save connection to database',
        error instanceof Error ? error.stack : String(error),
      );
      throw new BadRequestException(
        `Failed to save connection: ${error instanceof Error ? error.message : 'Database error'}`,
      );
    }

    // Test connection in background (completely async, fire and forget)
    // Use setImmediate to ensure it runs after the response is sent
    // Errors are completely isolated - they never affect the API response
    setImmediate(() => {
      this.testConnectionInBackground(connection.id, config).catch(() => {
        // Silently ignore - errors are already handled in testConnectionInBackground
        // This catch is just to prevent unhandled promise rejections
      });
    });

    return connection;
  }

  /**
   * Test connection in background and update status
   * This method is completely isolated - errors never bubble up
   */
  private async testConnectionInBackground(
    connectionId: string,
    config: PostgresConnectionConfig,
  ): Promise<void> {
    // Wrap everything in try-catch to ensure no errors escape
    try {
      // Set a timeout for the entire test to prevent hanging
      const testPromise = this.performConnectionTest(connectionId, config);
      const timeoutPromise = new Promise<void>((_, reject) => {
        setTimeout(() => reject(new Error('Connection test timeout')), 60000); // 60 second max
      });

      await Promise.race([testPromise, timeoutPromise]);
    } catch (error) {
      // Silently handle all errors - just log and update status
      // This should NEVER throw or affect the API response
      try {
        await this.connectionRepository.update(connectionId, {
          status: 'error',
          lastError: error instanceof Error ? error.message : 'Connection test failed',
          lastConnectedAt: null,
        });
      } catch (updateError) {
        // Even the update failed - log but don't throw
        this.logger.error(
          `Failed to update connection status for ${connectionId}`,
          updateError instanceof Error ? updateError.stack : String(updateError),
        );
      }
    }
  }

  /**
   * Perform the actual connection test
   */
  private async performConnectionTest(
    connectionId: string,
    config: PostgresConnectionConfig,
  ): Promise<void> {
    const testResult = await this.testConnection(config);
    const connection = await this.connectionRepository.findById(connectionId);
    if (!connection) return;

    if (testResult.success) {
      // Update connection status to active
      await this.connectionRepository.update(connectionId, {
        status: 'active',
        lastConnectedAt: new Date(),
        lastError: null,
      });

      // Create connection pool
      try {
        const updatedConnection = await this.connectionRepository.findById(connectionId);
        if (updatedConnection) {
          const credentials = this.connectionRepository.decryptCredentials(updatedConnection);
          await this.connectionPoolService.createPool(connectionId, credentials);
        }
      } catch (error) {
        // Pool creation failed, update status
        await this.connectionRepository.update(connectionId, {
          status: 'error',
          lastError: error instanceof Error ? error.message : 'Pool creation failed',
        });
      }
    } else {
      // Update connection status to error
      await this.connectionRepository.update(connectionId, {
        status: 'error',
        lastError: testResult.error || 'Connection test failed',
        lastConnectedAt: null,
      });
    }
  }

  /**
   * Validate UUID or generate a new one (only for creation)
   */
  private validateOrGenerateUUID(value: string): string {
    // UUID v4 regex pattern
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

    if (uuidRegex.test(value)) {
      return value;
    }

    // Generate a new UUID v4
    return crypto.randomUUID();
  }

  /**
   * Validate UUID (throws error if invalid - for queries)
   */
  private validateUUID(value: string, fieldName: string = 'ID'): string {
    // UUID v4 regex pattern
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

    if (uuidRegex.test(value)) {
      return value;
    }

    throw new BadRequestException(`Invalid ${fieldName} format. Must be a valid UUID v4.`);
  }

  /**
   * Get connection by ID
   */
  async getConnection(connectionId: string, orgId?: string): Promise<PostgresConnection> {
    // Validate UUIDs - throw error if invalid (for queries)
    const validConnectionId = this.validateUUID(connectionId, 'Connection ID');
    const validOrgId = orgId ? this.validateUUID(orgId, 'Organization ID') : undefined;

    const connection = await this.connectionRepository.findById(validConnectionId, validOrgId);
    if (!connection) {
      throw new NotFoundException('Connection not found');
    }
    return connection;
  }

  /**
   * List connections for organization
   */
  async listConnections(orgId: string): Promise<PostgresConnection[]> {
    // Validate UUID - throw error if invalid (for queries)
    const validOrgId = this.validateUUID(orgId, 'Organization ID');
    this.logger.log(`Listing connections for organization ${validOrgId}`);
    const connections = await this.connectionRepository.findByOrgId(validOrgId);
    this.logger.log(`Found ${connections.length} connection(s) for organization ${validOrgId}`);
    return connections;
  }

  /**
   * Update connection
   */
  async updateConnection(
    connectionId: string,
    orgId: string,
    updates: Partial<PostgresConnectionConfig> & Record<string, unknown>, // Accept UpdateConnectionDto or PostgresConnectionConfig
  ): Promise<PostgresConnection> {
    // Normalize updates to handle DTO types (e.g., SSLConfigDto with optional enabled)
    const normalizedUpdates = { ...updates };
    if (normalizedUpdates.ssl && typeof normalizedUpdates.ssl === 'object') {
      const ssl = normalizedUpdates.ssl as {
        enabled?: boolean;
        caCert?: string;
        rejectUnauthorized?: boolean;
      };
      if (ssl.enabled === undefined) {
        // If enabled is not provided, remove ssl from updates
        delete normalizedUpdates.ssl;
      } else {
        // Ensure enabled is a boolean
        normalizedUpdates.ssl = {
          enabled: ssl.enabled,
          caCert: ssl.caCert,
          rejectUnauthorized: ssl.rejectUnauthorized,
        } as PostgresConnectionConfig['ssl'];
      }
    }
    // Validate UUIDs - throw error if invalid (for queries)
    const validConnectionId = this.validateUUID(connectionId, 'Connection ID');
    const validOrgId = this.validateUUID(orgId, 'Organization ID');
    const connection = await this.getConnection(validConnectionId, validOrgId);

    // If credentials changed, test connection
    const updatesRecord = normalizedUpdates as Record<string, unknown>;
    if (
      updatesRecord.host ||
      updatesRecord.port ||
      updatesRecord.database ||
      updatesRecord.username ||
      updatesRecord.password
    ) {
      const testConfig: PostgresConnectionConfig = {
        host: (updatesRecord.host as string | undefined) || connection.host,
        port: (updatesRecord.port as number | undefined) || connection.port,
        database: (updatesRecord.database as string | undefined) || connection.database,
        username: (updatesRecord.username as string | undefined) || connection.username,
        password: (updatesRecord.password as string | undefined) || connection.password,
        ssl: normalizedUpdates.ssl,
        sshTunnel: normalizedUpdates.sshTunnel,
      };

      // Decrypt existing values if needed
      const credentials = this.connectionRepository.decryptCredentials(connection);
      if (!testConfig.host) testConfig.host = credentials.host;
      if (!testConfig.database) testConfig.database = credentials.database;
      if (!testConfig.username) testConfig.username = credentials.username;
      if (!testConfig.password) testConfig.password = credentials.password;

      const testResult = await this.testConnection(testConfig);
      if (!testResult.success) {
        throw new BadRequestException(testResult.error || 'Connection test failed');
      }
    }

    // Prepare update data - convert PostgresConnectionConfig to database format
    const updateData: Partial<NewPostgresConnection> = {
      lastConnectedAt: new Date(),
    };

    // Map plain text credentials from PostgresConnectionConfig to encrypted format
    if (updatesRecord.host !== undefined) {
      updateData.host = updatesRecord.host as string; // Will be encrypted in repository
    }
    if (updatesRecord.port !== undefined) {
      updateData.port = updatesRecord.port as number;
    }
    if (updatesRecord.database !== undefined) {
      updateData.database = updatesRecord.database as string; // Will be encrypted in repository
    }
    if (updatesRecord.username !== undefined) {
      updateData.username = updatesRecord.username as string; // Will be encrypted in repository
    }
    if (updatesRecord.password !== undefined) {
      updateData.password = updatesRecord.password as string; // Will be encrypted in repository
    }
    const ssl = updatesRecord.ssl as { enabled?: boolean; caCert?: string } | undefined;
    if (ssl?.enabled !== undefined) {
      updateData.sslEnabled = ssl.enabled;
    }
    if (ssl?.caCert !== undefined) {
      updateData.sslCaCert = ssl.caCert; // Will be encrypted in repository
    }
    const sshTunnel = updatesRecord.sshTunnel as
      | {
          enabled?: boolean;
          host?: string;
          port?: number;
          username?: string;
          privateKey?: string;
        }
      | undefined;
    if (sshTunnel?.enabled !== undefined) {
      updateData.sshTunnelEnabled = sshTunnel.enabled;
    }
    if (sshTunnel?.host !== undefined) {
      updateData.sshHost = sshTunnel.host; // Will be encrypted in repository
    }
    if (sshTunnel?.port !== undefined) {
      updateData.sshPort = sshTunnel.port;
    }
    if (sshTunnel?.username !== undefined) {
      updateData.sshUsername = sshTunnel.username; // Will be encrypted in repository
    }
    if (sshTunnel?.privateKey !== undefined) {
      updateData.sshPrivateKey = sshTunnel.privateKey; // Will be encrypted in repository
    }
    if (updatesRecord.poolSize !== undefined) {
      updateData.connectionPoolSize = updatesRecord.poolSize as number;
    }
    if (updatesRecord.queryTimeout !== undefined) {
      updateData.queryTimeoutSeconds = (updatesRecord.queryTimeout as number) / 1000; // Convert ms to seconds
    }

    // Update connection
    const updated = await this.connectionRepository.update(validConnectionId, updateData);

    // Recreate pool if credentials changed
    if (
      updatesRecord.host ||
      updatesRecord.port ||
      updatesRecord.database ||
      updatesRecord.username ||
      updatesRecord.password
    ) {
      await this.connectionPoolService.closePool(connectionId);
      const credentials = this.connectionRepository.decryptCredentials(updated);
      await this.connectionPoolService.createPool(connectionId, credentials);
    }

    return updated;
  }

  /**
   * Delete connection
   */
  async deleteConnection(connectionId: string, orgId: string): Promise<void> {
    // Validate UUIDs - throw error if invalid (for queries)
    const validConnectionId = this.validateUUID(connectionId, 'Connection ID');
    const validOrgId = this.validateUUID(orgId, 'Organization ID');
    await this.getConnection(validConnectionId, validOrgId); // Verify exists and belongs to org
    await this.connectionPoolService.closePool(validConnectionId);
    await this.connectionRepository.delete(validConnectionId);
  }

  /**
   * Discover schema
   */
  async discoverSchema(
    connectionId: string,
    orgId: string,
    forceRefresh: boolean = false,
  ): Promise<SchemaDiscoveryResult> {
    // Validate UUIDs - throw error if invalid (for queries)
    const validConnectionId = this.validateUUID(connectionId, 'Connection ID');
    const validOrgId = this.validateUUID(orgId, 'Organization ID');
    const connection = await this.getConnection(validConnectionId, validOrgId); // Verify access

    // Ensure pool exists
    const pool = this.connectionPoolService.getPool(validConnectionId);
    if (!pool) {
      const credentials = this.connectionRepository.decryptCredentials(connection);
      await this.connectionPoolService.createPool(validConnectionId, credentials);
    }

    return await this.schemaDiscoveryService.discoverSchema(validConnectionId, forceRefresh);
  }

  /**
   * Discover schemas with their tables
   */
  async discoverSchemasWithTables(connectionId: string, orgId: string): Promise<SchemaInfo[]> {
    // Validate UUIDs - throw error if invalid (for queries)
    const validConnectionId = this.validateUUID(connectionId, 'Connection ID');
    const validOrgId = this.validateUUID(orgId, 'Organization ID');
    const connection = await this.getConnection(validConnectionId, validOrgId); // Verify access

    // Ensure pool exists
    const pool = this.connectionPoolService.getPool(validConnectionId);
    if (!pool) {
      const credentials = this.connectionRepository.decryptCredentials(connection);
      await this.connectionPoolService.createPool(validConnectionId, credentials);
    }

    // Get pool again after ensuring it exists
    const finalPool = this.connectionPoolService.getPool(validConnectionId);
    if (!finalPool) {
      throw new Error(`Pool not found for connection ${validConnectionId}`);
    }

    // Discover schemas and tables
    const schemas = await this.schemaDiscoveryService.discoverSchemas(finalPool);
    const allTables = await this.schemaDiscoveryService.discoverAllTables(finalPool);

    // Group tables by schema
    const tablesBySchema = new Map<string, TableInfo[]>();
    for (const table of allTables) {
      if (!tablesBySchema.has(table.schema)) {
        tablesBySchema.set(table.schema, []);
      }
      tablesBySchema.get(table.schema)?.push(table);
    }

    // Map schemas with their tables
    return schemas.map((schema) => ({
      ...schema,
      tables: tablesBySchema.get(schema.name) || [],
    }));
  }

  /**
   * Discover tables for a specific schema
   */
  async discoverTablesForSchema(
    connectionId: string,
    orgId: string,
    schema: string,
  ): Promise<TableInfo[]> {
    // Validate UUIDs - throw error if invalid (for queries)
    const validConnectionId = this.validateUUID(connectionId, 'Connection ID');
    const validOrgId = this.validateUUID(orgId, 'Organization ID');
    const connection = await this.getConnection(validConnectionId, validOrgId); // Verify access

    // Ensure pool exists
    const pool = this.connectionPoolService.getPool(validConnectionId);
    if (!pool) {
      const credentials = this.connectionRepository.decryptCredentials(connection);
      await this.connectionPoolService.createPool(validConnectionId, credentials);
    }

    // Get pool again after ensuring it exists
    const finalPool = this.connectionPoolService.getPool(validConnectionId);
    if (!finalPool) {
      throw new Error(`Pool not found for connection ${validConnectionId}`);
    }

    // Check if schema exists
    const schemaExists = await this.schemaDiscoveryService.schemaExists(finalPool, schema);
    if (!schemaExists) {
      throw new NotFoundException(`Schema "${schema}" not found`);
    }

    // Discover tables for the schema
    return await this.schemaDiscoveryService.discoverTables(finalPool, schema);
  }

  /**
   * Execute query
   */
  async executeQuery(
    connectionId: string,
    orgId: string,
    userId: string,
    query: string,
    params?: any[],
    timeout?: number,
  ): Promise<QueryExecutionResult> {
    // Validate UUIDs - throw error if invalid (for queries)
    const validConnectionId = this.validateUUID(connectionId, 'Connection ID');
    const validOrgId = this.validateUUID(orgId, 'Organization ID');
    // For userId, generate UUID if invalid (for query logging purposes)
    const validUserId = this.validateOrGenerateUUID(userId);
    const connection = await this.getConnection(validConnectionId, validOrgId); // Verify access

    // Ensure pool exists
    const pool = this.connectionPoolService.getPool(validConnectionId);
    if (!pool) {
      const credentials = this.connectionRepository.decryptCredentials(connection);
      await this.connectionPoolService.createPool(validConnectionId, credentials);
    }

    const result = await this.queryExecutorService.executeQuery(
      validConnectionId,
      validUserId,
      query,
      params,
      timeout,
    );

    // Log query
    await this.queryLogRepository.create({
      connectionId: validConnectionId,
      userId: validUserId,
      query,
      executionTimeMs: result.executionTimeMs,
      rowsReturned: result.rowCount,
      status: 'success',
    });

    return result;
  }

  /**
   * Explain query
   */
  async explainQuery(
    connectionId: string,
    orgId: string,
    query: string,
    params?: any[],
  ): Promise<any> {
    // Validate UUIDs - throw error if invalid (for queries)
    const validConnectionId = this.validateUUID(connectionId, 'Connection ID');
    const validOrgId = this.validateUUID(orgId, 'Organization ID');
    const connection = await this.getConnection(validConnectionId, validOrgId); // Verify access

    // Ensure pool exists
    const pool = this.connectionPoolService.getPool(validConnectionId);
    if (!pool) {
      const credentials = this.connectionRepository.decryptCredentials(connection);
      await this.connectionPoolService.createPool(validConnectionId, credentials);
    }

    return await this.queryExecutorService.explainQuery(validConnectionId, query, params);
  }

  /**
   * Create sync job
   */
  async createSyncJob(
    connectionId: string,
    orgId: string,
    tableName: string,
    schema: string,
    syncMode: 'full' | 'incremental',
    incrementalColumn?: string,
    customWhereClause?: string,
    syncFrequency: 'manual' | '15min' | '1hour' | '24hours' = 'manual',
  ): Promise<any> {
    // Validate UUIDs - throw error if invalid (for queries)
    const validConnectionId = this.validateUUID(connectionId, 'Connection ID');
    const validOrgId = this.validateUUID(orgId, 'Organization ID');
    await this.getConnection(validConnectionId, validOrgId); // Verify access

    const validation = this.validator.validateSyncJob({
      tableName,
      schema,
      syncMode,
      incrementalColumn,
      customWhereClause,
      syncFrequency,
    });

    if (!validation.isValid) {
      throw new BadRequestException(validation.error);
    }

    const destinationTable = `raw_postgres_${validOrgId}_${tableName}`;

    const job = await this.syncJobRepository.create({
      connectionId: validConnectionId,
      tableName,
      syncMode,
      incrementalColumn,
      destinationTable,
      status: 'pending',
      syncFrequency,
      customWhereClause,
      nextSyncAt:
        syncFrequency !== 'manual' ? this.calculateNextSyncTime(syncFrequency) : undefined,
    });

    // Start sync if manual
    if (syncFrequency === 'manual') {
      return await this.syncService.startSync(
        validConnectionId,
        job.id,
        tableName,
        schema,
        syncMode as SyncMode,
        incrementalColumn,
        customWhereClause,
      );
    }

    return job;
  }

  /**
   * Get sync jobs
   */
  async getSyncJobs(connectionId: string, orgId: string): Promise<any[]> {
    // Validate UUIDs - throw error if invalid (for queries)
    const validConnectionId = this.validateUUID(connectionId, 'Connection ID');
    const validOrgId = this.validateUUID(orgId, 'Organization ID');
    await this.getConnection(validConnectionId, validOrgId); // Verify access
    return await this.syncJobRepository.findByConnectionId(validConnectionId);
  }

  /**
   * Get sync job by ID
   */
  async getSyncJob(connectionId: string, jobId: string, orgId: string): Promise<any> {
    // Validate UUIDs - throw error if invalid (for queries)
    const validConnectionId = this.validateUUID(connectionId, 'Connection ID');
    const validJobId = this.validateUUID(jobId, 'Job ID');
    const validOrgId = this.validateUUID(orgId, 'Organization ID');
    await this.getConnection(validConnectionId, validOrgId); // Verify access
    const job = await this.syncJobRepository.findById(validJobId, validConnectionId);
    if (!job) {
      throw new NotFoundException('Sync job not found');
    }
    return job;
  }

  /**
   * Cancel sync job
   */
  async cancelSyncJob(connectionId: string, jobId: string, orgId: string): Promise<void> {
    // Validate UUIDs - throw error if invalid (for queries)
    const validConnectionId = this.validateUUID(connectionId, 'Connection ID');
    const validJobId = this.validateUUID(jobId, 'Job ID');
    const validOrgId = this.validateUUID(orgId, 'Organization ID');
    await this.getSyncJob(validConnectionId, validJobId, validOrgId); // Verify access
    await this.syncService.cancelSync(validJobId);
  }

  /**
   * Get connection health
   */
  async getConnectionHealth(connectionId: string, orgId: string): Promise<ConnectionHealth> {
    // Validate UUIDs - throw error if invalid (for queries)
    const validConnectionId = this.validateUUID(connectionId, 'Connection ID');
    const validOrgId = this.validateUUID(orgId, 'Organization ID');
    await this.getConnection(validConnectionId, validOrgId); // Verify access
    return await this.healthMonitorService.checkHealth(validConnectionId);
  }

  /**
   * Get connection metrics
   */
  async getConnectionMetrics(connectionId: string, orgId: string): Promise<ConnectionMetrics> {
    // Validate UUIDs - throw error if invalid (for queries)
    const validConnectionId = this.validateUUID(connectionId, 'Connection ID');
    const validOrgId = this.validateUUID(orgId, 'Organization ID');
    await this.getConnection(validConnectionId, validOrgId); // Verify access

    const stats = await this.queryLogRepository.getStatistics(validConnectionId);
    const poolStats = this.connectionPoolService.getPoolStats(validConnectionId);

    return {
      connectionId: validConnectionId,
      ...stats,
      connectionPoolUtilization: poolStats
        ? (poolStats.activeConnections / (poolStats.activeConnections + 10)) * 100
        : 0,
      dataVolumeTransferred: 0, // TODO: Calculate from query logs
    };
  }

  /**
   * Get query logs
   */
  async getQueryLogs(
    connectionId: string,
    orgId: string,
    limit: number = 100,
    offset: number = 0,
  ): Promise<any[]> {
    // Validate UUIDs - throw error if invalid (for queries)
    const validConnectionId = this.validateUUID(connectionId, 'Connection ID');
    const validOrgId = this.validateUUID(orgId, 'Organization ID');
    await this.getConnection(validConnectionId, validOrgId); // Verify access
    return await this.queryLogRepository.findByConnectionId(validConnectionId, limit, offset);
  }

  /**
   * Calculate next sync time based on frequency
   */
  private calculateNextSyncTime(frequency: '15min' | '1hour' | '24hours'): Date {
    const now = new Date();
    switch (frequency) {
      case '15min':
        return new Date(now.getTime() + 15 * 60 * 1000);
      case '1hour':
        return new Date(now.getTime() + 60 * 60 * 1000);
      case '24hours':
        return new Date(now.getTime() + 24 * 60 * 60 * 1000);
      default:
        return now;
    }
  }
}
