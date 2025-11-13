/**
 * PostgreSQL Connector Service
 * Main service that orchestrates all PostgreSQL connector functionality
 */

import {
  Injectable,
  NotFoundException,
  BadRequestException,
} from '@nestjs/common';
import * as crypto from 'crypto';
import { PostgresConnectionRepository } from './repositories/postgres-connection.repository';
import { PostgresSyncJobRepository } from './repositories/postgres-sync-job.repository';
import { PostgresQueryLogRepository } from './repositories/postgres-query-log.repository';
import { PostgresConnectionPoolService } from './services/postgres-connection-pool.service';
import { PostgresSchemaDiscoveryService } from './services/postgres-schema-discovery.service';
import { PostgresQueryExecutorService } from './services/postgres-query-executor.service';
import { PostgresSyncService } from './services/postgres-sync.service';
import { PostgresHealthMonitorService } from './services/postgres-health-monitor.service';
import { PostgresValidator } from './postgres.validator';
import {
  PostgresConnectionConfig,
  SchemaDiscoveryResult,
  QueryExecutionResult,
  SyncProgress,
  ConnectionHealth,
  ConnectionMetrics,
  PostgresConnection,
  SyncMode,
} from './postgres.types';
import { PostgresErrorCode } from './constants/error-codes.constants';

@Injectable()
export class PostgresService {
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
    // Validate UUIDs - generate if invalid
    const validOrgId = this.validateOrGenerateUUID(orgId);
    const validUserId = this.validateOrGenerateUUID(userId);
    
    // Validate connection count
    const currentCount = await this.connectionRepository.countByOrgId(validOrgId);
    const countValidation =
      this.validator.validateConnectionCount(currentCount);
    if (!countValidation.isValid) {
      throw new BadRequestException(countValidation.error);
    }

    // Validate config
    const validation = this.validator.validateConnectionConfig(config);
    if (!validation.isValid) {
      throw new BadRequestException(validation.error);
    }

    // Detect Supabase connections and auto-enable SSL
    const isSupabase = config.host.includes('supabase.co') || config.host.includes('supabase.com');
    const sslEnabled = config.ssl?.enabled !== false && (isSupabase || config.ssl?.enabled === true);

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
      console.error('Failed to save connection to database:', error);
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
          lastError:
            error instanceof Error ? error.message : 'Connection test failed',
          lastConnectedAt: null,
        });
      } catch (updateError) {
        // Even the update failed - log but don't throw
        console.error(`Failed to update connection status for ${connectionId}:`, updateError);
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
          const credentials =
            this.connectionRepository.decryptCredentials(updatedConnection);
          await this.connectionPoolService.createPool(connectionId, credentials);
        }
      } catch (error) {
        // Pool creation failed, update status
        await this.connectionRepository.update(connectionId, {
          status: 'error',
          lastError:
            error instanceof Error ? error.message : 'Pool creation failed',
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
   * Validate UUID or generate a new one
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
   * Get connection by ID
   */
  async getConnection(
    connectionId: string,
    orgId?: string,
  ): Promise<PostgresConnection> {
    // Validate UUID if provided
    const validOrgId = orgId ? this.validateOrGenerateUUID(orgId) : undefined;
    
    const connection = await this.connectionRepository.findById(
      connectionId,
      validOrgId,
    );
    if (!connection) {
      throw new NotFoundException('Connection not found');
    }
    return connection;
  }

  /**
   * List connections for organization
   */
  async listConnections(orgId: string): Promise<PostgresConnection[]> {
    // Validate UUID - generate if invalid
    const validOrgId = this.validateOrGenerateUUID(orgId);
    return await this.connectionRepository.findByOrgId(validOrgId);
  }

  /**
   * Update connection
   */
  async updateConnection(
    connectionId: string,
    orgId: string,
    updates: Partial<PostgresConnectionConfig>,
  ): Promise<PostgresConnection> {
    // Validate UUID
    const validOrgId = this.validateOrGenerateUUID(orgId);
    const connection = await this.getConnection(connectionId, validOrgId);

    // If credentials changed, test connection
    if (
      updates.host ||
      updates.port ||
      updates.database ||
      updates.username ||
      updates.password
    ) {
      const testConfig: PostgresConnectionConfig = {
        host: updates.host || connection.host,
        port: updates.port || connection.port,
        database: updates.database || connection.database,
        username: updates.username || connection.username,
        password: updates.password || connection.password,
        ssl: updates.ssl,
        sshTunnel: updates.sshTunnel,
      };

      // Decrypt existing values if needed
      const credentials =
        this.connectionRepository.decryptCredentials(connection);
      if (!testConfig.host) testConfig.host = credentials.host;
      if (!testConfig.database) testConfig.database = credentials.database;
      if (!testConfig.username) testConfig.username = credentials.username;
      if (!testConfig.password) testConfig.password = credentials.password;

      const testResult = await this.testConnection(testConfig);
      if (!testResult.success) {
        throw new BadRequestException(
          testResult.error || 'Connection test failed',
        );
      }
    }

    // Update connection
    const updated = await this.connectionRepository.update(connectionId, {
      ...updates,
      lastConnectedAt: new Date(),
    });

    // Recreate pool if credentials changed
    if (
      updates.host ||
      updates.port ||
      updates.database ||
      updates.username ||
      updates.password
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
    // Validate UUID
    const validOrgId = this.validateOrGenerateUUID(orgId);
    await this.getConnection(connectionId, validOrgId); // Verify exists and belongs to org
    await this.connectionPoolService.closePool(connectionId);
    await this.connectionRepository.delete(connectionId);
  }

  /**
   * Discover schema
   */
  async discoverSchema(
    connectionId: string,
    orgId: string,
    forceRefresh: boolean = false,
  ): Promise<SchemaDiscoveryResult> {
    // Validate UUID
    const validOrgId = this.validateOrGenerateUUID(orgId);
    const connection = await this.getConnection(connectionId, validOrgId); // Verify access

    // Ensure pool exists
    const pool = this.connectionPoolService.getPool(connectionId);
    if (!pool) {
      const credentials =
        this.connectionRepository.decryptCredentials(connection);
      await this.connectionPoolService.createPool(connectionId, credentials);
    }

    return await this.schemaDiscoveryService.discoverSchema(
      connectionId,
      forceRefresh,
    );
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
    // Validate UUIDs
    const validOrgId = this.validateOrGenerateUUID(orgId);
    const validUserId = this.validateOrGenerateUUID(userId);
    await this.getConnection(connectionId, validOrgId); // Verify access

    const result = await this.queryExecutorService.executeQuery(
      connectionId,
      validUserId,
      query,
      params,
      timeout,
    );

    // Log query
    await this.queryLogRepository.create({
      connectionId,
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
    // Validate UUID
    const validOrgId = this.validateOrGenerateUUID(orgId);
    await this.getConnection(connectionId, validOrgId); // Verify access
    return await this.queryExecutorService.explainQuery(
      connectionId,
      query,
      params,
    );
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
    // Validate UUID
    const validOrgId = this.validateOrGenerateUUID(orgId);
    const connection = await this.getConnection(connectionId, validOrgId); // Verify access

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
      connectionId,
      tableName,
      syncMode,
      incrementalColumn,
      destinationTable,
      status: 'pending',
      syncFrequency,
      customWhereClause,
      nextSyncAt:
        syncFrequency !== 'manual'
          ? this.calculateNextSyncTime(syncFrequency)
          : undefined,
    });

    // Start sync if manual
    if (syncFrequency === 'manual') {
      return await this.syncService.startSync(
        connectionId,
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
    // Validate UUID
    const validOrgId = this.validateOrGenerateUUID(orgId);
    await this.getConnection(connectionId, validOrgId); // Verify access
    return await this.syncJobRepository.findByConnectionId(connectionId);
  }

  /**
   * Get sync job by ID
   */
  async getSyncJob(
    connectionId: string,
    jobId: string,
    orgId: string,
  ): Promise<any> {
    // Validate UUID
    const validOrgId = this.validateOrGenerateUUID(orgId);
    await this.getConnection(connectionId, validOrgId); // Verify access
    const job = await this.syncJobRepository.findById(jobId, connectionId);
    if (!job) {
      throw new NotFoundException('Sync job not found');
    }
    return job;
  }

  /**
   * Cancel sync job
   */
  async cancelSyncJob(
    connectionId: string,
    jobId: string,
    orgId: string,
  ): Promise<void> {
    // Validate UUID
    const validOrgId = this.validateOrGenerateUUID(orgId);
    await this.getSyncJob(connectionId, jobId, validOrgId); // Verify access
    await this.syncService.cancelSync(jobId);
  }

  /**
   * Get connection health
   */
  async getConnectionHealth(
    connectionId: string,
    orgId: string,
  ): Promise<ConnectionHealth> {
    // Validate UUID
    const validOrgId = this.validateOrGenerateUUID(orgId);
    await this.getConnection(connectionId, validOrgId); // Verify access
    return await this.healthMonitorService.checkHealth(connectionId);
  }

  /**
   * Get connection metrics
   */
  async getConnectionMetrics(
    connectionId: string,
    orgId: string,
  ): Promise<ConnectionMetrics> {
    // Validate UUID
    const validOrgId = this.validateOrGenerateUUID(orgId);
    await this.getConnection(connectionId, validOrgId); // Verify access

    const stats = await this.queryLogRepository.getStatistics(connectionId);
    const poolStats = this.connectionPoolService.getPoolStats(connectionId);

    return {
      connectionId,
      ...stats,
      connectionPoolUtilization: poolStats
        ? (poolStats.activeConnections / (poolStats.activeConnections + 10)) *
          100
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
    // Validate UUID
    const validOrgId = this.validateOrGenerateUUID(orgId);
    await this.getConnection(connectionId, validOrgId); // Verify access
    return await this.queryLogRepository.findByConnectionId(
      connectionId,
      limit,
      offset,
    );
  }

  /**
   * Calculate next sync time based on frequency
   */
  private calculateNextSyncTime(
    frequency: '15min' | '1hour' | '24hours',
  ): Date {
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
