/**
 * PostgreSQL Connector Service
 * Main service that orchestrates all PostgreSQL connector functionality
 */

import {
  Injectable,
  NotFoundException,
  BadRequestException,
} from '@nestjs/common';
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
    // Validate connection count
    const currentCount = await this.connectionRepository.countByOrgId(orgId);
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

    // Test connection first (non-blocking - save even if test fails)
    let connectionStatus: 'active' | 'inactive' | 'error' = 'active';
    let lastConnectedAt: Date | null = new Date();
    let lastError: string | null = null;

    try {
      const testResult = await this.testConnection(config);
      if (!testResult.success) {
        connectionStatus = 'error';
        lastError = testResult.error || 'Connection test failed';
        lastConnectedAt = null;
      }
    } catch (error) {
      // Connection test failed, but we'll still save the connection
      connectionStatus = 'error';
      lastError =
        error instanceof Error ? error.message : 'Connection test failed';
      lastConnectedAt = null;
    }

    // Detect Supabase connections and auto-enable SSL
    const isSupabase = config.host.includes('supabase.co') || config.host.includes('supabase.com');
    const sslEnabled = config.ssl?.enabled !== false && (isSupabase || config.ssl?.enabled === true);

    // Create connection record (save even if test failed)
    const connection = await this.connectionRepository.create({
      orgId,
      userId,
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
      status: connectionStatus,
      lastConnectedAt,
      lastError,
    });

    // Only create connection pool if test was successful
    if (connectionStatus === 'active') {
      try {
        const credentials =
          this.connectionRepository.decryptCredentials(connection);
        await this.connectionPoolService.createPool(connection.id, credentials);
      } catch (error) {
        // Pool creation failed, update status
        await this.connectionRepository.update(connection.id, {
          status: 'error',
          lastError:
            error instanceof Error ? error.message : 'Pool creation failed',
        });
      }
    }

    return connection;
  }

  /**
   * Get connection by ID
   */
  async getConnection(
    connectionId: string,
    orgId?: string,
  ): Promise<PostgresConnection> {
    const connection = await this.connectionRepository.findById(
      connectionId,
      orgId,
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
    return await this.connectionRepository.findByOrgId(orgId);
  }

  /**
   * Update connection
   */
  async updateConnection(
    connectionId: string,
    orgId: string,
    updates: Partial<PostgresConnectionConfig>,
  ): Promise<PostgresConnection> {
    const connection = await this.getConnection(connectionId, orgId);

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
    await this.getConnection(connectionId, orgId); // Verify exists and belongs to org
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
    await this.getConnection(connectionId, orgId); // Verify access

    // Ensure pool exists
    const connection = await this.getConnection(connectionId, orgId);
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
    await this.getConnection(connectionId, orgId); // Verify access

    const result = await this.queryExecutorService.executeQuery(
      connectionId,
      userId,
      query,
      params,
      timeout,
    );

    // Log query
    await this.queryLogRepository.create({
      connectionId,
      userId,
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
    await this.getConnection(connectionId, orgId); // Verify access
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
    await this.getConnection(connectionId, orgId); // Verify access

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

    const connection = await this.getConnection(connectionId, orgId);
    const destinationTable = `raw_postgres_${orgId}_${tableName}`;

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
    await this.getConnection(connectionId, orgId); // Verify access
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
    await this.getConnection(connectionId, orgId); // Verify access
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
    await this.getSyncJob(connectionId, jobId, orgId); // Verify access
    await this.syncService.cancelSync(jobId);
  }

  /**
   * Get connection health
   */
  async getConnectionHealth(
    connectionId: string,
    orgId: string,
  ): Promise<ConnectionHealth> {
    await this.getConnection(connectionId, orgId); // Verify access
    return await this.healthMonitorService.checkHealth(connectionId);
  }

  /**
   * Get connection metrics
   */
  async getConnectionMetrics(
    connectionId: string,
    orgId: string,
  ): Promise<ConnectionMetrics> {
    await this.getConnection(connectionId, orgId); // Verify access

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
    await this.getConnection(connectionId, orgId); // Verify access
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
