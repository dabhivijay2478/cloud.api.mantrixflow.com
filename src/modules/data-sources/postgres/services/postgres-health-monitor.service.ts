/**
 * PostgreSQL Health Monitor Service
 * Monitors connection health and performance
 */

import { Injectable, OnModuleInit } from '@nestjs/common';
import { PostgresConnectionPoolService } from './postgres-connection-pool.service';
import { PostgresConnectionRepository } from '../repositories/postgres-connection.repository';
import { ConnectionHealth } from '../postgres.types';
import { HEALTH_CHECK } from '../constants/postgres.constants';

@Injectable()
export class PostgresHealthMonitorService implements OnModuleInit {
  private healthCheckInterval?: NodeJS.Timeout;

  constructor(
    private readonly connectionPoolService: PostgresConnectionPoolService,
    private readonly connectionRepository: PostgresConnectionRepository,
  ) {}

  /**
   * Start health monitoring
   */
  onModuleInit() {
    // Start periodic health checks
    this.healthCheckInterval = setInterval(() => {
      this.performHealthChecks().catch((error) => {
        console.error('Health check error:', error);
      });
    }, HEALTH_CHECK.INTERVAL_MS);
  }

  /**
   * Perform health check for connection
   */
  async checkHealth(connectionId: string): Promise<ConnectionHealth> {
    const startTime = Date.now();
    const pool = this.connectionPoolService.getPool(connectionId);

    if (!pool) {
      return {
        status: 'error',
        lastChecked: new Date(),
        error: 'Connection pool not found',
      };
    }

    try {
      const client = await pool.connect();
      try {
        const result = await client.query('SELECT version(), NOW()');
        const responseTimeMs = Date.now() - startTime;

        const version =
          (result.rows[0] as { version?: string })?.version || 'Unknown';

        // Get pool stats
        const stats = this.connectionPoolService.getPoolStats(connectionId);

        return {
          status: 'healthy',
          lastChecked: new Date(),
          responseTimeMs,
          version,
          activeConnections: stats?.activeConnections || 0,
          maxConnections: pool.totalCount || 0,
        };
      } finally {
        client.release();
      }
    } catch (error) {
      return {
        status: 'unhealthy',
        lastChecked: new Date(),
        error: error instanceof Error ? error.message : 'Health check failed',
      };
    }
  }

  /**
   * Perform health checks for all active connections
   */
  private async performHealthChecks(): Promise<void> {
    const activePools = this.connectionPoolService.getActivePools();

    for (const connectionId of activePools) {
      try {
        const health = await this.checkHealth(connectionId);

        // Update connection status in database
        const connection =
          await this.connectionRepository.findById(connectionId);
        if (connection) {
          await this.connectionRepository.update(connectionId, {
            status:
              health.status === 'healthy'
                ? 'active'
                : health.status === 'unhealthy'
                  ? 'error'
                  : 'inactive',
            lastConnectedAt:
              health.status === 'healthy'
                ? new Date()
                : connection.lastConnectedAt,
            lastError: health.error || null,
          });
        }
      } catch (error) {
        console.error(
          `Health check failed for connection ${connectionId}:`,
          error,
        );
      }
    }
  }

  /**
   * Cleanup on module destroy
   */
  onModuleDestroy() {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
    }
  }
}
