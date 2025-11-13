/**
 * PostgreSQL Connection Pool Service
 * Manages connection pools with SSH tunneling and SSL support
 */

import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Pool, PoolConfig, QueryResult } from 'pg';
import { Client as SSHClient, ConnectConfig } from 'ssh2';
import { EventEmitter } from 'events';
import {
  PostgresConnectionConfig,
  DecryptedConnectionCredentials,
} from '../postgres.types';
import { CONNECTION_DEFAULTS } from '../constants/postgres.constants';

/**
 * Connection pool with metadata
 */
interface ConnectionPoolMetadata {
  pool: Pool;
  connectionId: string;
  createdAt: Date;
  lastUsedAt: Date;
  activeConnections: number;
  totalQueries: number;
  sshTunnel?: SSHClient;
}

@Injectable()
export class PostgresConnectionPoolService implements OnModuleDestroy {
  private pools = new Map<string, ConnectionPoolMetadata>();
  private readonly events = new EventEmitter();

  /**
   * Create or get connection pool
   */
  async createPool(
    connectionId: string,
    credentials: DecryptedConnectionCredentials,
  ): Promise<Pool> {
    // Check if pool already exists
    const existing = this.pools.get(connectionId);
    if (existing) {
      existing.lastUsedAt = new Date();
      return existing.pool;
    }

    // Create SSH tunnel if enabled
    let sshTunnel: SSHClient | undefined;
    let actualHost = credentials.host;
    let actualPort = credentials.port;

    if (credentials.sshTunnelEnabled) {
      sshTunnel = await this.createSSHTunnel(credentials);
      // SSH tunnel forwards to localhost
      actualHost = 'localhost';

      actualPort = (sshTunnel as { localPort?: number }).localPort || 5432;
    }

    // Detect Supabase connections (supabase.co or supabase.com domains)
    const isSupabase =
      actualHost.includes('supabase.co') || actualHost.includes('supabase.com');

    // Supabase requires SSL, so enable it if not explicitly disabled
    const shouldUseSSL = credentials.sslEnabled || isSupabase;

    // Build pool configuration
    const poolConfig: PoolConfig = {
      host: actualHost,
      port: actualPort,
      database: credentials.database,
      user: credentials.username,
      password: credentials.password,
      max: credentials.connectionPoolSize ?? CONNECTION_DEFAULTS.MAX_POOL_SIZE,
      min: 1,
      idleTimeoutMillis: CONNECTION_DEFAULTS.IDLE_TIMEOUT_MS,
      connectionTimeoutMillis: CONNECTION_DEFAULTS.CONNECTION_TIMEOUT_MS,
      ssl: shouldUseSSL
        ? {
            rejectUnauthorized: credentials.sslCaCert ? true : false, // Only reject if CA cert is provided
            ca: credentials.sslCaCert
              ? Buffer.from(credentials.sslCaCert)
              : undefined,
          }
        : false,
    };

    // Create pool
    const pool = new Pool(poolConfig);

    // Set up error handling
    pool.on('error', (err) => {
      this.events.emit('pool-error', { connectionId, error: err });
    });

    pool.on('connect', () => {
      this.events.emit('pool-connect', { connectionId });
    });

    // Test connection
    try {
      const client = await pool.connect();
      await client.query('SELECT NOW()');
      client.release();
    } catch (error) {
      // Cleanup on failure
      await pool.end();
      if (sshTunnel) {
        sshTunnel.end();
      }
      throw error;
    }

    // Store pool metadata
    const metadata: ConnectionPoolMetadata = {
      pool,
      connectionId,
      createdAt: new Date(),
      lastUsedAt: new Date(),
      activeConnections: 0,
      totalQueries: 0,
      sshTunnel,
    };

    this.pools.set(connectionId, metadata);
    this.events.emit('pool-created', { connectionId });

    return pool;
  }

  /**
   * Get existing pool
   */
  getPool(connectionId: string): Pool | undefined {
    const metadata = this.pools.get(connectionId);
    if (metadata) {
      metadata.lastUsedAt = new Date();
      return metadata.pool;
    }
    return undefined;
  }

  /**
   * Execute query using pool
   */
  async executeQuery<T extends Record<string, any> = any>(
    connectionId: string,
    query: string,
    params?: any[],
    timeout?: number,
  ): Promise<QueryResult<T>> {
    const pool = this.getPool(connectionId);
    if (!pool) {
      throw new Error(`Pool not found for connection ${connectionId}`);
    }

    const metadata = this.pools.get(connectionId)!;
    metadata.totalQueries++;
    metadata.activeConnections++;

    try {
      const client = await pool.connect();
      try {
        // Set query timeout if provided
        if (timeout) {
          await client.query(`SET statement_timeout = ${timeout * 1000}`); // Convert to milliseconds
        }

        const result = await client.query<T>(query, params);

        // Reset timeout
        if (timeout) {
          await client.query('RESET statement_timeout');
        }

        return result;
      } finally {
        client.release();
        metadata.activeConnections--;
      }
    } catch (error) {
      metadata.activeConnections--;
      throw error;
    }
  }

  /**
   * Test connection
   */
  async testConnection(config: PostgresConnectionConfig): Promise<{
    success: boolean;
    error?: string;
    version?: string;
    responseTimeMs?: number;
  }> {
    const startTime = Date.now();
    let pool: Pool | undefined;
    let sshTunnel: SSHClient | undefined;

    try {
      // Create SSH tunnel if needed
      let actualHost = config.host;
      let actualPort = config.port || CONNECTION_DEFAULTS.PORT;

      if (config.sshTunnel?.enabled) {
        sshTunnel = await this.createSSHTunnel({
          host: config.host,
          port: config.port || CONNECTION_DEFAULTS.PORT,
          database: config.database,
          username: config.username,
          password: config.password,
          sslEnabled: config.ssl?.enabled || false,
          sshTunnelEnabled: true,
          sshHost: config.sshTunnel.host,
          sshPort: config.sshTunnel.port,
          sshUsername: config.sshTunnel.username,
          sshPrivateKey: config.sshTunnel.privateKey,
        });
        actualHost = 'localhost';

        actualPort = (sshTunnel as { localPort?: number }).localPort || 5432;
      }

      // Detect Supabase connections (supabase.co or supabase.com domains)
      const isSupabase =
        actualHost.includes('supabase.co') ||
        actualHost.includes('supabase.com');

      // Supabase requires SSL, so enable it if not explicitly disabled
      const shouldUseSSL =
        config.ssl?.enabled !== false &&
        (isSupabase || config.ssl?.enabled === true);

      // Increase timeout for Supabase connections (they may need more time)
      const connectionTimeout = isSupabase
        ? config.connectionTimeout ||
          CONNECTION_DEFAULTS.CONNECTION_TIMEOUT_MS * 2 // Double timeout for Supabase
        : config.connectionTimeout || CONNECTION_DEFAULTS.CONNECTION_TIMEOUT_MS;

      // Create temporary pool for testing
      pool = new Pool({
        host: actualHost,
        port: actualPort,
        database: config.database,
        user: config.username,
        password: config.password,
        max: 1,
        connectionTimeoutMillis: connectionTimeout,
        // For Supabase, use SSL but don't reject unauthorized if no CA cert provided
        ssl: shouldUseSSL
          ? {
              rejectUnauthorized: config.ssl?.caCert
                ? config.ssl.rejectUnauthorized !== false
                : false,
              ca: config.ssl?.caCert
                ? Buffer.from(config.ssl.caCert)
                : undefined,
            }
          : false,
      });

      // Test connection
      const client = await pool.connect();
      try {
        const result = await client.query('SELECT version()');

        const version =
          (result.rows[0] as { version?: string })?.version || 'Unknown';
        const responseTimeMs = Date.now() - startTime;

        return {
          success: true,
          version,
          responseTimeMs,
        };
      } finally {
        client.release();
      }
    } catch (error) {
      // Map error to user-friendly message without throwing
      const errorMessage =
        error instanceof Error ? error.message : 'Connection failed';

      // Check if it's a timeout error
      if (
        errorMessage.includes('timeout') ||
        errorMessage.includes('ETIMEDOUT')
      ) {
        return {
          success: false,
          error:
            'Connection timed out. Please check your network and database settings.',
        };
      }

      return {
        success: false,
        error: errorMessage,
      };
    } finally {
      // Cleanup
      if (pool) {
        await pool.end();
      }
      if (sshTunnel) {
        sshTunnel.end();
      }
    }
  }

  /**
   * Create SSH tunnel
   */
  private async createSSHTunnel(
    credentials: DecryptedConnectionCredentials,
  ): Promise<SSHClient> {
    return new Promise((resolve, reject) => {
      const ssh = new SSHClient();
      let localPort: number;

      const sshConfig: ConnectConfig = {
        host: credentials.sshHost!,
        port: credentials.sshPort || 22,
        username: credentials.sshUsername!,
        privateKey: Buffer.from(credentials.sshPrivateKey!),
        readyTimeout: CONNECTION_DEFAULTS.CONNECTION_TIMEOUT_MS,
      };

      ssh.on('ready', () => {
        // Forward local port to remote PostgreSQL
        ssh.forwardOut(
          '127.0.0.1',
          0, // Let SSH choose local port
          credentials.host,
          credentials.port,
          (err, stream) => {
            if (err) {
              reject(err);
              return;
            }

            // Create a local server that forwards to the stream
            // eslint-disable-next-line @typescript-eslint/no-require-imports, @typescript-eslint/no-unsafe-assignment
            const net = require('net');
            // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
            const server = net.createServer(
              (localStream: NodeJS.ReadWriteStream) => {
                localStream.pipe(stream).pipe(localStream);
              },
            );

            // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
            server.listen(0, () => {
              // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
              const address = server.address();
              if (address && typeof address === 'object' && 'port' in address) {
                // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
                localPort = address.port as number;

                (
                  ssh as unknown as {
                    config: { localPort?: number; server?: unknown };
                  }
                ).config.localPort = localPort;

                (
                  ssh as unknown as {
                    config: { localPort?: number; server?: unknown };
                  }
                ).config.server = server;
                resolve(ssh);
              } else {
                reject(new Error('Failed to get local port'));
              }
            });
          },
        );
      });

      ssh.on('error', (err) => {
        reject(err);
      });

      ssh.connect(sshConfig);
    });
  }

  /**
   * Close pool and cleanup
   */
  async closePool(connectionId: string): Promise<void> {
    const metadata = this.pools.get(connectionId);
    if (!metadata) {
      return;
    }

    try {
      await metadata.pool.end();
      if (metadata.sshTunnel) {
        const sshConfig = (
          metadata.sshTunnel as { config?: { server?: { close: () => void } } }
        ).config;
        if (sshConfig?.server) {
          sshConfig.server.close();
        }
        metadata.sshTunnel.end();
      }
      this.pools.delete(connectionId);
      this.events.emit('pool-closed', { connectionId });
    } catch (error) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      this.events.emit('pool-close-error', { connectionId, error });
    }
  }

  /**
   * Get pool statistics
   */
  getPoolStats(connectionId: string): {
    activeConnections: number;
    totalQueries: number;
    createdAt: Date;
    lastUsedAt: Date;
  } | null {
    const metadata = this.pools.get(connectionId);
    if (!metadata) {
      return null;
    }

    return {
      activeConnections: metadata.activeConnections,
      totalQueries: metadata.totalQueries,
      createdAt: metadata.createdAt,
      lastUsedAt: metadata.lastUsedAt,
    };
  }

  /**
   * Get all active pools
   */
  getActivePools(): string[] {
    return Array.from(this.pools.keys());
  }

  /**
   * Cleanup on module destroy
   */
  async onModuleDestroy() {
    const closePromises = Array.from(this.pools.keys()).map((id) =>
      this.closePool(id),
    );
    await Promise.all(closePromises);
  }

  /**
   * Get event emitter for monitoring
   */
  getEvents(): EventEmitter {
    return this.events;
  }
}
