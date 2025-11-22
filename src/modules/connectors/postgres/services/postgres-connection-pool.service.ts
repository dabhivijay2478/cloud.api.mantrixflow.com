/**
 * PostgreSQL Connection Pool Service
 * Manages connection pools with SSH tunneling and SSL support
 */

import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Pool, PoolConfig, QueryResult } from 'pg';
import { Client as SSHClient, ConnectConfig } from 'ssh2';
import { EventEmitter } from 'events';
import { promises as dns, setDefaultResultOrder } from 'dns';
import {
  PostgresConnectionConfig,
  DecryptedConnectionCredentials,
} from '../postgres.types';
import { CONNECTION_DEFAULTS } from '../constants/postgres.constants';

// Set Node.js to prefer IPv4 for DNS lookups (affects all connections)
// This helps prevent IPv6 connectivity issues for ALL PostgreSQL connections
if (typeof setDefaultResultOrder === 'function') {
  setDefaultResultOrder('ipv4first');
}

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
   * Resolve hostname to IPv4 address to force IPv4 connections
   * This prevents IPv6 connectivity issues (EHOSTUNREACH)
   * Works for ALL PostgreSQL connections, not just Supabase
   */
  private async resolveToIPv4(host: string): Promise<string> {
    // If it's already an IP address, return as-is
    if (/^\d+\.\d+\.\d+\.\d+$/.test(host)) {
      return host;
    }

    // If it's localhost, return as-is (will use IPv4 by default)
    if (host === 'localhost' || host === '127.0.0.1') {
      return host;
    }

    try {
      // First, try to resolve to IPv4 address explicitly with a timeout
      // Use all: true to get all addresses, then filter for IPv4
      const allAddresses = await Promise.race([
        dns.lookup(host, { family: 4, all: true }),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error('DNS lookup timeout')), 5000),
        ),
      ]);

      // Find the first IPv4 address
      if (Array.isArray(allAddresses) && allAddresses.length > 0) {
        const ipv4Address = allAddresses.find((addr) => addr.family === 4);
        if (ipv4Address) {
          return ipv4Address.address;
        }
      }

      // If no IPv4 found in all addresses, try single lookup
      const singleAddress = await dns.lookup(host, { family: 4, all: false });
      return singleAddress.address;
    } catch {
      // If IPv4 lookup fails, try getting all addresses without family restriction
      try {
        const allAddresses = await Promise.race([
          dns.lookup(host, { all: true }),
          new Promise<never>((_, reject) =>
            setTimeout(() => reject(new Error('DNS lookup timeout')), 5000),
          ),
        ]);

        if (Array.isArray(allAddresses) && allAddresses.length > 0) {
          // Find the first IPv4 address
          const ipv4Address = allAddresses.find((addr) => addr.family === 4);
          if (ipv4Address) {
            return ipv4Address.address;
          }
        }
      } catch (fallbackError) {
        // If all lookups fail, we'll still try with the hostname
        // but log a warning that IPv6 issues might occur
        console.warn(
          `[PostgreSQL Connection] Failed to resolve ${host} to IPv4 address. ` +
          `Connection will attempt with hostname (may try IPv6). Error: ${fallbackError instanceof Error ? fallbackError.message : String(fallbackError)}`,
        );
      }
      // Return original hostname as last resort
      // Note: This might still try IPv6, but it's better than failing completely
      return host;
    }
  }



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
    const actualUsername = credentials.username;

    if (credentials.sshTunnelEnabled) {
      sshTunnel = await this.createSSHTunnel(credentials);
      // SSH tunnel forwards to localhost
      actualHost = 'localhost';

      actualPort = (sshTunnel as { localPort?: number }).localPort || 5432;
    }

    // Resolve hostname to IPv4 address to force IPv4 connections
    // This prevents IPv6 connectivity issues (EHOSTUNREACH)
    const resolvedHost = await this.resolveToIPv4(actualHost);

    // SSL is enabled if explicitly configured
    const shouldUseSSL = credentials.sslEnabled === true;

    // Build pool configuration
    const poolConfig: PoolConfig = {
      host: resolvedHost,
      port: actualPort,
      database: credentials.database,
      user: actualUsername, // Use corrected username for Supabase
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

      const actualUsername = config.username;

      // Resolve hostname to IPv4 address to force IPv4 connections
      // This prevents IPv6 connectivity issues (EHOSTUNREACH)
      // For Supabase direct connections, they're IPv6-only, so resolution will return hostname
      // Resolve hostname to IPv4 address to force IPv4 connections
      // This prevents IPv6 connectivity issues (EHOSTUNREACH)
      // For Supabase direct connections, they're IPv6-only, so resolution will return hostname
      let resolvedHost: string;
      const shouldUseSSL = config.ssl?.enabled === true;

      try {
        resolvedHost = await this.resolveToIPv4(actualHost);
        // If resolution returned the hostname (not an IP), it means it's IPv6-only or DNS failed
        // We'll try to connect anyway and handle IPv6 errors in the catch block
      } catch {
        // DNS resolution completely failed, use hostname and let connection attempt fail with clearer error
        resolvedHost = actualHost;
      }

      // Use configured timeout or default
      const connectionTimeout =
        config.connectionTimeout || CONNECTION_DEFAULTS.CONNECTION_TIMEOUT_MS;

      // Try connection
      try {
        // Create temporary pool for testing
        pool = new Pool({
          host: resolvedHost,
          port: actualPort,
          database: config.database,
          user: actualUsername, // Use corrected username for Supabase
          password: config.password,
          max: 1,
          connectionTimeoutMillis: connectionTimeout,
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
        throw error;
      }
    } catch (error) {
      // Properly extract and return error message
      let errorMessage = 'Connection failed';
      let errorCode = '';

      // Handle AggregateError (used by pg library for connection errors)
      if (error && typeof error === 'object' && 'errors' in error) {
        const aggregateError = error as {
          errors?: Array<{ message?: string; code?: string }>;
          code?: string;
        };

        // Try to get the first error's message
        if (
          aggregateError.errors &&
          Array.isArray(aggregateError.errors) &&
          aggregateError.errors.length > 0
        ) {
          const firstError = aggregateError.errors[0];
          errorMessage =
            firstError.message || firstError.code || 'Connection failed';
          errorCode = firstError.code || aggregateError.code || '';
        } else if (aggregateError.code) {
          errorCode = aggregateError.code;
          errorMessage = aggregateError.code;
        }
      } else if (error instanceof Error) {
        errorMessage = error.message || 'Connection failed';
        errorCode = (error as { code?: string }).code || '';
      } else if (typeof error === 'string') {
        errorMessage = error;
      } else if (error && typeof error === 'object' && 'message' in error) {
        errorMessage =
          String((error as { message: unknown }).message) ||
          'Connection failed';
        errorCode = (error as { code?: unknown }).code
          ? String((error as { code: unknown }).code)
          : '';
      }

      // Use error code if message is still empty
      if (!errorMessage || errorMessage.trim() === '') {
        errorMessage = errorCode || 'Connection failed';
      }

      // Check for specific error types and provide user-friendly messages
      if (
        errorMessage.includes('timeout') ||
        errorMessage.includes('ETIMEDOUT') ||
        errorCode === 'ETIMEDOUT'
      ) {
        return {
          success: false,
          error:
            'Connection timed out. Please check your network and database settings.',
        };
      }

      if (
        errorMessage.includes('ECONNREFUSED') ||
        errorCode === 'ECONNREFUSED'
      ) {
        return {
          success: false,
          error:
            'Connection refused. Please verify the host and port are correct.',
        };
      }

      if (errorMessage.includes('ENOTFOUND') || errorCode === 'ENOTFOUND') {
        return {
          success: false,
          error: 'Host not found. Please verify the hostname or IP address.',
        };
      }

      if (
        errorMessage.includes('EHOSTUNREACH') ||
        errorCode === 'EHOSTUNREACH'
      ) {
        return {
          success: false,
          error:
            'Host unreachable. The server may not be accessible via IPv6. Please check your network configuration or use an IPv4 address.',
        };
      }

      if (errorMessage.includes('password authentication failed')) {
        return {
          success: false,
          error:
            'Authentication failed. Please verify your username and password.',
        };
      }

      if (errorMessage.includes('no pg_hba.conf entry')) {
        return {
          success: false,
          error:
            'Access denied. The database server does not allow connections from your IP address.',
        };
      }

      if (errorMessage.includes('does not exist')) {
        return {
          success: false,
          error: 'Database does not exist. Please verify the database name.',
        };
      }

      // Return the original error message if no specific match
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
