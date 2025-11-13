/**
 * PostgreSQL Connection Repository
 * Handles database operations for postgres_connections table
 */

import { Injectable, Inject } from '@nestjs/common';
import { eq, and, sql } from 'drizzle-orm';
import {
  postgresConnections,
  PostgresConnection,
  NewPostgresConnection,
} from '../../../../database/drizzle/schema/postgres-connectors.schema';
import { EncryptionService } from '../../../../common/encryption/encryption.service';
import { DecryptedConnectionCredentials } from '../postgres.types';
import type { DrizzleDatabase } from '../../../../database/drizzle/database';

@Injectable()
export class PostgresConnectionRepository {
  constructor(
    @Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase,
    private readonly encryptionService: EncryptionService,
  ) {}

  /**
   * Create connection with encrypted credentials
   */
  async create(
    data: Omit<
      NewPostgresConnection,
      | 'host'
      | 'database'
      | 'username'
      | 'password'
      | 'sslCaCert'
      | 'sshHost'
      | 'sshUsername'
      | 'sshPrivateKey'
    > & {
      host: string;
      database: string;
      username: string;
      password: string;
      sslCaCert?: string;
      sshHost?: string;
      sshUsername?: string;
      sshPrivateKey?: string;
    },
  ): Promise<PostgresConnection> {
    // Encrypt sensitive fields
    const encrypted = this.encryptionService.encryptFields({
      host: data.host,
      database: data.database,
      username: data.username,
      password: data.password,
      ...(data.sslCaCert && { sslCaCert: data.sslCaCert }),
      ...(data.sshHost && { sshHost: data.sshHost }),
      ...(data.sshUsername && { sshUsername: data.sshUsername }),
      ...(data.sshPrivateKey && { sshPrivateKey: data.sshPrivateKey }),
    });

    const connectionData: NewPostgresConnection = {
      ...data,
      host: encrypted.host,
      database: encrypted.database,
      username: encrypted.username,
      password: encrypted.password,
      sslCaCert: encrypted.sslCaCert,
      sshHost: encrypted.sshHost,
      sshUsername: encrypted.sshUsername,
      sshPrivateKey: encrypted.sshPrivateKey,
    };

    // Insert into database using Drizzle
    try {
      console.log('Attempting to insert connection into database...');
      console.log('Connection data keys:', Object.keys(connectionData));
      
      const [connection] = await this.db
        .insert(postgresConnections)
        .values(connectionData)
        .returning();

      console.log('Connection saved successfully with ID:', connection.id);
      return connection;
    } catch (error) {
      // Log the full error for debugging
      console.error('Failed to insert connection into database:');
      console.error('Error type:', error?.constructor?.name);
      console.error('Error message:', error instanceof Error ? error.message : String(error));
      console.error('Error details:', JSON.stringify(error, null, 2));
      console.error('Connection data that failed:', JSON.stringify(connectionData, null, 2));
      
      throw new Error(
        `Failed to save connection to database: ${error instanceof Error ? error.message : 'Unknown error'}`,
      );
    }
  }

  /**
   * Find connection by ID
   */
  async findById(
    id: string,
    orgId?: string,
  ): Promise<PostgresConnection | null> {
    const conditions = orgId
      ? and(eq(postgresConnections.id, id), eq(postgresConnections.orgId, orgId))
      : eq(postgresConnections.id, id);
    
    const [connection] = await this.db
      .select()
      .from(postgresConnections)
      .where(conditions)
      .limit(1);
    
    return connection || null;
  }

  /**
   * Find all connections for organization
   */
  async findByOrgId(orgId: string): Promise<PostgresConnection[]> {
    return await this.db
      .select()
      .from(postgresConnections)
      .where(eq(postgresConnections.orgId, orgId));
  }

  /**
   * Count connections for organization
   */
  async countByOrgId(orgId: string): Promise<number> {
    const result = await this.db
      .select({ count: sql<number>`count(*)::int` })
      .from(postgresConnections)
      .where(eq(postgresConnections.orgId, orgId));
    return result[0]?.count || 0;
  }

  /**
   * Update connection
   */
  async update(
    id: string,
    data: Partial<NewPostgresConnection>,
  ): Promise<PostgresConnection> {
    // Encrypt sensitive fields if provided
    const encrypted: Partial<NewPostgresConnection> = { ...data };
    if (data.host) encrypted.host = this.encryptionService.encrypt(data.host);
    if (data.database)
      encrypted.database = this.encryptionService.encrypt(data.database);
    if (data.username)
      encrypted.username = this.encryptionService.encrypt(data.username);
    if (data.password)
      encrypted.password = this.encryptionService.encrypt(data.password);
    if (data.sslCaCert)
      encrypted.sslCaCert = this.encryptionService.encrypt(data.sslCaCert);
    if (data.sshHost)
      encrypted.sshHost = this.encryptionService.encrypt(data.sshHost);
    if (data.sshUsername)
      encrypted.sshUsername = this.encryptionService.encrypt(data.sshUsername);
    if (data.sshPrivateKey)
      encrypted.sshPrivateKey = this.encryptionService.encrypt(
        data.sshPrivateKey,
      );

    encrypted.updatedAt = new Date();

    const [connection] = await this.db
      .update(postgresConnections)
      .set(encrypted)
      .where(eq(postgresConnections.id, id))
      .returning();
    
    return connection;
  }

  /**
   * Delete connection
   */
  async delete(id: string): Promise<void> {
    await this.db
      .delete(postgresConnections)
      .where(eq(postgresConnections.id, id));
  }

  /**
   * Decrypt connection credentials
   */
  decryptCredentials(
    connection: PostgresConnection,
  ): DecryptedConnectionCredentials {
    // Validate that required encrypted fields exist
    if (!connection.host || !connection.database || !connection.username || !connection.password) {
      throw new Error('Connection is missing required credentials. Connection may not be properly initialized.');
    }

    return {
      host: this.encryptionService.decrypt(connection.host),
      port: connection.port,
      database: this.encryptionService.decrypt(connection.database),
      username: this.encryptionService.decrypt(connection.username),
      password: this.encryptionService.decrypt(connection.password),
      sslEnabled: connection.sslEnabled,
      sslCaCert: connection.sslCaCert
        ? this.encryptionService.decrypt(connection.sslCaCert)
        : undefined,
      sshTunnelEnabled: connection.sshTunnelEnabled,
      sshHost: connection.sshHost
        ? this.encryptionService.decrypt(connection.sshHost)
        : undefined,
      sshPort: connection.sshPort || undefined,
      sshUsername: connection.sshUsername
        ? this.encryptionService.decrypt(connection.sshUsername)
        : undefined,
      sshPrivateKey: connection.sshPrivateKey
        ? this.encryptionService.decrypt(connection.sshPrivateKey)
        : undefined,
      connectionPoolSize: connection.connectionPoolSize,
    };
  }
}
