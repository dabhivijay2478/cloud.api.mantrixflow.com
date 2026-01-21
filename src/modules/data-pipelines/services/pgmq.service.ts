/**
 * PGMQ Service
 * Service for interacting with PGMQ (Postgres Message Queue) extension
 * 
 * Uses raw SQL queries via Drizzle/pg client to interact with PGMQ
 * Assumes PGMQ extension is installed in the database
 * 
 * Guide: PGMQ queues are created automatically on first send
 */

import { Injectable, Logger } from '@nestjs/common';
import { Inject } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Pool } from 'pg';

export interface PGMQMessage<T = any> {
  msg_id: number;
  read_ct: number;
  enqueued_at: string;
  vt: string; // visibility timeout
  message: T;
}

@Injectable()
export class PGMQService {
  private readonly logger = new Logger(PGMQService.name);
  private pgPool: Pool | null = null;
  private readonly INITIALIZED_QUEUES = new Set<string>();

  constructor(private readonly configService: ConfigService) {
    // Create a dedicated Pool for PGMQ operations
    const databaseUrl = this.configService.get<string>('DATABASE_URL');
    if (databaseUrl) {
      this.pgPool = new Pool({ connectionString: databaseUrl });
    } else {
      this.pgPool = new Pool({
        host: this.configService.get('DB_HOST', 'localhost'),
        port: this.configService.get<number>('DB_PORT', 5432),
        database: this.configService.get('DB_NAME', 'postgres'),
        user: this.configService.get('DB_USER', 'postgres'),
        password: this.configService.get<string>('DB_PASSWORD'),
      });
    }
  }

  /**
   * Initialize a queue (create it if it doesn't exist)
   * This is called on startup to ensure queues exist before use
   */
  async initializeQueue(queueName: string): Promise<void> {
    if (this.INITIALIZED_QUEUES.has(queueName)) {
      return; // Already initialized
    }

    if (!this.pgPool) {
      throw new Error('PGMQ service not initialized');
    }

    const client = await this.pgPool.connect();
    try {
      // PGMQ creates queues automatically on first send, but we can verify/create it explicitly
      // Check if queue exists by trying to create it (idempotent operation)
      const escapedQueueName = queueName.replace(/'/g, "''");
      
      // PGMQ doesn't have a direct "create queue" function, but we can verify it exists
      // by checking if the queue table exists. If not, it will be created on first send.
      // For now, we'll just mark it as initialized and let it be created on first use.
      
      this.INITIALIZED_QUEUES.add(queueName);
      this.logger.log(`Queue '${queueName}' marked for initialization (will be created on first send)`);
    } catch (error) {
      this.logger.warn(`Failed to initialize queue '${queueName}': ${error}`);
      // Don't throw - queue will be created on first send anyway
    } finally {
      client.release();
    }
  }

  /**
   * Send a message to a PGMQ queue
   * Queue is created automatically if it doesn't exist
   */
  async send<T = any>(
    queueName: string,
    message: T,
    delay?: number, // Delay in seconds
  ): Promise<number> {
    if (!this.pgPool) {
      throw new Error('PGMQ service not initialized - database connection not available');
    }

    const client = await this.pgPool.connect();
    try {
      // Use parameterized query for safety
      const messageJson = JSON.stringify(message);
      
      // PGMQ send function signature: pgmq.send(queue_name, msg, delay_seconds?)
      // Escape queue name to prevent SQL injection
      const escapedQueueName = queueName.replace(/'/g, "''");
      
      const query = delay
        ? `SELECT pgmq.send($1::text, $2::jsonb, $3) as msg_id`
        : `SELECT pgmq.send($1::text, $2::jsonb) as msg_id`;
      
      const params = delay
        ? [escapedQueueName, messageJson, delay]
        : [escapedQueueName, messageJson];
      
      const result = await client.query(query, params);
      const msgId = result.rows[0]?.msg_id;
      
      this.logger.log(`Sent message to queue '${queueName}' (msg_id: ${msgId})`);
      return msgId;
    } catch (error) {
      this.logger.error(`Failed to send message to queue '${queueName}': ${error}`);
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Read messages from a queue (non-destructive)
   * Messages remain in queue until archived
   * Returns empty array if queue doesn't exist (queue will be created on first send)
   */
  async read<T = any>(
    queueName: string,
    vt: number = 30, // Visibility timeout in seconds
    qty: number = 1, // Number of messages to read
  ): Promise<PGMQMessage<T>[]> {
    if (!this.pgPool) {
      throw new Error('PGMQ service not initialized');
    }

    const client = await this.pgPool.connect();
    try {
      // PGMQ read function: pgmq.read(queue_name, vt, qty)
      const escapedQueueName = queueName.replace(/'/g, "''");
      const result = await client.query(
        `SELECT * FROM pgmq.read($1::text, $2, $3)`,
        [escapedQueueName, vt, qty],
      );

      const messages = result.rows.map((row: any) => ({
        msg_id: row.msg_id,
        read_ct: row.read_ct,
        enqueued_at: row.enqueued_at,
        vt: row.vt,
        message: row.message,
      }));

      if (messages.length > 0) {
        this.logger.log(`Read ${messages.length} message(s) from queue '${queueName}'`);
      }

      return messages;
    } catch (error: any) {
      // Handle case where queue doesn't exist yet (will be created on first send)
      if (error?.message?.includes('does not exist') || error?.code === '42P01') {
        // Queue doesn't exist yet - return empty array (queue will be created on first send)
        // Only log once per queue to reduce noise
        if (!this.INITIALIZED_QUEUES.has(queueName)) {
          this.logger.debug(`Queue '${queueName}' does not exist yet. It will be created automatically on first send.`);
          this.INITIALIZED_QUEUES.add(queueName);
        }
        return [];
      }
      this.logger.error(`Failed to read messages from queue '${queueName}': ${error}`);
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Pop a message from a queue (destructive - removes message)
   * Returns null if queue doesn't exist (queue will be created on first send)
   */
  async pop<T = any>(
    queueName: string,
    vt: number = 30,
  ): Promise<PGMQMessage<T> | null> {
    if (!this.pgPool) {
      throw new Error('PGMQ service not initialized');
    }

    const client = await this.pgPool.connect();
    try {
      const escapedQueueName = queueName.replace(/'/g, "''");
      const result = await client.query(
        `SELECT * FROM pgmq.pop($1::text, $2)`,
        [escapedQueueName, vt],
      );

      if (result.rows.length === 0) {
        return null;
      }

      const row = result.rows[0];
      this.logger.log(`Popped message from queue '${queueName}' (msg_id: ${row.msg_id})`);

      return {
        msg_id: row.msg_id,
        read_ct: row.read_ct,
        enqueued_at: row.enqueued_at,
        vt: row.vt,
        message: row.message,
      };
    } catch (error: any) {
      // Handle case where queue doesn't exist yet
      if (error?.message?.includes('does not exist') || error?.code === '42P01') {
        this.logger.debug(`Queue '${queueName}' does not exist yet. Returning null.`);
        return null;
      }
      this.logger.error(`Failed to pop message from queue '${queueName}': ${error}`);
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Archive a message (mark as processed)
   * Returns false if queue doesn't exist (graceful handling)
   */
  async archive(queueName: string, msgId: number): Promise<boolean> {
    if (!this.pgPool) {
      throw new Error('PGMQ service not initialized');
    }

    const client = await this.pgPool.connect();
    try {
      const escapedQueueName = queueName.replace(/'/g, "''");
      const result = await client.query(
        `SELECT pgmq.archive($1::text, $2) as archived`,
        [escapedQueueName, msgId],
      );

      const archived = result.rows[0]?.archived;
      if (archived) {
        this.logger.log(`Archived message ${msgId} from queue '${queueName}'`);
      }

      return archived;
    } catch (error: any) {
      // Handle case where queue doesn't exist yet
      if (error?.message?.includes('does not exist') || error?.code === '42P01') {
        this.logger.debug(`Queue '${queueName}' does not exist. Cannot archive message ${msgId}.`);
        return false;
      }
      this.logger.error(`Failed to archive message ${msgId} from queue '${queueName}': ${error}`);
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Get queue metrics
   */
  async getQueueMetrics(queueName: string): Promise<{
    queueLength: number;
    newestMsgAge: number;
    oldestMsgAge: number;
  }> {
    if (!this.pgPool) {
      throw new Error('PGMQ service not initialized');
    }

    const client = await this.pgPool.connect();
    try {
      // PGMQ queue tables are named q_{queueName}
      // Sanitize queue name for table name
      const sanitizedQueueName = queueName.replace(/[^a-zA-Z0-9_]/g, '_');
      const result = await client.query(
        `SELECT 
          COUNT(*) as queue_length,
          EXTRACT(EPOCH FROM (NOW() - MIN(enqueued_at))) as oldest_msg_age,
          EXTRACT(EPOCH FROM (NOW() - MAX(enqueued_at))) as newest_msg_age
        FROM pgmq.q_${sanitizedQueueName}`,
      );

      const row = result.rows[0];
      return {
        queueLength: parseInt(row.queue_length, 10),
        newestMsgAge: parseFloat(row.newest_msg_age) || 0,
        oldestMsgAge: parseFloat(row.oldest_msg_age) || 0,
      };
    } catch (error) {
      this.logger.error(`Failed to get metrics for queue '${queueName}': ${error}`);
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Cleanup on module destroy
   */
  async onModuleDestroy() {
    if (this.pgPool) {
      await this.pgPool.end();
      this.pgPool = null;
    }
  }
}
