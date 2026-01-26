/**
 * RabbitMQ Service
 * Service for publishing and consuming messages via RabbitMQ
 * Handles job queuing, scheduling, and pub/sub messaging
 */

import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as amqp from 'amqplib';

// Job types for pipeline operations
export interface FullSyncJobData {
  pipelineId: string;
  organizationId: string;
  userId: string;
  triggerType: 'manual' | 'scheduled';
  batchSize?: number;
}

export interface IncrementalSyncJobData {
  pipelineId: string;
  organizationId: string;
  userId: string;
  triggerType: 'polling' | 'manual' | 'resume';
  checkpoint: {
    watermarkField?: string;
    lastValue?: string | number;
    walPosition?: string;
    lsn?: string;
    slotName?: string;
    publicationName?: string;
    pauseTimestamp?: string;
  };
  batchSize?: number;
}

export interface DeltaCheckJobData {
  pipelineId: string;
  organizationId: string;
}

export interface StatusUpdateEventData {
  pipelineId: string;
  organizationId: string;
  status: string;
  rowsProcessed?: number;
  newRowsCount?: number;
  error?: string;
  timestamp: string;
}

// Queue names
export const QUEUE_NAMES = {
  FULL_SYNC: 'pipeline-full-sync',
  INCREMENTAL_SYNC: 'pipeline-incremental-sync',
  DELTA_CHECK: 'delta-check',
} as const;

// Exchange name for pub/sub
export const EXCHANGE_NAME = 'pipeline-events';

@Injectable()
export class RabbitMQService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RabbitMQService.name);
  private connection: amqp.Connection | null = null;
  private channel: amqp.Channel | null = null;
  private exchangeChannel: amqp.Channel | null = null;
  private readonly rabbitmqUrl: string;

  constructor(private readonly configService: ConfigService) {
    this.rabbitmqUrl =
      this.configService.get<string>('RABBITMQ_URL') ||
      this.configService.get<string>('AMQP_URL') ||
      'amqp://guest:guest@localhost:5672';
  }

  async onModuleInit(): Promise<void> {
    await this.connect();
    await this.setupQueues();
    await this.setupExchange();
  }

  async onModuleDestroy(): Promise<void> {
    await this.disconnect();
  }

  /**
   * Connect to RabbitMQ
   */
  private async connect(): Promise<void> {
    try {
      this.logger.log('Connecting to RabbitMQ...');
      const conn = await amqp.connect(this.rabbitmqUrl);
      this.connection = conn as any;

      if (this.connection) {
        this.connection.on('error', (err: Error) => {
          this.logger.error(`RabbitMQ connection error: ${err.message}`);
        });

        this.connection.on('close', () => {
          this.logger.warn('RabbitMQ connection closed, attempting reconnect...');
          setTimeout(() => this.connect(), 5000);
        });

        this.channel = await (this.connection as any).createChannel();
        this.exchangeChannel = await (this.connection as any).createChannel();
      }

      this.logger.log('✅ Connected to RabbitMQ');
    } catch (error) {
      this.logger.error(`Failed to connect to RabbitMQ: ${error}`);
      throw error;
    }
  }

  /**
   * Disconnect from RabbitMQ
   */
  private async disconnect(): Promise<void> {
    try {
      if (this.exchangeChannel) {
        await this.exchangeChannel.close();
      }
      if (this.channel) {
        await this.channel.close();
      }
      if (this.connection) {
        await (this.connection as any).close();
      }
      this.logger.log('Disconnected from RabbitMQ');
    } catch (error) {
      this.logger.error(`Error disconnecting from RabbitMQ: ${error}`);
    }
  }

  /**
   * Setup queues
   */
  private async setupQueues(): Promise<void> {
    if (!this.channel) {
      throw new Error('RabbitMQ channel not initialized');
    }

    // Declare queues with durability
    await this.channel.assertQueue(QUEUE_NAMES.FULL_SYNC, {
      durable: true,
    });

    await this.channel.assertQueue(QUEUE_NAMES.INCREMENTAL_SYNC, {
      durable: true,
    });

    await this.channel.assertQueue(QUEUE_NAMES.DELTA_CHECK, {
      durable: true,
    });

    this.logger.log('✅ Queues declared');
  }

  /**
   * Setup topic exchange for pub/sub
   */
  private async setupExchange(): Promise<void> {
    if (!this.exchangeChannel) {
      throw new Error('RabbitMQ exchange channel not initialized');
    }

    await this.exchangeChannel.assertExchange(EXCHANGE_NAME, 'topic', {
      durable: true,
    });

    this.logger.log('✅ Exchange declared');
  }

  /**
   * Enqueue full sync job
   */
  async enqueueFullSync(data: FullSyncJobData): Promise<void> {
    if (!this.channel) {
      throw new Error('RabbitMQ channel not initialized');
    }

    await this.channel.sendToQueue(QUEUE_NAMES.FULL_SYNC, Buffer.from(JSON.stringify(data)), {
      persistent: true,
      priority: data.triggerType === 'manual' ? 10 : 5,
    });

    this.logger.debug(`Enqueued full sync job for pipeline ${data.pipelineId}`);
  }

  /**
   * Enqueue incremental sync job
   */
  async enqueueIncrementalSync(data: IncrementalSyncJobData): Promise<void> {
    if (!this.channel) {
      throw new Error('RabbitMQ channel not initialized');
    }

    await this.channel.sendToQueue(
      QUEUE_NAMES.INCREMENTAL_SYNC,
      Buffer.from(JSON.stringify(data)),
      {
        persistent: true,
        priority: data.triggerType === 'manual' ? 10 : 5,
      },
    );

    this.logger.debug(`Enqueued incremental sync job for pipeline ${data.pipelineId}`);
  }

  /**
   * Enqueue delta check job
   */
  async enqueueDeltaCheck(data: DeltaCheckJobData): Promise<void> {
    if (!this.channel) {
      throw new Error('RabbitMQ channel not initialized');
    }

    await this.channel.sendToQueue(QUEUE_NAMES.DELTA_CHECK, Buffer.from(JSON.stringify(data)), {
      persistent: true,
    });

    this.logger.debug(`Enqueued delta check job for pipeline ${data.pipelineId}`);
  }

  /**
   * Publish status update event
   */
  async publishStatusUpdate(data: StatusUpdateEventData): Promise<void> {
    if (!this.exchangeChannel) {
      throw new Error('RabbitMQ exchange channel not initialized');
    }

    await this.exchangeChannel.publish(
      EXCHANGE_NAME,
      `pipeline.${data.pipelineId}.status`,
      Buffer.from(JSON.stringify(data)),
      {
        persistent: true,
      },
    );

    this.logger.debug(`Published status update for pipeline ${data.pipelineId}`);
  }

  /**
   * Consume messages from a queue
   */
  async consumeQueue<T>(
    queueName: string,
    handler: (data: T, ack: () => void, nack: () => void) => Promise<void>,
  ): Promise<void> {
    if (!this.channel) {
      throw new Error('RabbitMQ channel not initialized');
    }

    await this.channel.consume(
      queueName,
      async (msg) => {
        if (!msg) {
          return;
        }

        try {
          const data = JSON.parse(msg.content.toString()) as T;
          await handler(
            data,
            () => this.channel!.ack(msg),
            () => this.channel!.nack(msg, false, true), // Requeue on failure
          );
        } catch (error) {
          this.logger.error(`Error processing message from ${queueName}: ${error}`);
          if (this.channel) {
            this.channel.nack(msg, false, true); // Requeue
          }
        }
      },
      {
        noAck: false,
      },
    );

    this.logger.log(`✅ Started consuming from queue: ${queueName}`);
  }

  /**
   * Subscribe to status update events
   */
  async subscribeToStatusUpdates(
    routingKey: string,
    handler: (data: StatusUpdateEventData) => Promise<void>,
  ): Promise<void> {
    if (!this.exchangeChannel) {
      throw new Error('RabbitMQ exchange channel not initialized');
    }

    // Create a temporary queue for this subscription
    const queueResult = await this.exchangeChannel.assertQueue('', {
      exclusive: true,
      autoDelete: true,
    });

    await this.exchangeChannel.bindQueue(queueResult.queue, EXCHANGE_NAME, routingKey);

    await this.exchangeChannel.consume(
      queueResult.queue,
      async (msg) => {
        if (!msg) {
          return;
        }

        try {
          const data = JSON.parse(msg.content.toString()) as StatusUpdateEventData;
          await handler(data);
          this.exchangeChannel!.ack(msg);
        } catch (error) {
          this.logger.error(`Error processing status update: ${error}`);
          this.exchangeChannel!.nack(msg, false, true);
        }
      },
      {
        noAck: false,
      },
    );

    this.logger.log(`✅ Subscribed to status updates with routing key: ${routingKey}`);
  }

  /**
   * Schedule a delayed message (using RabbitMQ delayed message plugin)
   */
  async scheduleDelayedMessage<T>(queueName: string, data: T, delayMs: number): Promise<void> {
    if (!this.channel) {
      throw new Error('RabbitMQ channel not initialized');
    }

    // Requires rabbitmq-delayed-message-exchange plugin
    await this.channel.sendToQueue(queueName, Buffer.from(JSON.stringify(data)), {
      persistent: true,
      headers: {
        'x-delay': delayMs,
      },
    });

    this.logger.debug(`Scheduled delayed message for queue ${queueName} (${delayMs}ms delay)`);
  }

  /**
   * Check if service is ready
   */
  isReady(): boolean {
    return this.connection !== null && this.channel !== null;
  }
}
