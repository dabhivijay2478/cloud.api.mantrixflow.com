/**
 * RabbitMQ Module
 * Provides RabbitMQ message queue integration
 * Handles job queuing, scheduling, and pub/sub messaging
 */

import { DynamicModule, Global, Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { RabbitMQService } from './rabbitmq.service';

@Global()
@Module({})
export class RabbitMQModule {
  static forRootAsync(options?: {
    inject?: any[];
    useFactory?: (...args: any[]) => Promise<any> | any;
  }): DynamicModule {
    return {
      module: RabbitMQModule,
      providers: [RabbitMQService],
      exports: [RabbitMQService],
    };
  }

  static forRoot(): DynamicModule {
    return {
      module: RabbitMQModule,
      providers: [RabbitMQService],
      exports: [RabbitMQService],
    };
  }
}
