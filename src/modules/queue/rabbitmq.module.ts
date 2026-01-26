/**
 * RabbitMQ Module
 * Provides RabbitMQ message queue integration
 * Handles job queuing, scheduling, and pub/sub messaging
 */

import { DynamicModule, Global, Module } from '@nestjs/common';
import { RabbitMQService } from './rabbitmq.service';

@Global()
@Module({})
// biome-ignore lint/complexity/noStaticOnlyClass: NestJS module pattern requires static methods
export class RabbitMQModule {
  static forRootAsync(_options?: {
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
