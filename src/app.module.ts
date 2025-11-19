import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { BullModule } from '@nestjs/bullmq';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { PostgresModule } from './modules/connectors/postgres/postgres.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: '.env',
      cache: true,
    }),
    // Global BullMQ configuration
    BullModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => ({
        connection: {
          host: configService.get('REDIS_HOST', 'localhost'),
          port: configService.get('REDIS_PORT', 6379),
          // Optional: Add password if Redis requires authentication
          // password: configService.get('REDIS_PASSWORD'),
        },
      }),
    }),
    PostgresModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule { }
