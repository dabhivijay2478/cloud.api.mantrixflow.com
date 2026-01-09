import { BullModule } from '@nestjs/bullmq';
import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { ActivityLogModule } from './modules/activity-logs/activity-log.module';
import { DataPipelineModule } from './modules/data-pipelines/data-pipeline.module';
import { PostgresDataSourceModule } from './modules/data-sources/postgres/postgres-data-source.module';
import { OnboardingModule } from './modules/onboarding/onboarding.module';
import { OrganizationModule } from './modules/organizations/organization.module';
import { UserModule } from './modules/users/user.module';

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
      useFactory: (configService: ConfigService) => {
        const redisUrl = configService.get<string>('REDIS_URL');

        // If REDIS_URL is provided, parse it
        if (redisUrl) {
          try {
            const url = new URL(redisUrl);
            return {
              connection: {
                host: url.hostname,
                port: parseInt(url.port, 10),
                password: url.password || undefined,
                username: url.username || undefined,
              },
            };
          } catch (error) {
            console.warn('Failed to parse REDIS_URL, falling back to individual config', error);
          }
        }

        // Fallback to individual configuration
        return {
          connection: {
            host: configService.get('REDIS_HOST', 'localhost'),
            port: configService.get<number>('REDIS_PORT', 6379),
            password: configService.get<string>('REDIS_PASSWORD'),
            username: configService.get<string>('REDIS_USERNAME'),
          },
        };
      },
    }),
    PostgresDataSourceModule,
    DataPipelineModule,
    OrganizationModule,
    UserModule,
    OnboardingModule,
    ActivityLogModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
