import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { BullModule } from '@nestjs/bullmq';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { PostgresDataSourceModule } from './modules/data-sources/postgres/postgres-data-source.module';
import { DataPipelineModule } from './modules/data-pipelines/data-pipeline.module';
import { OrganizationModule } from './modules/organizations/organization.module';
import { UserModule } from './modules/users/user.module';
import { OnboardingModule } from './modules/onboarding/onboarding.module';

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
    PostgresDataSourceModule,
    DataPipelineModule,
    OrganizationModule,
    UserModule,
    OnboardingModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule { }
