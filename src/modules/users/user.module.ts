import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { UserController } from './user.controller';
import { UserService } from './user.service';
import { UserRepository } from './repositories/user.repository';
import { SupabaseUserWebhookController } from './webhooks/supabase-user-webhook.controller';
import { createDrizzleDatabase } from '../../database/drizzle/database';

@Module({
  controllers: [UserController, SupabaseUserWebhookController],
  providers: [
    // Database provider
    {
      provide: 'DRIZZLE_DB',
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => {
        return createDrizzleDatabase(configService);
      },
    },
    UserService,
    UserRepository,
  ],
  exports: [UserService, UserRepository],
})
export class UserModule {}
