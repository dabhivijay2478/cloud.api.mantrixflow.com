import { forwardRef, Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createDrizzleDatabase } from '../../database/drizzle/database';
import { OrganizationModule } from '../organizations/organization.module';
import { UserRepository } from './repositories/user.repository';
import { UserController } from './user.controller';
import { UserService } from './user.service';
import { SupabaseUserWebhookController } from './webhooks/supabase-user-webhook.controller';

@Module({
  imports: [forwardRef(() => OrganizationModule)],
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
