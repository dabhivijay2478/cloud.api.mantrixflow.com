import { forwardRef, Module } from '@nestjs/common';
import { OrganizationModule } from '../organizations/organization.module';
import { UserRepository } from './repositories/user.repository';
import { UserController } from './user.controller';
import { UserService } from './user.service';
import { SupabaseUserWebhookController } from './webhooks/supabase-user-webhook.controller';

@Module({
  imports: [forwardRef(() => OrganizationModule)],
  controllers: [UserController, SupabaseUserWebhookController],
  providers: [UserService, UserRepository],
  exports: [UserService, UserRepository],
})
export class UserModule {}
