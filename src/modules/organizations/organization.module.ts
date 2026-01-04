import { Module, forwardRef } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { OrganizationController } from './organization.controller';
import { OrganizationMemberController } from './organization-member.controller';
import { OrganizationService } from './organization.service';
import { OrganizationMemberService } from './organization-member.service';
import { OrganizationRepository } from './repositories/organization.repository';
import { OrganizationMemberRepository } from './repositories/organization-member.repository';
import { UserModule } from '../users/user.module';
import { createDrizzleDatabase } from '../../database/drizzle/database';

@Module({
  imports: [forwardRef(() => UserModule)],
  controllers: [OrganizationController, OrganizationMemberController],
  providers: [
    // Database provider
    {
      provide: 'DRIZZLE_DB',
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => {
        return createDrizzleDatabase(configService);
      },
    },
    OrganizationService,
    OrganizationRepository,
    OrganizationMemberService,
    OrganizationMemberRepository,
  ],
  exports: [OrganizationService, OrganizationMemberService],
})
export class OrganizationModule {}
