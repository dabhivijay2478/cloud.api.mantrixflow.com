import { forwardRef, Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createDrizzleDatabase } from '../../database/drizzle/database';
import { OrganizationRoleGuard } from '../../common/guards/organization-role.guard';
import { ActivityLogModule } from '../activity-logs/activity-log.module';
import { UserModule } from '../users/user.module';
import { OrganizationController } from './organization.controller';
import { OrganizationService } from './organization.service';
import { OrganizationMemberController } from './organization-member.controller';
import { OrganizationMemberService } from './organization-member.service';
import { OrganizationRepository } from './repositories/organization.repository';
import { OrganizationMemberRepository } from './repositories/organization-member.repository';
import { OrganizationOwnerRepository } from './repositories/organization-owner.repository';
import { OrganizationRoleService } from './services/organization-role.service';

@Module({
  imports: [forwardRef(() => UserModule), ActivityLogModule],
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
    OrganizationOwnerRepository,
    OrganizationRoleService,
    OrganizationRoleGuard,
  ],
  exports: [
    OrganizationService,
    OrganizationMemberService,
    OrganizationRepository,
    OrganizationMemberRepository,
    OrganizationRoleService,
  ],
})
export class OrganizationModule {}
