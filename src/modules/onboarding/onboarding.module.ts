import { Module } from '@nestjs/common';
import { OrganizationModule } from '../organizations/organization.module';
import { UserModule } from '../users/user.module';
import { OnboardingController } from './onboarding.controller';

@Module({
  imports: [UserModule, OrganizationModule],
  controllers: [OnboardingController],
})
export class OnboardingModule {}
