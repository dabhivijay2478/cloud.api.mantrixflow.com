import { Module } from '@nestjs/common';
import { OnboardingController } from './onboarding.controller';
import { UserModule } from '../users/user.module';
import { OrganizationModule } from '../organizations/organization.module';

@Module({
  imports: [UserModule, OrganizationModule],
  controllers: [OnboardingController],
})
export class OnboardingModule {}
