/**
 * Activity Log Module
 * Module for activity log management
 */

import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createDrizzleDatabase } from '../../database/drizzle/database';
import { ActivityLogController } from './activity-log.controller';
import { ActivityLogService } from './activity-log.service';
import { ActivityLogRepository } from './repositories/activity-log.repository';

@Module({
  controllers: [ActivityLogController],
  providers: [
    // Database provider
    {
      provide: 'DRIZZLE_DB',
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => {
        return createDrizzleDatabase(configService);
      },
    },
    ActivityLogService,
    ActivityLogRepository,
  ],
  exports: [ActivityLogService], // Export service so other modules can use it
})
export class ActivityLogModule {}
