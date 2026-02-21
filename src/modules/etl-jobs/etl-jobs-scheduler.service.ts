/**
 * ETL Jobs Scheduler
 * Calls processQueue every minute (alternative to pg_cron+pg_net)
 */

import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { EtlJobsService } from './etl-jobs.service';

@Injectable()
export class EtlJobsSchedulerService {
  private readonly logger = new Logger(EtlJobsSchedulerService.name);

  constructor(private readonly etlJobsService: EtlJobsService) {}

  @Cron('* * * * *') // Every minute
  async handleProcessQueue() {
    try {
      const processed = await this.etlJobsService.processQueue(5);
      if (processed > 0) {
        this.logger.log(`Processed ${processed} ETL job(s)`);
      }
    } catch (error) {
      this.logger.error(
        `processQueue failed: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }
}
