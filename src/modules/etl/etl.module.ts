import { Module } from '@nestjs/common';
import { EtlService } from './etl.service';
import { EtlController } from './etl.controller';

@Module({
  controllers: [EtlController],
  providers: [EtlService],
  exports: [EtlService],
})
export class EtlModule {}
