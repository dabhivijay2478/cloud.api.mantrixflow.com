import { Module } from '@nestjs/common';
import { ConnectorsController } from './connectors.controller';
import { EtlModule } from '../etl/etl.module';

@Module({
  imports: [EtlModule],
  controllers: [ConnectorsController],
})
export class ConnectorsModule {}
