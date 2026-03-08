import { Module } from '@nestjs/common';
import { DataSourceModule } from '../data-sources/data-source.module';
import { InternalController } from './internal.controller';

@Module({
  imports: [DataSourceModule],
  controllers: [InternalController],
})
export class InternalModule {}
