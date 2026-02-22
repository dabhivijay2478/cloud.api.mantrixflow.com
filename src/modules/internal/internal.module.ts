import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createDrizzleDatabase } from '../../database/drizzle/database';
import { DataSourceModule } from '../data-sources/data-source.module';
import { InternalController } from './internal.controller';

@Module({
  imports: [DataSourceModule],
  controllers: [InternalController],
  providers: [
    {
      provide: 'DRIZZLE_DB',
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => createDrizzleDatabase(configService),
    },
  ],
})
export class InternalModule {}
