import { forwardRef, Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { ConnectorsController } from './connectors.controller';
import { ConnectorMetadataService } from './connector-metadata.service';
import { DataSourceModule } from '../data-sources/data-source.module';

@Module({
  imports: [
    HttpModule.register({
      timeout: 10000,
      maxRedirects: 5,
    }),
    forwardRef(() => DataSourceModule),
  ],
  controllers: [ConnectorsController],
  providers: [ConnectorMetadataService],
  exports: [ConnectorMetadataService],
})
export class ConnectorsModule {}
