/**
 * Global Search Module
 * Provides global search functionality across multiple entity types
 */

import { Module, OnModuleInit } from '@nestjs/common';
import { SearchController } from './search.controller';
import { SearchService } from './search.service';
import { UserSearchHandler } from './handlers/user-search.handler';
import { PipelineSearchHandler } from './handlers/pipeline-search.handler';
import { DataSourceSearchHandler } from './handlers/data-source-search.handler';
import { ConnectorSearchHandler } from './handlers/connector-search.handler';

@Module({
  controllers: [SearchController],
  providers: [
    SearchService,
    UserSearchHandler,
    PipelineSearchHandler,
    DataSourceSearchHandler,
    ConnectorSearchHandler,
  ],
  exports: [SearchService],
})
export class SearchModule implements OnModuleInit {
  constructor(
    private readonly searchService: SearchService,
    private readonly userHandler: UserSearchHandler,
    private readonly pipelineHandler: PipelineSearchHandler,
    private readonly dataSourceHandler: DataSourceSearchHandler,
    private readonly connectorHandler: ConnectorSearchHandler,
  ) {}

  onModuleInit() {
    // Register all handlers when module initializes
    this.searchService.registerHandler(this.userHandler);
    this.searchService.registerHandler(this.pipelineHandler);
    this.searchService.registerHandler(this.dataSourceHandler);
    this.searchService.registerHandler(this.connectorHandler);
  }
}
