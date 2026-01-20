/**
 * Source Handlers Index
 * Exports all data source handlers
 * 
 * Adding a new source handler:
 * 1. Create a new file: {source-name}.handler.ts
 * 2. Implement ISourceHandler interface
 * 3. Export it from this file
 * 4. Register it in handler-registry.ts
 */

export { PostgresHandler } from './postgres.handler';
export { MySQLHandler } from './mysql.handler';
export { MongoDBHandler } from './mongodb.handler';
export { S3Handler } from './s3.handler';
export { APIHandler } from './api.handler';
export { BigQueryHandler } from './bigquery.handler';
export { SnowflakeHandler } from './snowflake.handler';
export { createHandlerRegistry, getHandler, hasHandler, getRegisteredTypes } from './handler-registry';

// Re-export types
export * from '../../types/source-handler.types';
