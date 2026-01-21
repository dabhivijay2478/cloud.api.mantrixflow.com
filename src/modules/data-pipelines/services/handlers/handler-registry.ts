/**
 * Handler Registry
 * Central registry for all data source handlers
 * 
 * This follows the Strategy Pattern for extensibility:
 * - Each handler implements ISourceHandler
 * - Handlers are registered by their DataSourceType
 * - CollectorService uses this registry to get the appropriate handler
 * 
 * ADDING A NEW SOURCE:
 * 1. Add the new type to DataSourceType enum (types/common.types.ts)
 * 2. Create a new handler file (e.g., kafka.handler.ts)
 * 3. Implement ISourceHandler interface
 * 4. Register it in this file's createHandlerRegistry function
 * 5. No changes needed to CollectorService or other core code!
 */

/**
 * Handler Registry
 * Central registry for all data source handlers
 * 
 * This follows the Strategy Pattern for extensibility:
 * - Each handler implements ISourceHandler
 * - Handlers are registered by their DataSourceType
 * - CollectorService uses this registry to get the appropriate handler
 * 
 * ADDING A NEW SOURCE:
 * 1. Add the new type to DataSourceType enum (types/common.types.ts)
 * 2. Create a new handler file (e.g., kafka.handler.ts)
 * 3. Implement ISourceHandler interface with collectIncremental method
 * 4. Register it in this file's createHandlerRegistry function
 * 5. No changes needed to CollectorService or other core code!
 */

import { DataSourceType } from '../../types/common.types';
import { ISourceHandler, SourceHandlerRegistry } from '../../types/source-handler.types';
import { PostgresHandler } from './postgres.handler';
import { MySQLHandler } from './mysql.handler';
import { MongoDBHandler } from './mongodb.handler';

/**
 * Create and populate the handler registry
 * This is the single place where all handlers are registered
 * 
 * Only PostgreSQL, MySQL, and MongoDB handlers are registered
 */
export function createHandlerRegistry(): SourceHandlerRegistry {
  const registry: SourceHandlerRegistry = new Map();

  // Register only the three supported handlers
  const handlers: ISourceHandler[] = [
    new PostgresHandler(),
    new MySQLHandler(),
    new MongoDBHandler(),
  ];

  // Add to registry
  for (const handler of handlers) {
    registry.set(handler.type, handler);
  }

  return registry;
}

/**
 * Get handler for a specific data source type
 */
export function getHandler(
  registry: SourceHandlerRegistry,
  type: DataSourceType | string,
): ISourceHandler | undefined {
  // Normalize type to enum value
  const normalizedType = (type.toLowerCase() as DataSourceType);
  return registry.get(normalizedType);
}

/**
 * Check if a handler exists for the given type
 */
export function hasHandler(
  registry: SourceHandlerRegistry,
  type: DataSourceType | string,
): boolean {
  const normalizedType = (type.toLowerCase() as DataSourceType);
  return registry.has(normalizedType);
}

/**
 * Get all registered handler types
 */
export function getRegisteredTypes(registry: SourceHandlerRegistry): DataSourceType[] {
  return Array.from(registry.keys());
}
