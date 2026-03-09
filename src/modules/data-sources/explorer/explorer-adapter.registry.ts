/**
 * Explorer Adapter Registry
 * Maps connection types to IExplorerDbAdapter implementations.
 * Adding a new RDBMS requires only creating an adapter and registering it here.
 */

import { BadRequestException } from '@nestjs/common';
import type { IExplorerDbAdapter } from './adapters/explorer-db.adapter.interface';
import { PostgresExplorerAdapter } from './adapters/postgres-explorer.adapter';

export class ExplorerAdapterRegistry {
  private readonly adapters = new Map<string, IExplorerDbAdapter>();

  constructor() {
    this.register(new PostgresExplorerAdapter());
  }

  register(adapter: IExplorerDbAdapter): void {
    this.adapters.set(adapter.type, adapter);
  }

  getAdapter(connectionType: string): IExplorerDbAdapter {
    const normalized = connectionType?.toLowerCase().trim() || 'postgres';
    const adapter = this.adapters.get(normalized);
    if (!adapter) {
      throw new BadRequestException(
        `Explorer is not supported for connection type "${connectionType}". ` +
          `Supported types: ${Array.from(this.adapters.keys()).join(', ')}.`,
      );
    }
    return adapter;
  }
}
