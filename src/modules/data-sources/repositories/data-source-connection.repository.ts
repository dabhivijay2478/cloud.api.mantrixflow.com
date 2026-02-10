/**
 * Data Source Connection Repository
 * Database operations for data_source_connections table
 */

import { Inject, Injectable } from '@nestjs/common';
import { eq } from 'drizzle-orm';
import type { DrizzleDatabase } from '../../../database/drizzle/database';
import {
  type DataSourceConnection,
  type NewDataSourceConnection,
  dataSourceConnections,
} from '../../../database/schemas/data-sources';

@Injectable()
export class DataSourceConnectionRepository {
  constructor(@Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase) {}

  /**
   * Create a new connection
   */
  async create(data: NewDataSourceConnection): Promise<DataSourceConnection> {
    const [connection] = await this.db.insert(dataSourceConnections).values(data).returning();
    return connection;
  }

  /**
   * Find connection by data source ID
   */
  async findByDataSourceId(dataSourceId: string): Promise<DataSourceConnection | null> {
    const [connection] = await this.db
      .select()
      .from(dataSourceConnections)
      .where(eq(dataSourceConnections.dataSourceId, dataSourceId))
      .limit(1);
    return connection || null;
  }

  /**
   * Find connection by ID
   */
  async findById(id: string): Promise<DataSourceConnection | null> {
    const [connection] = await this.db
      .select()
      .from(dataSourceConnections)
      .where(eq(dataSourceConnections.id, id))
      .limit(1);
    return connection || null;
  }

  /**
   * List all connections
   */
  async findAll(): Promise<DataSourceConnection[]> {
    return this.db.select().from(dataSourceConnections);
  }

  /**
   * Update connection
   */
  async update(id: string, data: Partial<NewDataSourceConnection>): Promise<DataSourceConnection> {
    const [connection] = await this.db
      .update(dataSourceConnections)
      .set({ ...data, updatedAt: new Date() })
      .where(eq(dataSourceConnections.id, id))
      .returning();
    return connection;
  }

  /**
   * Update connection by data source ID
   */
  async updateByDataSourceId(
    dataSourceId: string,
    data: Partial<NewDataSourceConnection>,
  ): Promise<DataSourceConnection> {
    const [connection] = await this.db
      .update(dataSourceConnections)
      .set({ ...data, updatedAt: new Date() })
      .where(eq(dataSourceConnections.dataSourceId, dataSourceId))
      .returning();
    return connection;
  }

  /**
   * Delete connection
   */
  async delete(id: string): Promise<void> {
    await this.db.delete(dataSourceConnections).where(eq(dataSourceConnections.id, id));
  }

  /**
   * Delete connection by data source ID
   */
  async deleteByDataSourceId(dataSourceId: string): Promise<void> {
    await this.db
      .delete(dataSourceConnections)
      .where(eq(dataSourceConnections.dataSourceId, dataSourceId));
  }
}
