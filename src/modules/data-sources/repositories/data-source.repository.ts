/**
 * Data Source Repository
 * Database operations for data_sources table
 */

import { Inject, Injectable } from '@nestjs/common';
import { and, count as drizzleCount, desc, eq, isNull } from 'drizzle-orm';
import type { DrizzleDatabase } from '../../../database/drizzle/database';
import type { PaginatedResult } from '../../../common/dto/pagination-query.dto';
import {
  type DataSource,
  type NewDataSource,
  dataSources,
} from '../../../database/schemas/data-sources';

@Injectable()
export class DataSourceRepository {
  constructor(@Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase) {}

  /**
   * Create a new data source
   */
  async create(data: NewDataSource): Promise<DataSource> {
    const [dataSource] = await this.db.insert(dataSources).values(data).returning();
    return dataSource;
  }

  /**
   * Find data source by ID
   */
  async findById(id: string): Promise<DataSource | null> {
    const [dataSource] = await this.db
      .select()
      .from(dataSources)
      .where(and(eq(dataSources.id, id), isNull(dataSources.deletedAt)))
      .limit(1);
    return dataSource || null;
  }

  /**
   * Find data source by name in organization
   */
  async findByName(organizationId: string, name: string): Promise<DataSource | null> {
    const [dataSource] = await this.db
      .select()
      .from(dataSources)
      .where(
        and(
          eq(dataSources.organizationId, organizationId),
          eq(dataSources.name, name),
          isNull(dataSources.deletedAt),
        ),
      )
      .limit(1);
    return dataSource || null;
  }

  /**
   * Find all data sources for an organization
   */
  async findByOrganization(
    organizationId: string,
    filters?: {
      sourceType?: string;
      isActive?: boolean;
    },
  ): Promise<DataSource[]> {
    const conditions = [
      eq(dataSources.organizationId, organizationId),
      isNull(dataSources.deletedAt),
    ];

    if (filters?.sourceType) {
      conditions.push(eq(dataSources.sourceType, filters.sourceType));
    }

    if (filters?.isActive !== undefined) {
      conditions.push(eq(dataSources.isActive, filters.isActive));
    }

    return this.db
      .select()
      .from(dataSources)
      .where(and(...conditions));
  }

  /**
   * Find all data sources for an organization with pagination
   */
  async findByOrganizationPaginated(
    organizationId: string,
    limit: number = 20,
    offset: number = 0,
    filters?: { sourceType?: string; isActive?: boolean },
  ): Promise<PaginatedResult<DataSource>> {
    const conditions = [
      eq(dataSources.organizationId, organizationId),
      isNull(dataSources.deletedAt),
    ];

    if (filters?.sourceType) {
      conditions.push(eq(dataSources.sourceType, filters.sourceType));
    }

    if (filters?.isActive !== undefined) {
      conditions.push(eq(dataSources.isActive, filters.isActive));
    }

    const [countResult, data] = await Promise.all([
      this.db
        .select({ count: drizzleCount() })
        .from(dataSources)
        .where(and(...conditions)),
      this.db
        .select()
        .from(dataSources)
        .where(and(...conditions))
        .orderBy(desc(dataSources.createdAt))
        .limit(limit)
        .offset(offset),
    ]);

    return {
      data,
      total: Number(countResult[0]?.count || 0),
    };
  }

  /**
   * Update data source
   */
  async update(id: string, data: Partial<NewDataSource>): Promise<DataSource> {
    const [dataSource] = await this.db
      .update(dataSources)
      .set({ ...data, updatedAt: new Date() })
      .where(eq(dataSources.id, id))
      .returning();
    return dataSource;
  }

  /**
   * Soft delete data source
   */
  async softDelete(id: string): Promise<void> {
    await this.db
      .update(dataSources)
      .set({ deletedAt: new Date(), updatedAt: new Date() })
      .where(eq(dataSources.id, id));
  }

  /**
   * Hard delete data source
   */
  async hardDelete(id: string): Promise<void> {
    await this.db.delete(dataSources).where(eq(dataSources.id, id));
  }
}
