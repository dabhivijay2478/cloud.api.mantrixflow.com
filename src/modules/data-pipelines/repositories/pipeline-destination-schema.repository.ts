/**
 * Pipeline Destination Schema Repository
 * Data access layer for pipeline_destination_schemas table
 */

import { Inject, Injectable, NotFoundException } from '@nestjs/common';
import { and, count as drizzleCount, desc, eq, isNull } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import type { PaginatedResult } from '../../../common/dto/pagination-query.dto';
import type {
  NewPipelineDestinationSchema,
  PipelineDestinationSchema,
} from '../../../database/schemas';
import { pipelineDestinationSchemas } from '../../../database/schemas';

@Injectable()
export class PipelineDestinationSchemaRepository {
  constructor(
    @Inject('DRIZZLE_DB')
    private readonly db: NodePgDatabase<any>,
  ) {}

  /**
   * Create destination schema
   */
  async create(schema: NewPipelineDestinationSchema): Promise<PipelineDestinationSchema> {
    const [created] = await this.db.insert(pipelineDestinationSchemas).values(schema).returning();
    return created;
  }

  /**
   * Find by ID
   */
  async findById(id: string): Promise<PipelineDestinationSchema | null> {
    const [schema] = await this.db
      .select()
      .from(pipelineDestinationSchemas)
      .where(
        and(eq(pipelineDestinationSchemas.id, id), isNull(pipelineDestinationSchemas.deletedAt)),
      )
      .limit(1);

    return schema || null;
  }

  /**
   * Find by organization
   */
  async findByOrganization(organizationId: string): Promise<PipelineDestinationSchema[]> {
    return await this.db
      .select()
      .from(pipelineDestinationSchemas)
      .where(
        and(
          eq(pipelineDestinationSchemas.organizationId, organizationId),
          isNull(pipelineDestinationSchemas.deletedAt),
        ),
      );
  }

  /**
   * Find by organization with pagination
   */
  async findByOrganizationPaginated(
    organizationId: string,
    limit: number = 20,
    offset: number = 0,
  ): Promise<PaginatedResult<PipelineDestinationSchema>> {
    const conditions = [
      eq(pipelineDestinationSchemas.organizationId, organizationId),
      isNull(pipelineDestinationSchemas.deletedAt),
    ];

    const [countResult, data] = await Promise.all([
      this.db
        .select({ count: drizzleCount() })
        .from(pipelineDestinationSchemas)
        .where(and(...conditions)),
      this.db
        .select()
        .from(pipelineDestinationSchemas)
        .where(and(...conditions))
        .orderBy(desc(pipelineDestinationSchemas.createdAt))
        .limit(limit)
        .offset(offset),
    ]);

    return {
      data,
      total: Number(countResult[0]?.count || 0),
    };
  }

  /**
   * Update destination schema
   */
  async update(
    id: string,
    updates: Partial<PipelineDestinationSchema>,
  ): Promise<PipelineDestinationSchema> {
    const [updated] = await this.db
      .update(pipelineDestinationSchemas)
      .set({ ...updates, updatedAt: new Date() })
      .where(eq(pipelineDestinationSchemas.id, id))
      .returning();

    if (!updated) {
      throw new NotFoundException(`Destination schema ${id} not found`);
    }

    return updated;
  }

  /**
   * Soft delete
   */
  async delete(id: string): Promise<void> {
    await this.db
      .update(pipelineDestinationSchemas)
      .set({ deletedAt: new Date() })
      .where(eq(pipelineDestinationSchemas.id, id));
  }
}
