/**
 * Pipeline Source Schema Repository
 * Data access layer for pipeline_source_schemas table
 */

import { Inject, Injectable, NotFoundException } from '@nestjs/common';
import { and, count as drizzleCount, desc, eq, isNull } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import type { PaginatedResult } from '../../../common/dto/pagination-query.dto';
import type { NewPipelineSourceSchema, PipelineSourceSchema } from '../../../database/schemas';
import { pipelineSourceSchemas } from '../../../database/schemas';

@Injectable()
export class PipelineSourceSchemaRepository {
  constructor(
    @Inject('DRIZZLE_DB')
    private readonly db: NodePgDatabase<any>,
  ) {}

  /**
   * Create source schema
   */
  async create(schema: NewPipelineSourceSchema): Promise<PipelineSourceSchema> {
    const [created] = await this.db.insert(pipelineSourceSchemas).values(schema).returning();
    return created;
  }

  /**
   * Find by ID
   */
  async findById(id: string): Promise<PipelineSourceSchema | null> {
    const [schema] = await this.db
      .select()
      .from(pipelineSourceSchemas)
      .where(and(eq(pipelineSourceSchemas.id, id), isNull(pipelineSourceSchemas.deletedAt)))
      .limit(1);

    return schema || null;
  }

  /**
   * Find by organization
   */
  async findByOrganization(organizationId: string): Promise<PipelineSourceSchema[]> {
    return await this.db
      .select()
      .from(pipelineSourceSchemas)
      .where(
        and(
          eq(pipelineSourceSchemas.organizationId, organizationId),
          isNull(pipelineSourceSchemas.deletedAt),
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
  ): Promise<PaginatedResult<PipelineSourceSchema>> {
    const conditions = [
      eq(pipelineSourceSchemas.organizationId, organizationId),
      isNull(pipelineSourceSchemas.deletedAt),
    ];

    const [countResult, data] = await Promise.all([
      this.db
        .select({ count: drizzleCount() })
        .from(pipelineSourceSchemas)
        .where(and(...conditions)),
      this.db
        .select()
        .from(pipelineSourceSchemas)
        .where(and(...conditions))
        .orderBy(desc(pipelineSourceSchemas.createdAt))
        .limit(limit)
        .offset(offset),
    ]);

    return {
      data,
      total: Number(countResult[0]?.count || 0),
    };
  }

  /**
   * Update source schema
   */
  async update(id: string, updates: Partial<PipelineSourceSchema>): Promise<PipelineSourceSchema> {
    const [updated] = await this.db
      .update(pipelineSourceSchemas)
      .set({ ...updates, updatedAt: new Date() })
      .where(eq(pipelineSourceSchemas.id, id))
      .returning();

    if (!updated) {
      throw new NotFoundException(`Source schema ${id} not found`);
    }

    return updated;
  }

  /**
   * Soft delete
   */
  async delete(id: string): Promise<void> {
    await this.db
      .update(pipelineSourceSchemas)
      .set({ deletedAt: new Date() })
      .where(eq(pipelineSourceSchemas.id, id));
  }
}
