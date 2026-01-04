/**
 * Pipeline Source Schema Repository
 * Data access layer for source schema entities
 */

import { Injectable, Inject, NotFoundException } from '@nestjs/common';
import { NodePgDatabase } from 'drizzle-orm/node-postgres';
import { eq, and, isNull } from 'drizzle-orm';
import {
  pipelineSourceSchemas,
  type PipelineSourceSchema,
  type NewPipelineSourceSchema,
} from '@db/schema';

@Injectable()
export class PipelineSourceSchemaRepository {
  constructor(
    @Inject('DRIZZLE_DB')
    private readonly db: NodePgDatabase<any>,
  ) {}

  /**
   * Create new source schema
   */
  async create(schema: NewPipelineSourceSchema): Promise<PipelineSourceSchema> {
    const [created] = await this.db
      .insert(pipelineSourceSchemas)
      .values(schema)
      .returning();
    return created;
  }

  /**
   * Find source schema by ID
   */
  async findById(id: string): Promise<PipelineSourceSchema | null> {
    const [schema] = await this.db
      .select()
      .from(pipelineSourceSchemas)
      .where(
        and(
          eq(pipelineSourceSchemas.id, id),
          isNull(pipelineSourceSchemas.deletedAt),
        ),
      )
      .limit(1);

    return schema || null;
  }

  /**
   * Update source schema
   */
  async update(
    id: string,
    updates: Partial<PipelineSourceSchema>,
  ): Promise<PipelineSourceSchema> {
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
}

