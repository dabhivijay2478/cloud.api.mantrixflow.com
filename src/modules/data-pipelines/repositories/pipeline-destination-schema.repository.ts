/**
 * Pipeline Destination Schema Repository
 * Data access layer for destination schema entities
 */

import {
  type NewPipelineDestinationSchema,
  type PipelineDestinationSchema,
  pipelineDestinationSchemas,
} from '../../../database/schemas';
import { Inject, Injectable, NotFoundException } from '@nestjs/common';
import { and, eq, isNull } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';

@Injectable()
export class PipelineDestinationSchemaRepository {
  constructor(
    @Inject('DRIZZLE_DB')
    private readonly db: NodePgDatabase<any>,
  ) {}

  /**
   * Create new destination schema
   */
  async create(schema: NewPipelineDestinationSchema): Promise<PipelineDestinationSchema> {
    const [created] = await this.db.insert(pipelineDestinationSchemas).values(schema).returning();
    return created;
  }

  /**
   * Find destination schema by ID
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
}
