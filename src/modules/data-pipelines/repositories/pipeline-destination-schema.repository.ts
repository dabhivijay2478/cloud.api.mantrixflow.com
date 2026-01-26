/**
 * Pipeline Destination Schema Repository
 * Data access layer for pipeline_destination_schemas table
 */

import { Inject, Injectable, NotFoundException } from '@nestjs/common';
import { and, eq, isNull } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
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
