/**
 * PostgreSQL Pipeline Repository
 * Data access layer for pipeline entities
 */

// Import types
import type {
  NewPipeline,
  NewPipelineRun,
  PipelineDestinationSchema,
  PipelineSourceSchema,
  Pipeline,
  PipelineRun,
} from '../../../database/schemas';
// Import table definitions (runtime values)
import {
  pipelineDestinationSchemas,
  pipelineSourceSchemas,
  pipelineRuns,
  pipelines,
} from '../../../database/schemas';
import { Inject, Injectable, NotFoundException } from '@nestjs/common';
import { and, desc, eq, isNull, lte, or } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';

/**
 * Pipeline with loaded schemas
 */
export interface PipelineWithSchemas {
  pipeline: Pipeline;
  sourceSchema: PipelineSourceSchema;
  destinationSchema: PipelineDestinationSchema;
}

@Injectable()
export class PostgresPipelineRepository {
  constructor(
    @Inject('DRIZZLE_DB')
    private readonly db: NodePgDatabase<any>,
  ) {}

  /**
   * Create new pipeline
   */
  async create(pipeline: NewPipeline): Promise<Pipeline> {
    const [created] = await this.db.insert(pipelines).values(pipeline).returning();
    return created;
  }

  /**
   * Find pipeline by name and organizationId (for duplicate prevention)
   */
  async findByNameAndOrganizationId(
    name: string,
    organizationId: string,
  ): Promise<Pipeline | null> {
    const results = await this.db
      .select()
      .from(pipelines)
      .where(and(eq(pipelines.name, name), eq(pipelines.organizationId, organizationId)))
      .limit(1);

    return results[0] || null;
  }

  /**
   * Find pipeline by ID
   */
  async findById(id: string, organizationId?: string): Promise<Pipeline | null> {
    const conditions = [eq(pipelines.id, id), isNull(pipelines.deletedAt)];

    if (organizationId) {
      conditions.push(eq(pipelines.organizationId, organizationId));
    }

    const [pipeline] = await this.db
      .select()
      .from(pipelines)
      .where(and(...conditions))
      .limit(1);

    return pipeline || null;
  }

  /**
   * Find pipeline by ID with schemas loaded
   */
  async findByIdWithSchemas(
    id: string,
    organizationId?: string,
  ): Promise<PipelineWithSchemas | null> {
    const pipeline = await this.findById(id, organizationId);
    if (!pipeline) {
      return null;
    }

    // Load source schema
    const [sourceSchema] = await this.db
      .select()
      .from(pipelineSourceSchemas)
      .where(
        and(
          eq(pipelineSourceSchemas.id, pipeline.sourceSchemaId),
          isNull(pipelineSourceSchemas.deletedAt),
        ),
      )
      .limit(1);

    if (!sourceSchema) {
      throw new NotFoundException(`Source schema ${pipeline.sourceSchemaId} not found`);
    }

    // Load destination schema
    const [destinationSchema] = await this.db
      .select()
      .from(pipelineDestinationSchemas)
      .where(
        and(
          eq(pipelineDestinationSchemas.id, pipeline.destinationSchemaId),
          isNull(pipelineDestinationSchemas.deletedAt),
        ),
      )
      .limit(1);

    if (!destinationSchema) {
      throw new NotFoundException(`Destination schema ${pipeline.destinationSchemaId} not found`);
    }

    return {
      pipeline,
      sourceSchema,
      destinationSchema,
    };
  }

  /**
   * Find pipelines by organization
   */
  async findByOrganization(organizationId: string): Promise<Pipeline[]> {
    return await this.db
      .select()
      .from(pipelines)
      .where(and(eq(pipelines.organizationId, organizationId), isNull(pipelines.deletedAt)))
      .orderBy(desc(pipelines.createdAt));
  }

  /**
   * Find active pipelines that are due for sync
   * Only returns pipelines that are due for checking (nextSyncAt is null or in the past)
   * This optimizes resource usage by skipping pipelines that were recently checked
   */
  async findActivePipelinesDueForSync(): Promise<Pipeline[]> {
    const now = new Date();
    return await this.db
      .select()
      .from(pipelines)
      .where(
        and(
          eq(pipelines.status, 'active'),
          isNull(pipelines.deletedAt),
          // Only check pipelines where nextSyncAt is null or in the past
          or(isNull(pipelines.nextSyncAt), lte(pipelines.nextSyncAt, now)),
        ),
      );
  }

  /**
   * Update pipeline
   */
  async update(id: string, updates: Partial<Pipeline>): Promise<Pipeline> {
    const [updated] = await this.db
      .update(pipelines)
      .set({ ...updates, updatedAt: new Date() })
      .where(eq(pipelines.id, id))
      .returning();

    if (!updated) {
      throw new NotFoundException(`Pipeline ${id} not found`);
    }

    return updated;
  }

  /**
   * Soft delete pipeline
   */
  async delete(id: string): Promise<void> {
    await this.db.update(pipelines).set({ deletedAt: new Date() }).where(eq(pipelines.id, id));
  }

  /**
   * Hard delete pipeline
   */
  async hardDelete(id: string): Promise<void> {
    await this.db.delete(pipelines).where(eq(pipelines.id, id));
  }

  /**
   * Create pipeline run
   */
  async createRun(run: NewPipelineRun): Promise<PipelineRun> {
    const [created] = await this.db.insert(pipelineRuns).values(run).returning();
    return created;
  }

  /**
   * Update pipeline run
   */
  async updateRun(id: string, updates: Partial<PipelineRun>): Promise<PipelineRun> {
    const [updated] = await this.db
      .update(pipelineRuns)
      .set({ ...updates, updatedAt: new Date() })
      .where(eq(pipelineRuns.id, id))
      .returning();

    if (!updated) {
      throw new NotFoundException(`Pipeline run ${id} not found`);
    }

    return updated;
  }

  /**
   * Find runs by pipeline ID
   */
  async findRunsByPipeline(
    pipelineId: string,
    limit: number = 20,
    offset: number = 0,
  ): Promise<PipelineRun[]> {
    return await this.db
      .select()
      .from(pipelineRuns)
      .where(eq(pipelineRuns.pipelineId, pipelineId))
      .orderBy(desc(pipelineRuns.createdAt))
      .limit(limit)
      .offset(offset);
  }

  /**
   * Find run by ID
   */
  async findRunById(id: string): Promise<PipelineRun | null> {
    const [run] = await this.db.select().from(pipelineRuns).where(eq(pipelineRuns.id, id)).limit(1);

    return run || null;
  }

  /**
   * Get pipeline statistics
   */
  async getStats(pipelineId: string): Promise<{
    totalRowsProcessed: number;
    totalRunsSuccessful: number;
    totalRunsFailed: number;
    lastSuccessfulRun?: Date;
    averageDuration: number;
  }> {
    const pipeline = await this.findById(pipelineId);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    const runs = await this.findRunsByPipeline(pipelineId, 100);

    const successfulRuns = runs.filter((r) => r.status === 'success');
    const lastSuccessful = successfulRuns[0];

    const avgDuration =
      successfulRuns.length > 0
        ? successfulRuns.reduce((sum, r) => sum + (r.durationSeconds || 0), 0) /
          successfulRuns.length
        : 0;

    return {
      totalRowsProcessed: pipeline.totalRowsProcessed ?? 0,
      totalRunsSuccessful: pipeline.totalRunsSuccessful ?? 0,
      totalRunsFailed: pipeline.totalRunsFailed ?? 0,
      lastSuccessfulRun: lastSuccessful?.completedAt || undefined,
      averageDuration: Math.round(avgDuration),
    };
  }
}
