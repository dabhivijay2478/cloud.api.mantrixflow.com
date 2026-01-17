/**
 * Pipeline Repository
 * Data access layer for pipelines and pipeline runs using Drizzle ORM
 */

import { Inject, Injectable, Logger, NotFoundException } from '@nestjs/common';
import { and, desc, eq, isNull, sql } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import type {
  NewPipeline,
  NewPipelineRun,
  Pipeline,
  PipelineDestinationSchema,
  PipelineRun,
  PipelineSourceSchema,
} from '../../../database/schemas';
import {
  pipelineDestinationSchemas,
  pipelineRuns,
  pipelines,
  pipelineSourceSchemas,
} from '../../../database/schemas';

@Injectable()
export class PipelineRepository {
  private readonly logger = new Logger(PipelineRepository.name);

  constructor(
    @Inject('DRIZZLE_DB')
    private readonly db: NodePgDatabase<any>,
  ) {}

  // ============================================================================
  // PIPELINE CRUD OPERATIONS
  // ============================================================================

  /**
   * Create a new pipeline
   */
  async create(data: NewPipeline): Promise<Pipeline> {
    const [pipeline] = await this.db.insert(pipelines).values(data).returning();
    this.logger.log(`Pipeline created: ${pipeline.id}`);
    return pipeline;
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
   * Find pipeline by name and organization
   */
  async findByNameAndOrganizationId(
    name: string,
    organizationId: string,
  ): Promise<Pipeline | null> {
    const [pipeline] = await this.db
      .select()
      .from(pipelines)
      .where(
        and(
          eq(pipelines.name, name),
          eq(pipelines.organizationId, organizationId),
          isNull(pipelines.deletedAt),
        ),
      )
      .limit(1);

    return pipeline || null;
  }

  /**
   * Find pipelines by organization with source and destination schemas
   */
  async findByOrganization(organizationId: string): Promise<
    (Pipeline & {
      sourceSchema?: PipelineSourceSchema | null;
      destinationSchema?: PipelineDestinationSchema | null;
    })[]
  > {
    const result = await this.db
      .select({
        pipeline: pipelines,
        sourceSchema: pipelineSourceSchemas,
        destinationSchema: pipelineDestinationSchemas,
      })
      .from(pipelines)
      .leftJoin(pipelineSourceSchemas, eq(pipelines.sourceSchemaId, pipelineSourceSchemas.id))
      .leftJoin(
        pipelineDestinationSchemas,
        eq(pipelines.destinationSchemaId, pipelineDestinationSchemas.id),
      )
      .where(and(eq(pipelines.organizationId, organizationId), isNull(pipelines.deletedAt)))
      .orderBy(desc(pipelines.createdAt));

    // Flatten the result to include schemas as properties on the pipeline
    return result.map((row) => ({
      ...row.pipeline,
      sourceSchema: row.sourceSchema,
      destinationSchema: row.destinationSchema,
    }));
  }

  /**
   * Find pipeline with source and destination schemas
   */
  async findByIdWithSchemas(
    id: string,
    organizationId?: string,
  ): Promise<{
    pipeline: Pipeline;
    sourceSchema: PipelineSourceSchema;
    destinationSchema: PipelineDestinationSchema;
  } | null> {
    const conditions = [eq(pipelines.id, id), isNull(pipelines.deletedAt)];

    if (organizationId) {
      conditions.push(eq(pipelines.organizationId, organizationId));
    }

    const result = await this.db
      .select({
        pipeline: pipelines,
        sourceSchema: pipelineSourceSchemas,
        destinationSchema: pipelineDestinationSchemas,
      })
      .from(pipelines)
      .innerJoin(pipelineSourceSchemas, eq(pipelines.sourceSchemaId, pipelineSourceSchemas.id))
      .innerJoin(
        pipelineDestinationSchemas,
        eq(pipelines.destinationSchemaId, pipelineDestinationSchemas.id),
      )
      .where(and(...conditions))
      .limit(1);

    if (result.length === 0) {
      return null;
    }

    return result[0];
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
    await this.db
      .update(pipelines)
      .set({ deletedAt: new Date(), updatedAt: new Date() })
      .where(eq(pipelines.id, id));

    this.logger.log(`Pipeline soft deleted: ${id}`);
  }

  // ============================================================================
  // PIPELINE RUN OPERATIONS
  // ============================================================================

  /**
   * Create pipeline run
   */
  async createRun(data: NewPipelineRun): Promise<PipelineRun> {
    const [run] = await this.db.insert(pipelineRuns).values(data).returning();
    this.logger.log(`Pipeline run created: ${run.id}`);
    return run;
  }

  /**
   * Find run by ID
   */
  async findRunById(id: string): Promise<PipelineRun | null> {
    const [run] = await this.db
      .select()
      .from(pipelineRuns)
      .where(eq(pipelineRuns.id, id))
      .limit(1);

    return run || null;
  }

  /**
   * Find runs by pipeline
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
   * Find active runs for pipeline (running or pending)
   */
  async findActiveRuns(pipelineId: string): Promise<PipelineRun[]> {
    return await this.db
      .select()
      .from(pipelineRuns)
      .where(
        and(
          eq(pipelineRuns.pipelineId, pipelineId),
          sql`${pipelineRuns.status} IN ('pending', 'running')`,
        ),
      )
      .orderBy(desc(pipelineRuns.createdAt));
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

  // ============================================================================
  // STATISTICS
  // ============================================================================

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
    // Get pipeline for basic stats
    const pipeline = await this.findById(pipelineId);
    if (!pipeline) {
      throw new NotFoundException(`Pipeline ${pipelineId} not found`);
    }

    // Get last successful run
    const [lastSuccessful] = await this.db
      .select()
      .from(pipelineRuns)
      .where(and(eq(pipelineRuns.pipelineId, pipelineId), eq(pipelineRuns.status, 'success')))
      .orderBy(desc(pipelineRuns.completedAt))
      .limit(1);

    // Calculate average duration from successful runs
    const avgResult = await this.db
      .select({
        avgDuration: sql<number>`AVG(${pipelineRuns.durationSeconds})`,
      })
      .from(pipelineRuns)
      .where(
        and(
          eq(pipelineRuns.pipelineId, pipelineId),
          eq(pipelineRuns.status, 'success'),
          sql`${pipelineRuns.durationSeconds} IS NOT NULL`,
        ),
      );

    const averageDuration = avgResult[0]?.avgDuration || 0;

    return {
      totalRowsProcessed: pipeline.totalRowsProcessed || 0,
      totalRunsSuccessful: pipeline.totalRunsSuccessful || 0,
      totalRunsFailed: pipeline.totalRunsFailed || 0,
      lastSuccessfulRun: lastSuccessful?.completedAt || undefined,
      averageDuration: Math.round(averageDuration),
    };
  }

  /**
   * Get run counts by status for a pipeline
   */
  async getRunCountsByStatus(pipelineId: string): Promise<Record<string, number>> {
    const results = await this.db
      .select({
        status: pipelineRuns.status,
        count: sql<number>`COUNT(*)`,
      })
      .from(pipelineRuns)
      .where(eq(pipelineRuns.pipelineId, pipelineId))
      .groupBy(pipelineRuns.status);

    const counts: Record<string, number> = {};
    for (const row of results) {
      if (row.status) {
        counts[row.status] = Number(row.count);
      }
    }

    return counts;
  }

  /**
   * Get runs by organization
   */
  async findRunsByOrganization(
    organizationId: string,
    limit: number = 50,
    offset: number = 0,
  ): Promise<PipelineRun[]> {
    return await this.db
      .select()
      .from(pipelineRuns)
      .where(eq(pipelineRuns.organizationId, organizationId))
      .orderBy(desc(pipelineRuns.createdAt))
      .limit(limit)
      .offset(offset);
  }

  /**
   * Get pipelines that need to run (scheduled)
   */
  async findPipelinesDueForSync(now: Date = new Date()): Promise<Pipeline[]> {
    return await this.db
      .select()
      .from(pipelines)
      .where(
        and(
          isNull(pipelines.deletedAt),
          eq(pipelines.status, 'active'),
          sql`${pipelines.syncFrequency} != 'manual'`,
          sql`${pipelines.nextSyncAt} <= ${now}`,
        ),
      )
      .orderBy(pipelines.nextSyncAt);
  }

  /**
   * Count pipelines by organization
   */
  async countByOrganization(organizationId: string): Promise<number> {
    const result = await this.db
      .select({ count: sql<number>`COUNT(*)` })
      .from(pipelines)
      .where(and(eq(pipelines.organizationId, organizationId), isNull(pipelines.deletedAt)));

    return Number(result[0]?.count || 0);
  }

  /**
   * Count runs by pipeline
   */
  async countRunsByPipeline(pipelineId: string): Promise<number> {
    const result = await this.db
      .select({ count: sql<number>`COUNT(*)` })
      .from(pipelineRuns)
      .where(eq(pipelineRuns.pipelineId, pipelineId));

    return Number(result[0]?.count || 0);
  }
}
