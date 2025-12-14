/**
 * PostgreSQL Pipeline Repository
 * Data access layer for pipeline entities
 */

import { Injectable, Inject, NotFoundException } from '@nestjs/common';
import { NodePgDatabase } from 'drizzle-orm/node-postgres';
import { eq, and, isNull, desc } from 'drizzle-orm';

// Import types
import type {
    PostgresPipeline,
    NewPostgresPipeline,
    PostgresPipelineRun,
    NewPostgresPipelineRun,
    PipelineSourceSchema,
    PipelineDestinationSchema,
} from '@db/schema';

// Import table definitions (runtime values)
import {
    postgresPipelines,
    postgresPipelineRuns,
    pipelineSourceSchemas,
    pipelineDestinationSchemas,
} from '@db/schema';

/**
 * Pipeline with loaded schemas
 */
export interface PipelineWithSchemas {
    pipeline: PostgresPipeline;
    sourceSchema: PipelineSourceSchema;
    destinationSchema: PipelineDestinationSchema;
}

@Injectable()
export class PostgresPipelineRepository {
    constructor(
        @Inject('DRIZZLE_DB')
        private readonly db: NodePgDatabase<any>,
    ) { }

    /**
     * Create new pipeline
     */
    async create(pipeline: NewPostgresPipeline): Promise<PostgresPipeline> {
        const [created] = await this.db
            .insert(postgresPipelines)
            .values(pipeline)
            .returning();
        return created;
    }

    /**
     * Find pipeline by ID
     */
    async findById(
        id: string,
        orgId?: string,
    ): Promise<PostgresPipeline | null> {
        const conditions = [eq(postgresPipelines.id, id), isNull(postgresPipelines.deletedAt)];

        if (orgId) {
            conditions.push(eq(postgresPipelines.orgId, orgId));
        }

        const [pipeline] = await this.db
            .select()
            .from(postgresPipelines)
            .where(and(...conditions))
            .limit(1);

        return pipeline || null;
    }

    /**
     * Find pipeline by ID with schemas loaded
     */
    async findByIdWithSchemas(
        id: string,
        orgId?: string,
    ): Promise<PipelineWithSchemas | null> {
        const pipeline = await this.findById(id, orgId);
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
            throw new NotFoundException(
                `Source schema ${pipeline.sourceSchemaId} not found`,
            );
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
            throw new NotFoundException(
                `Destination schema ${pipeline.destinationSchemaId} not found`,
            );
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
    async findByOrg(orgId: string): Promise<PostgresPipeline[]> {
        return await this.db
            .select()
            .from(postgresPipelines)
            .where(
                and(
                    eq(postgresPipelines.orgId, orgId),
                    isNull(postgresPipelines.deletedAt),
                ),
            )
            .orderBy(desc(postgresPipelines.createdAt));
    }

    /**
     * Update pipeline
     */
    async update(
        id: string,
        updates: Partial<PostgresPipeline>,
    ): Promise<PostgresPipeline> {
        const [updated] = await this.db
            .update(postgresPipelines)
            .set({ ...updates, updatedAt: new Date() })
            .where(eq(postgresPipelines.id, id))
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
            .update(postgresPipelines)
            .set({ deletedAt: new Date() })
            .where(eq(postgresPipelines.id, id));
    }

    /**
     * Hard delete pipeline
     */
    async hardDelete(id: string): Promise<void> {
        await this.db
            .delete(postgresPipelines)
            .where(eq(postgresPipelines.id, id));
    }

    /**
     * Create pipeline run
     */
    async createRun(run: NewPostgresPipelineRun): Promise<PostgresPipelineRun> {
        const [created] = await this.db
            .insert(postgresPipelineRuns)
            .values(run)
            .returning();
        return created;
    }

    /**
     * Update pipeline run
     */
    async updateRun(
        id: string,
        updates: Partial<PostgresPipelineRun>,
    ): Promise<PostgresPipelineRun> {
        const [updated] = await this.db
            .update(postgresPipelineRuns)
            .set(updates)
            .where(eq(postgresPipelineRuns.id, id))
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
    ): Promise<PostgresPipelineRun[]> {
        return await this.db
            .select()
            .from(postgresPipelineRuns)
            .where(eq(postgresPipelineRuns.pipelineId, pipelineId))
            .orderBy(desc(postgresPipelineRuns.createdAt))
            .limit(limit)
            .offset(offset);
    }

    /**
     * Find run by ID
     */
    async findRunById(id: string): Promise<PostgresPipelineRun | null> {
        const [run] = await this.db
            .select()
            .from(postgresPipelineRuns)
            .where(eq(postgresPipelineRuns.id, id))
            .limit(1);

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
