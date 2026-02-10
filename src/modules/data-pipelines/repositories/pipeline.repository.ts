/**
 * Pipeline Repository
 * Data access layer for pipelines and pipeline runs using Drizzle ORM
 */

import { Inject, Injectable, Logger, NotFoundException } from '@nestjs/common';
import {
  and,
  count as drizzleCount,
  desc,
  eq,
  inArray,
  isNotNull,
  isNull,
  lte,
  ne,
  or,
  sql,
} from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import type { PaginatedResult } from '../../../common/dto/pagination-query.dto';
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
    try {
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
    } catch (error: any) {
      // Extract the actual database error from postgres-js/Drizzle
      let actualError: string = error?.message || String(error);
      let postgresError: any = null;

      // Try to extract the underlying PostgreSQL error
      if (error?.cause) {
        postgresError = error.cause;
        actualError = postgresError?.message || postgresError?.detail || actualError;
      } else if (error?.originalError) {
        postgresError = error.originalError;
        actualError = postgresError?.message || postgresError?.detail || actualError;
      }

      // Log the full error structure for debugging
      this.logger.error(
        `[findByOrganization] Database error details:`,
        JSON.stringify(
          {
            message: error?.message,
            cause: error?.cause,
            code: postgresError?.code,
            detail: postgresError?.detail,
            hint: postgresError?.hint,
            table: postgresError?.table,
            column: postgresError?.column,
          },
          null,
          2,
        ),
      );

      // Create enhanced error with actual database error
      const enhancedError = new Error(
        `Failed to query pipelines for organization ${organizationId}: ${actualError}${postgresError?.code ? ` [PostgreSQL Error Code: ${postgresError.code}]` : ''}`,
      );

      if (error instanceof Error && error.stack) {
        enhancedError.stack = error.stack;
      }

      throw enhancedError;
    }
  }

  /**
   * Find pipelines by organization with pagination
   */
  async findByOrganizationPaginated(
    organizationId: string,
    limit: number = 20,
    offset: number = 0,
  ): Promise<
    PaginatedResult<
      Pipeline & {
        sourceSchema?: PipelineSourceSchema | null;
        destinationSchema?: PipelineDestinationSchema | null;
      }
    >
  > {
    const conditions = [eq(pipelines.organizationId, organizationId), isNull(pipelines.deletedAt)];

    const [countResult, rows] = await Promise.all([
      this.db
        .select({ count: drizzleCount() })
        .from(pipelines)
        .where(and(...conditions)),
      this.db
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
        .where(and(...conditions))
        .orderBy(desc(pipelines.createdAt))
        .limit(limit)
        .offset(offset),
    ]);

    return {
      data: rows.map((row) => ({
        ...row.pipeline,
        sourceSchema: row.sourceSchema,
        destinationSchema: row.destinationSchema,
      })),
      total: Number(countResult[0]?.count || 0),
    };
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
    const [run] = await this.db.select().from(pipelineRuns).where(eq(pipelineRuns.id, id)).limit(1);

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
          // Only get pipelines that are in IDLE or LISTING mode and ready for sync
          sql`${pipelines.status} IN ('idle', 'listing')`,
          sql`${pipelines.syncFrequency} != 'manual'`,
          sql`${pipelines.nextSyncAt} <= ${now}`,
        ),
      )
      .orderBy(pipelines.nextSyncAt);
  }

  /**
   * Find pipelines that are due to run based on their schedule
   * Returns pipelines where:
   * - scheduleType is NOT 'none'
   * - nextScheduledRunAt is in the past
   * - status is 'idle' (not already running)
   * - not soft deleted
   */
  async findDuePipelines(now: Date = new Date()): Promise<Pipeline[]> {
    // Convert date to ISO string for proper PostgreSQL compatibility
    const nowIso = now.toISOString();

    this.logger.debug(`[findDuePipelines] Checking for pipelines due before: ${nowIso}`);

    try {
      const results = await this.db
        .select()
        .from(pipelines)
        .where(
          and(
            isNull(pipelines.deletedAt),
            // Is due to run - nextScheduledRunAt is set and in the past
            isNotNull(pipelines.nextScheduledRunAt),
            lte(pipelines.nextScheduledRunAt, now),
            // Either has explicit schedule OR is incremental/CDC mode (syncMode = 'incremental')
            or(
              // Explicit schedule configured
              and(isNotNull(pipelines.scheduleType), ne(pipelines.scheduleType, 'none')),
              // OR incremental/CDC mode with nextScheduledRunAt set (auto 2-min polling)
              eq(pipelines.syncMode, 'incremental'),
            ),
            // Not currently running - include idle, listing (incremental waiting), completed, and failed
            // ROOT FIX: Also allow 'running' status if it's been running for more than 1 hour (stuck pipeline recovery)
            or(
              inArray(pipelines.status, ['idle', 'listing', 'completed', 'failed']),
              // Allow stuck 'running' pipelines that haven't updated in 1 hour
              and(
                eq(pipelines.status, 'running'),
                sql`${pipelines.updatedAt} < NOW() - INTERVAL '1 hour'`,
              ),
            ),
          ),
        )
        .orderBy(pipelines.nextScheduledRunAt);

      // Log debug info about why pipelines might not be found
      if (results.length === 0) {
        // Check if there are any scheduled pipelines at all
        const allScheduled = await this.db
          .select({
            id: pipelines.id,
            name: pipelines.name,
            scheduleType: pipelines.scheduleType,
            nextScheduledRunAt: pipelines.nextScheduledRunAt,
            status: pipelines.status,
          })
          .from(pipelines)
          .where(
            and(
              isNull(pipelines.deletedAt),
              isNotNull(pipelines.scheduleType),
              ne(pipelines.scheduleType, 'none'),
            ),
          );

        if (allScheduled.length > 0) {
          for (const p of allScheduled) {
            const nextRunAt = p.nextScheduledRunAt
              ? new Date(p.nextScheduledRunAt).toISOString()
              : 'NOT SET';
            const isDue = p.nextScheduledRunAt && new Date(p.nextScheduledRunAt) <= now;
            const statusOk = ['idle', 'listing', 'completed', 'failed'].includes(p.status || '');
            this.logger.debug(
              `[findDuePipelines] Pipeline "${p.name}" (${p.id}): ` +
                `scheduleType=${p.scheduleType}, nextRunAt=${nextRunAt}, ` +
                `status=${p.status}, isDue=${isDue}, statusOk=${statusOk}`,
            );
          }
        } else {
          this.logger.debug(`[findDuePipelines] No scheduled pipelines found in the database`);
        }
      }

      return results;
    } catch (error: any) {
      // Extract the actual database error from postgres-js/Drizzle
      // postgres-js errors have a 'cause' property with the actual PostgreSQL error
      let actualError: string = error?.message || String(error);
      let postgresError: any = null;

      // Try to extract the underlying PostgreSQL error
      if (error?.cause) {
        postgresError = error.cause;
        actualError = postgresError?.message || postgresError?.detail || actualError;
      } else if (error?.originalError) {
        postgresError = error.originalError;
        actualError = postgresError?.message || postgresError?.detail || actualError;
      }

      // Log the full error structure for debugging
      this.logger.error(
        `[findDuePipelines] Database error details:`,
        JSON.stringify(
          {
            message: error?.message,
            cause: error?.cause,
            code: postgresError?.code,
            detail: postgresError?.detail,
            hint: postgresError?.hint,
            position: postgresError?.position,
            internalPosition: postgresError?.internalPosition,
            internalQuery: postgresError?.internalQuery,
            where: postgresError?.where,
            schema: postgresError?.schema,
            table: postgresError?.table,
            column: postgresError?.column,
            dataType: postgresError?.dataType,
            constraint: postgresError?.constraint,
            file: postgresError?.file,
            line: postgresError?.line,
            routine: postgresError?.routine,
          },
          null,
          2,
        ),
      );

      // Create enhanced error with actual database error
      const enhancedError = new Error(
        `Failed to query due pipelines (checking for pipelines due before ${nowIso}): ${actualError}${postgresError?.code ? ` [PostgreSQL Error Code: ${postgresError.code}]` : ''}`,
      );

      if (error instanceof Error && error.stack) {
        enhancedError.stack = error.stack;
      }

      throw enhancedError;
    }
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

  // ============================================================================
  // POLLING/CDC SUPPORT METHODS (BullMQ Integration)
  // ============================================================================

  /**
   * Find all pipelines that are eligible for polling (CDC)
   * Returns pipelines with:
   * - status = 'listing' (waiting for incremental sync)
   * - syncMode = 'incremental'
   * - Has an incremental column configured
   * - Not soft deleted
   */
  /**
   * Find pipelines stuck in 'running' status for more than 1 hour
   * ROOT FIX: Automatically recover stuck pipelines
   */
  async findStuckPipelines(): Promise<Pipeline[]> {
    try {
      const result = await this.db
        .select()
        .from(pipelines)
        .where(
          and(
            isNull(pipelines.deletedAt),
            eq(pipelines.status, 'running'),
            // Stuck if updated more than 1 hour ago
            sql`${pipelines.updatedAt} < NOW() - INTERVAL '1 hour'`,
          ),
        );

      this.logger.debug(`[findStuckPipelines] Found ${result.length} stuck pipeline(s)`);
      return result;
    } catch (error) {
      this.logger.error(`[findStuckPipelines] Error: ${error}`);
      return [];
    }
  }

  /**
   * Find active pipelines for CDC polling
   * ROOT FIX: User requirement - only poll pipelines in 'listing' status
   * After first full sync, pipeline transitions to 'listing' → then always incremental
   * No need to check incrementalColumn - status-based detection only
   */
  async findActivePipelinesForPolling(): Promise<Pipeline[]> {
    try {
      const result = await this.db
        .select()
        .from(pipelines)
        .where(
          and(
            isNull(pipelines.deletedAt),
            // USER REQUIREMENT: Only poll pipelines in 'listing' status (CDC mode)
            eq(pipelines.status, 'listing'),
          ),
        );

      this.logger.debug(
        `[findActivePipelinesForPolling] Found ${result.length} active pipeline(s) for polling`,
      );
      return result;
    } catch (error) {
      this.logger.error(`[findActivePipelinesForPolling] Error: ${error}`);
      return [];
    }
  }

  /**
   * Find pipeline by ID with source and destination schemas for CDC operations
   * Used for delta checks and incremental sync operations
   * NOTE: Different from findByIdWithSchemas which returns structured object
   */
  async findByIdForCDC(id: string): Promise<
    | (Pipeline & {
        sourceSchema: PipelineSourceSchema | null;
        destinationSchema: PipelineDestinationSchema | null;
      })
    | null
  > {
    try {
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
        .where(and(eq(pipelines.id, id), isNull(pipelines.deletedAt)))
        .limit(1);

      if (result.length === 0) {
        return null;
      }

      const row = result[0];
      return {
        ...row.pipeline,
        sourceSchema: row.sourceSchema,
        destinationSchema: row.destinationSchema,
      };
    } catch (error) {
      this.logger.error(`[findByIdForCDC] Error finding pipeline ${id}: ${error}`);
      return null;
    }
  }

  /**
   * Persist checkpoint atomically (single transaction).
   */
  async saveCheckpointStateAtomic(
    pipelineId: string,
    checkpoint: Record<string, unknown>,
  ): Promise<void> {
    try {
      await this.db.transaction(async (tx) => {
        const checkpointValue = checkpoint as Record<string, unknown>;
        const lastSyncValue = checkpointValue.lastSyncValue;
        const lastSyncAtRaw = checkpointValue.lastSyncAt;
        const lastSyncAt =
          typeof lastSyncAtRaw === 'string' || lastSyncAtRaw instanceof Date
            ? new Date(lastSyncAtRaw)
            : new Date();

        await tx
          .update(pipelines)
          .set({
            checkpoint: checkpointValue as any,
            lastSyncValue:
              lastSyncValue === undefined || lastSyncValue === null ? null : String(lastSyncValue),
            lastSyncAt,
            updatedAt: new Date(),
          })
          .where(eq(pipelines.id, pipelineId));
      });
    } catch (error) {
      this.logger.error(`[saveCheckpointStateAtomic] Error updating checkpoint: ${error}`);
      throw error;
    }
  }

  /**
   * Backward-compatible alias.
   */
  async updateCheckpointAtomic(
    pipelineId: string,
    checkpoint: {
      watermarkField: string;
      lastSyncValue: string | number;
      lastSyncAt: string;
      rowsProcessed: number;
      [key: string]: unknown;
    },
  ): Promise<void> {
    await this.saveCheckpointStateAtomic(pipelineId, checkpoint);
  }

  /**
   * Update pipeline status atomically with validation
   * ROOT FIX: Ensures status transitions are valid and atomic
   */
  async updateStatusAtomic(
    pipelineId: string,
    newStatus:
      | 'idle'
      | 'initializing'
      | 'running'
      | 'listing'
      | 'listening'
      | 'paused'
      | 'failed'
      | 'completed',
    allowedFromStatuses: (
      | 'idle'
      | 'initializing'
      | 'running'
      | 'listing'
      | 'listening'
      | 'paused'
      | 'failed'
      | 'completed'
    )[],
  ): Promise<boolean> {
    try {
      await this.db
        .update(pipelines)
        .set({
          status: newStatus,
          updatedAt: new Date(),
        })
        .where(and(eq(pipelines.id, pipelineId), inArray(pipelines.status, allowedFromStatuses)));

      // Drizzle doesn't directly return rowCount, check if update was successful
      // by verifying the new status
      const updated = await this.findById(pipelineId);
      const success = updated?.status === newStatus;

      if (success) {
        this.logger.debug(
          `[updateStatusAtomic] Pipeline ${pipelineId} status changed to ${newStatus}`,
        );
      } else {
        this.logger.warn(
          `[updateStatusAtomic] Pipeline ${pipelineId} status update failed - not in allowed state`,
        );
      }

      return success;
    } catch (error) {
      this.logger.error(`[updateStatusAtomic] Error updating status: ${error}`);
      throw error;
    }
  }
}
