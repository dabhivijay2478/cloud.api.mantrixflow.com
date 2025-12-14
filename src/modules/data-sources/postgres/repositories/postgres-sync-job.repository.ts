/**
 * PostgreSQL Sync Job Repository
 * Handles database operations for postgres_sync_jobs table
 */

import { Injectable, Inject } from '@nestjs/common';
import { eq, and, desc, asc, lte } from 'drizzle-orm';
import {
  postgresSyncJobs,
  PostgresSyncJob,
  NewPostgresSyncJob,
} from '../../../../database/drizzle/schema/postgres-connectors.schema';
import type { DrizzleDatabase } from '../../../../database/drizzle/database';

@Injectable()
export class PostgresSyncJobRepository {
  constructor(@Inject('DRIZZLE_DB') private readonly db: DrizzleDatabase) {}

  /**
   * Create sync job
   */
  async create(data: NewPostgresSyncJob): Promise<PostgresSyncJob> {
    const [job] = await this.db
      .insert(postgresSyncJobs)
      .values(data)
      .returning();
    return job;
  }

  /**
   * Find sync job by ID
   */
  async findById(
    id: string,
    connectionId?: string,
  ): Promise<PostgresSyncJob | null> {
    const conditions = connectionId
      ? and(
          eq(postgresSyncJobs.id, id),
          eq(postgresSyncJobs.connectionId, connectionId),
        )
      : eq(postgresSyncJobs.id, id);

    const [job] = await this.db
      .select()
      .from(postgresSyncJobs)
      .where(conditions)
      .limit(1);

    return job || null;
  }

  /**
   * Find all sync jobs for connection
   */
  async findByConnectionId(connectionId: string): Promise<PostgresSyncJob[]> {
    return await this.db
      .select()
      .from(postgresSyncJobs)
      .where(eq(postgresSyncJobs.connectionId, connectionId))
      .orderBy(desc(postgresSyncJobs.createdAt));
  }

  /**
   * Update sync job
   */
  async update(
    id: string,
    data: Partial<NewPostgresSyncJob>,
  ): Promise<PostgresSyncJob> {
    const updateData = {
      ...data,
      updatedAt: new Date(),
    };

    const [job] = await this.db
      .update(postgresSyncJobs)
      .set(updateData)
      .where(eq(postgresSyncJobs.id, id))
      .returning();

    return job;
  }

  /**
   * Delete sync job
   */
  async delete(id: string): Promise<void> {
    await this.db.delete(postgresSyncJobs).where(eq(postgresSyncJobs.id, id));
  }

  /**
   * Find pending sync jobs
   */
  async findPendingJobs(): Promise<PostgresSyncJob[]> {
    return await this.db
      .select()
      .from(postgresSyncJobs)
      .where(eq(postgresSyncJobs.status, 'pending'))
      .orderBy(asc(postgresSyncJobs.createdAt));
  }

  /**
   * Find jobs scheduled for sync
   */
  async findScheduledJobs(now: Date = new Date()): Promise<PostgresSyncJob[]> {
    return await this.db
      .select()
      .from(postgresSyncJobs)
      .where(
        and(
          eq(postgresSyncJobs.status, 'pending'),
          lte(postgresSyncJobs.nextSyncAt, now),
        ),
      );
  }
}
