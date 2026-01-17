# PgBoss Migration Guide

## Overview

This guide covers the migration from BullMQ (Redis-based) to PgBoss (PostgreSQL-based) for job queuing and scheduling.

## Why PgBoss?

| Feature          | BullMQ (Redis)            | PgBoss (PostgreSQL)      |
| ---------------- | ------------------------- | ------------------------ |
| Infrastructure   | Requires Redis            | Uses existing PostgreSQL |
| ACID Compliance  | No                        | Yes                      |
| Transactions     | Separate from DB          | Same DB transaction      |
| Exactly-once     | Requires careful setup    | Built-in                 |
| Distributed Cron | Requires additional setup | Built-in                 |
| Persistence      | Redis persistence         | PostgreSQL durability    |

## Quick Start

### 1. Installation

```bash
bun add pg-boss @nestjs/schedule
bun remove @nestjs/bullmq bullmq ioredis
```

### 2. Configuration Changes

**Before (BullMQ):**

```typescript
import { BullModule } from '@nestjs/bullmq';

BullModule.forRootAsync({
  inject: [ConfigService],
  useFactory: (configService: ConfigService) => ({
    connection: {
      host: configService.get('REDIS_HOST'),
      port: configService.get('REDIS_PORT'),
    },
  }),
});
```

**After (PgBoss):**

```typescript
import { PgBossModule } from './modules/queue';
import { ScheduleModule } from '@nestjs/schedule';

// NestJS native scheduler for simple cron jobs
(ScheduleModule.forRoot(),
  // PgBoss for distributed job queue
  PgBossModule.forRootAsync({
    inject: [ConfigService],
    useFactory: (configService: ConfigService) => ({
      connectionString: configService.get('DATABASE_URL'),
      schema: 'pgboss',
      retryLimit: 3,
      retryBackoff: true,
    }),
  }));
```

## Migration Patterns

### Pattern 1: Simple Job Queue

**Before (BullMQ):**

```typescript
// Producer
@Injectable()
export class MyService {
  constructor(@InjectQueue('my-queue') private queue: Queue) {}

  async addJob(data: MyData) {
    await this.queue.add('process', data);
  }
}

// Consumer
@Processor('my-queue')
export class MyProcessor {
  @Process('process')
  async handleJob(job: Job<MyData>) {
    // Process job
  }
}
```

**After (PgBoss):**

```typescript
// Producer
@Injectable()
export class MyService {
  constructor(private pgBossService: PgBossService) {}

  async addJob(data: MyData) {
    await this.pgBossService.send('my-queue', data);
  }
}

// Consumer
@Injectable()
export class MyProcessor implements OnModuleInit {
  constructor(private pgBossService: PgBossService) {}

  async onModuleInit() {
    await this.pgBossService.work('my-queue', async (job) => {
      // Process job
      return { success: true };
    });
  }
}
```

### Pattern 2: Delayed Jobs

**Before (BullMQ):**

```typescript
await this.queue.add('email', data, { delay: 60000 }); // 60 seconds
```

**After (PgBoss):**

```typescript
await this.pgBossService.sendDelayed('email', data, 60); // 60 seconds
```

### Pattern 3: Scheduled/Recurring Jobs

**Before (BullMQ):**

```typescript
await this.queue.add('report', data, {
  repeat: { cron: '0 0 * * *' }, // Daily
});
```

**After (PgBoss):**

```typescript
await this.pgBossService.schedule('report', {
  cron: '0 0 * * *',
  timezone: 'UTC',
  data: { type: 'daily' },
});
```

### Pattern 4: Priority Jobs

**Before (BullMQ):**

```typescript
await this.queue.add('urgent', data, { priority: 1 });
```

**After (PgBoss):**

```typescript
await this.pgBossService.sendPriority('urgent', data, 'CRITICAL');
```

### Pattern 5: Singleton/Unique Jobs

**Before (BullMQ):**

```typescript
await this.queue.add('sync', data, {
  jobId: 'unique-key',
  removeOnComplete: true,
});
```

**After (PgBoss):**

```typescript
await this.pgBossService.sendSingleton('sync', data, 'unique-key');
```

## When to Use NestJS @Cron vs PgBoss

### Use NestJS @Cron for:

- Simple, fast in-memory tasks
- Tasks that should run on EVERY instance
- Cache warming/cleanup
- Metrics collection
- Health checks

```typescript
@Injectable()
export class TaskService {
  @Cron(CronExpression.EVERY_MINUTE)
  async cleanCache() {
    // Runs on every instance
  }
}
```

### Use PgBoss for:

- Distributed tasks (only one instance runs)
- Long-running jobs
- Jobs that need persistence
- Jobs with retry logic
- Dynamically created schedules

```typescript
@Injectable()
export class TaskService {
  async setupCron() {
    await this.pgBossService.schedule('cleanup', {
      cron: '0 0 * * *',
      timezone: 'UTC',
    });
  }
}
```

## Database Schema

PgBoss creates its own schema (`pgboss` by default):

```sql
-- Main tables created automatically
pgboss.job          -- Active jobs
pgboss.schedule     -- Cron schedules
pgboss.archive      -- Completed/failed jobs
pgboss.subscription -- Workers
```

## Environment Variables

**Remove:**

```env
REDIS_URL=...
REDIS_HOST=...
REDIS_PORT=...
REDIS_PASSWORD=...
```

**Keep:**

```env
DATABASE_URL=postgresql://user:pass@host:5432/db
```

## Monitoring

### Check Queue Status

```typescript
const size = await this.pgBossService.getQueueSize('my-queue');
const schedules = await this.pgBossService.getSchedules();
```

### View Jobs in PostgreSQL

```sql
-- Pending jobs
SELECT * FROM pgboss.job WHERE state = 'created';

-- Failed jobs
SELECT * FROM pgboss.job WHERE state = 'failed';

-- Scheduled crons
SELECT * FROM pgboss.schedule;

-- Completed jobs (archive)
SELECT * FROM pgboss.archive ORDER BY completedon DESC LIMIT 100;
```

## Best Practices

1. **Use singletons for idempotent operations**

   ```typescript
   await pgBossService.sendSingleton('sync', data, 'entity-123');
   ```

2. **Set appropriate timeouts**

   ```typescript
   await pgBossService.send('long-job', data, {
     expireInHours: 24,
     retryLimit: 3,
   });
   ```

3. **Use separate queues for different priorities**
   - `critical:emails` - High priority
   - `batch:exports` - Low priority

4. **Monitor dead letter queue**

   ```sql
   SELECT * FROM pgboss.job
   WHERE state = 'failed'
   AND retrycount >= retrylimit;
   ```

5. **Clean up regularly**
   - PgBoss handles this automatically via `archiveCompletedAfterSeconds`
   - Adjust `deleteAfterDays` based on audit requirements
