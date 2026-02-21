# ETL Jobs Queue (pgmq + pg_cron)

Async ETL job dispatch using Supabase pgmq (NO Redis, NO BullMQ).

## Environment Variables

Add to `apps/api/.env`:

| Variable | Description |
|----------|-------------|
| `INTERNAL_TOKEN` | Secret for `/internal/*` endpoints. Must match pg_cron config if using pg_net. |
| `INTERNAL_API_URL` | Full URL of this NestJS API (e.g. `https://api.example.com`). Used for callback URL. |
| `ETL_PYTHON_SERVICE_URL` | ETL FastAPI base URL (e.g. `http://localhost:8001`) |
| `ETL_PYTHON_SERVICE_TOKEN` | Bearer token for ETL auth (must match ETL `ETL_AUTH_TOKEN`) |
| `USE_ETL_JOBS_QUEUE` | Set to `false` to use legacy sync flow. Default: `true` |

## Flow

1. User triggers run → NestJS `enqueueJob` (INSERT etl_jobs + pgmq.send in one transaction)
2. NestJS scheduler (every minute) or pg_cron calls `POST /internal/process-etl-jobs`
3. NestJS reads pgmq, marks job running, POSTs to ETL with callback_url
4. ETL returns 202, runs meltano with `--job-id` in background
5. ETL POSTs to callback_url when done
6. NestJS updates etl_jobs, deletes pgmq message
7. Supabase Realtime pushes status to Next.js

## Migration

Run `bun run db:migrate:etl` to apply ETL migrations (0022, 0023, 0024):

- `0022_add_etl_jobs_pgmq.sql` — etl_jobs table, pgmq queues
- `0023_etl_jobs_backend_read_policy.sql` — RLS SELECT for backend
- `0024_etl_jobs_backend_insert_update.sql` — RLS INSERT/UPDATE for backend
