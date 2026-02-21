# ETL Jobs Queue (pgmq + pg_cron)

Async ETL job dispatch using Supabase pgmq (NO Redis, NO BullMQ).

## Environment Variables

Add to `apps/api/.env`:

| Variable | Description |
|----------|-------------|
| `INTERNAL_TOKEN` | Secret for `/internal/*` endpoints. Must match pg_cron config if using pg_net. |
| `INTERNAL_API_URL` | Full URL of this NestJS API (e.g. `http://host.docker.internal:5000`). Used for callback URL. Must include `/api` prefix in path for NestJS routes. |
| `ETL_PYTHON_SERVICE_URL` | ETL FastAPI base URL (e.g. `http://localhost:8001`) |
| `ETL_PYTHON_SERVICE_TOKEN` | Bearer token for ETL auth (must match ETL `ETL_AUTH_TOKEN`) |
| `USE_ETL_JOBS_QUEUE` | Set to `false` to use legacy sync flow. Default: `true` |

### Callback URL: ETL in Docker → API on host

When ETL runs in Docker and the API runs on the host, the ETL container cannot reach `localhost` (that resolves to the container itself). Use a host-reachable URL:

| Platform | `INTERNAL_API_URL` |
|----------|--------------------|
| **Mac / Windows** | `http://host.docker.internal:5000` |
| **Linux** | `http://172.17.0.1:5000` (Docker bridge gateway) or add `extra_hosts: host.docker.internal: host-gateway` to ETL compose and use `http://host.docker.internal:5000` |

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
