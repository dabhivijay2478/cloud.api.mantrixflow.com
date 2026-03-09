# API (NestJS)

Backend API for MantrixFlow: data sources, pipelines, WebSockets, Supabase.

## Setup

```bash
bun install
cp .env.example .env
# Edit .env: DATABASE_URL, SUPABASE_*, ENCRYPTION_MASTER_KEY, ETL_SERVICE_URL, PORT
bun run db:migrate
```

## Environment

Required variables (see `.env.example`):

- `DATABASE_URL` — PostgreSQL connection
- `SUPABASE_URL`, `SUPABASE_ANON_KEY`, `SUPABASE_SERVICE_ROLE_KEY`
- `ENCRYPTION_MASTER_KEY` — AES-256-GCM key (e.g. `openssl rand -hex 32`)
- `ETL_SERVICE_URL` — ETL service URL (local: `http://localhost:8000`)
- `PORT` — API port (e.g. 3001)
- `DATABASE_DIRECT_URL` — (Optional) Direct DB connection. When set, both migrations and the API use it. If using Supabase pooler (port 6543), the API and migrations prefer this or swap to port 5432 so schema and transactions match.

## Start

```bash
# Development
bun run start:dev

# Production
bun run build
bun run start:prod
```

## Swagger

When running: `http://localhost:{PORT}/api/docs`

## Troubleshooting

### "column cdc_prerequisites_status does not exist"

Run migrations against the same database the API uses:

```bash
bun run db:migrate
```

Migrations prefer `DATABASE_DIRECT_URL` (or port 5432) when `DATABASE_URL` uses Supabase pooler (port 6543), since DDL can fail over the pooler.

### "Data source with name X already exists"

This can occur when a data source was created but its connection failed (e.g. before migrations were applied). Remove orphaned data sources:

```bash
# Dry run: list orphaned sources
bun run db:delete-orphaned-sources -- --dry-run

# Delete all orphaned
bun run db:delete-orphaned-sources

# Delete orphaned by name
bun run db:delete-orphaned-sources -- --name "Neon"
```

Then retry adding the data source. The atomic `POST /data-sources/with-connection` endpoint prevents new orphans when connection creation fails.

## Deployment

See [DEPLOYMENT.md](DEPLOYMENT.md). NestJS requires a platform that supports WebSockets (Railway, Render, Fly.io, etc.).
