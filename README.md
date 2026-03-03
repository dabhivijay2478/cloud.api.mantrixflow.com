# API (NestJS)

Backend API for MANTrixFlow: data sources, pipelines, WebSockets, Supabase.

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

## Deployment

See [DEPLOYMENT.md](DEPLOYMENT.md). NestJS requires a platform that supports WebSockets (Railway, Render, Fly.io, etc.).
