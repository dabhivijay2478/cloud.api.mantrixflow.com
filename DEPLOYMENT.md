# Deployment Guide

## Architecture Overview

| Component | Deployment | Notes |
|-----------|------------|-------|
| **Frontend** | Vercel | Next.js app |
| **NestJS API** | Must support WebSockets | Not Vercel serverless |
| **ETL (Python)** | fly.io | See `apps/new-etl/` |

## WebSocket / Real-Time Requirements

The NestJS API uses **Socket.io** for real-time pipeline status updates (detail page). **Vercel serverless does not support long-lived WebSocket connections.**

### Recommended deployment options for NestJS API

- **Railway** | **Render** | **Fly.io** | **AWS ECS / EKS** | **Self-hosted Node** — persistent process, WebSockets work
- **Vercel serverless** — API routes work; WebSockets will not. Real-time updates (detail page) will fail.

### If using Vercel for API

If you deploy the NestJS API to Vercel:

1. Real-time updates on the **pipeline detail page** (Socket.io) will not work.
2. The **Supabase Realtime** subscription on the pipelines list page will still work (client-side).
3. Option: deploy a separate WebSocket service (e.g. on Fly.io) that listens to Postgres NOTIFY and forwards to clients. This requires more setup.

### Environment variables for API

- `ALLOWED_ORIGINS` — Comma-separated origins for CORS (e.g. `https://app.example.com`)
- `FRONTEND_URL` — Frontend URL (used for CORS and Socket.io)
- `PGMQ_PARALLEL_WORKERS` — Set to 10 for ~2k runs/day (see `apps/new-etl/README.md`)

## Scale targets (1M pipelines, 2k runs/day, 1000 users)

- Run migration `0027_add_scale_indexes.sql` for database indexes.
- Set `PGMQ_PARALLEL_WORKERS=10` (or 15–20) in API env.
- Ensure ETL machine has enough resources (see `apps/new-etl/README.md`).
- Deploy NestJS API on a platform that supports WebSockets.
