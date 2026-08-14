# SyncForge Architecture

This document is kept in sync with the implementation. It describes the system
as it exists today and marks where later phases extend it.

## Status

Phase 1 (Foundation) implemented. Later phases (2–10) build on this structure.

## Process model

| Process | Role | Why a separate process |
|---|---|---|
| `cmd/api` | REST API + webhook gateway | Webhook ingestion must stay up while workers are backlogged; API is latency-sensitive |
| `cmd/engine` | Worker host (event processor, sync, retry, reconciliation in Phases 2–6) | Scales independently of the API |
| `cmd/sim-salesforce`, `cmd/sim-hubspot` | Simulated external systems | They represent third-party systems, not SyncForge |

Infrastructure: PostgreSQL 17 (durable state + RLS), Redis 7 (rate limiting,
cache), Redpanda (durable event bus, Phase 2), Prometheus + Grafana (metrics),
Next.js dashboard.

## Key packages

- `internal/connectors` — `Connector` / `Adapter` interfaces, typed error
  classes (`TRANSIENT`, `PERMANENT`, `RATE_LIMITED`, `AUTHENTICATION`,
  `SCHEMA_ERROR`, `CONFLICT`, `NOT_FOUND`), shared HTTP client that classifies
  responses (429 → `RATE_LIMITED` with Retry-After, 5xx → `TRANSIENT`, …).
- `internal/connectors/{salesforce,hubspot}` — schema-mapping adapters.
- `internal/simulator` — provider simulator core: in-memory store, cursor
  pagination, token-bucket rate limiter, HMAC-signed webhook dispatch, and a
  fault-injection manager (`/admin/faults`).
- `internal/db` — pgx pools (app/engine), embedded migrations, and the
  `WithTenant` helper that scopes every query to a tenant via
  `SET LOCAL app.tenant_id`.
- `internal/store` — data-access layer for tenants, connections, api keys,
  source events.
- `internal/api` — HTTP handlers, auth middleware (API keys + bootstrap),
  webhook gateway.
- `internal/events` — immutable canonical event contract + partition key.
- `internal/observability` — OpenTelemetry SDK wiring, Prometheus exporter,
  per-service HTTP metrics.

## Security model

Two Postgres roles. `syncforge_app` is subject to Row-Level Security; every
tenant-scoped table has `FORCE ROW LEVEL SECURITY` so even the table owner
cannot bypass isolation. The API authenticates via API keys (SHA-256 hash at
rest) and runs each request inside `WithTenant`, which sets the GUC `app.tenant_id`
for the transaction. Reads with no tenant context return zero rows (fail-closed).
`syncforge_engine` has `BYPASSRLS` and is used for cross-tenant administration
(tenant management, key verification, internal workers).

## Event flow (Phase 2+)

```text
provider mutation ─▶ signed webhook ─▶ gateway (HMAC verify)
   ─▶ source_events (durable, unique tenant+source+event_id)
   ─▶ publish to Redpanda (partition key tenant:entity_type:entity_id)
   ─▶ sync worker (idempotency log → normalize → conflict check → dest)
```

The webhook gateway and `source_events` table already exist; publishing and
consuming land in Phase 2.
