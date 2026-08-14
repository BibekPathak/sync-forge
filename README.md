# SyncForge

Real-time, multi-tenant enterprise data synchronization and conflict-resolution
engine. SyncForge synchronizes entities bidirectionally between external
systems (CRM A ↔ SyncForge ↔ CRM B) while handling distributed events, duplicate
delivery, out-of-order events, retries, partial failures, rate limits, schema
evolution, synchronization loops, conflict detection/resolution, dead-letter
events, reconciliation and tenant isolation.

> **Status: Phase 1 (Foundation) — build in progress.** See
> [docs/architecture.md](docs/architecture.md) and the README section below for
> what exists today.

---

## 1. Problem statement

Enterprise integrations are usually hand-written point-to-point bridges. Every
new system pair means new mapping, retry, conflict, and ordering code. These
bridges silently lose data: a webhook is missed, an event arrives twice, an old
event overwrites a newer one, or two systems edit the same record and whichever
syncs last silently wins.

SyncForge is a centralized synchronization fabric. Each external system plugs in
once through a connector; SyncForge owns mapping to a canonical model, ordering,
idempotency, retries, conflicts, reconciliation, and tenant isolation.

## 2. Why point-to-point integrations fail

| Problem | Point-to-point | SyncForge |
|---|---|---|
| N systems | N×(N−1) bridges | N connectors, one engine |
| Duplicate delivery | applied twice | idempotency log (exactly-once effect) |
| Out-of-order events | stale overwrite | per-source version checks |
| Conflicts | last-writer-wins silently | detection + 4 resolution strategies |
| Sync loops | A→B→A→B forever | provenance + fingerprint matching |
| Retries | ad-hoc | durable exponential backoff + DLQ |
| Schema drift | breaks the bridge | version-aware adapters + DLQ |
| Tenant blast radius | none | PostgreSQL Row-Level Security |

## 3. Architecture (Phase 1)

```
                     ┌──────────────────────────────┐
                     │           cmd/api            │  REST control plane
                     │   /api/v1/*  +  webhook gw   │  auth (API keys, RLS)
                     └──────┬───────────────┬────────┘
                            │ Postgres (RLS)│ source_events (durable)
                            ▼               ▼
                     PostgreSQL 17    Redis 7    Redpanda (broker, Phase 2)
                            ▲               ▲
                     cmd/engine (workers arrive in Phase 2)
                     ──────────────────────────────────────────
                     sim-salesforce ──────┐  sim-hubspot
                     (REST + webhooks +   │  (different schema,
                      rate limit, faults) ┘   camelCase fields)
                     ──────────────────────────────────────────
                     Prometheus ─ Grafana ─ Next.js dashboard
```

### Services (separate processes only where it matters)

- **api** — HTTP API + webhook gateway. Stays up while workers are backlogged.
- **engine** — worker host (event processor, sync workers, retry, reconciliation
  land in Phase 2+).
- **sim-salesforce / sim-hubspot** — realistic fake providers (they *are* the
  "external systems"): REST, cursor pagination, per-provider rate limits, signed
  webhooks, failure injection.
- **postgres / redis / redpanda / prometheus / grafana / dashboard** — infra.

### The connector boundary

```text
Provider record ─▶ Provider Adapter ─▶ Canonical model ─▶ Sync Engine
                        ▲                                       │
                        └────────◀ denormalize ◀────────────────┘
```

`internal/connectors` defines `Connector` (HTTP verbs) and `Adapter`
(`Normalize`/`Denormalize`/`Validate`). Adding Shopify/SAP/Slack means adding an
adapter, not touching the engine.

## 4. Data model (Phase 1 tables)

Tenant-scoped tables all carry `tenant_id` and are protected by **PostgreSQL
Row-Level Security** (enforced, `FORCE ROW LEVEL SECURITY`, so the owning role
cannot bypass it). Roles:

- `syncforge_app` — RLS subject; API queries run inside a tenant context
  (`SET LOCAL app.tenant_id`), so the engine can never read across tenants.
- `syncforge_engine` — `BYPASSRLS`; internal workers + tenant administration.

Tables: `tenants`, `users`, `api_keys`, `connections`, `sync_policies`,
`canonical_records`, `source_events`, `processed_events`, `retry_queue`,
`dead_letter`, `conflicts`, `sync_jobs`, `reconciliation_runs`,
`sync_operations`, `outbound_writes`, `audit_log`.

Migrations are embedded (`migrations/`) and applied idempotently at startup.

## 5. Event model

Immutable events. Ingested provider webhooks are stored raw in `source_events`
keyed by `(tenant_id, source, event_id)` — the unique constraint guarantees the
gateway ingests each logical event exactly once (duplicates return "duplicate").
The canonical sync event contract lives in `internal/events`.

## 6. What Phase 1 delivers

- Go monorepo: `cmd/{api,engine,sim-salesforce,sim-hubspot}` + `internal/*`.
- PostgreSQL schema + migrations + enforced RLS + two roles.
- Connector interface + Salesforce & HubSpot adapters with schema mapping.
- Simulated providers: REST CRUD, cursor pagination, token-bucket rate limits
  (Salesforce 100/min, HubSpot 50/min), HMAC-signed webhooks, and fault
  injection (`POST /admin/faults`: failure rate, latency, auth failures,
  malformed payloads, rate limits, duplicate/delayed/out-of-order/dropped
  webhooks).
- Webhook gateway: signature validation → durable, idempotent ingestion.
- API: `/health`, `/api/v1/tenants`, `/api/v1/connections`,
  `/api/v1/metrics`, `/webhooks/{provider}/{tenant_slug}`.
- Auth: API keys (hashed at rest) + bootstrap key for tenant management.
- Observability: OpenTelemetry metrics via Prometheus, Grafana dashboard.
- Dashboard: Next.js static export + proxy.

## 7. Running it

```bash
docker compose -f deploy/compose/docker-compose.yml up --build -d
# or
make up
```

| Service | URL |
|---|---|
| API | http://localhost:8080 |
| Dashboard | http://localhost:3001 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |
| Redpanda | localhost:29092 |
| Salesforce sim | http://localhost:9081 |
| HubSpot sim | http://localhost:9082 |

Seed data is created automatically: tenant **Acme**, Salesforce + HubSpot
connections, API key **`sfk_acme_dev`**.

### Smoke test

```bash
./scripts/demo.sh
# 1. health + seeded tenants/connections
# 2. update a Salesforce record → signed webhook → gateway → source_events
```

## 8. Testing

```bash
make test-unit          # offline unit tests (simulators, connectors, rate limits)
make test-integration   # RLS isolation + webhook ingest against docker postgres
```

Tests that currently exist:

- **Unit**: token-bucket rate limiter, pagination cursors, fault injection
  (failure/latency/rate-limit/malformed/auth), webhook signing & duplication,
  connector normalize/denormalize round-trips, schema validation errors,
  429 classification.
- **Integration**: cross-tenant read/write isolation under RLS, fail-closed
  reads without a tenant context, signed webhook ingest + duplicate suppression,
  API auth (API key / bootstrap).

> Integration tests require `postgres` running (`make test-integration` starts
> it) and connect with the two service roles.

## 9. Known limitations (Phase 1)

- Engine has no event-processing workers yet (Phase 2).
- Tenant management is gated by a fixed bootstrap key until full RBAC (Phase 7).
- Simulated providers keep state in memory (intentional: they are external
  systems; their durability isn't SyncForge's concern).
- Redpanda is booted but unused until Phase 2 (broker-backed pipeline).

## 10. Next phases

Phase 2 — Salesforce → SyncForge → HubSpot one-way sync: webhook → queue →
worker → destination, with durable idempotency. Then bidirectional sync,
reliability (retries/DLQ/rate limiting), conflicts, reconciliation, RBAC,
observability, failure injection, and benchmarking. See `docs/architecture.md`.
