# SyncForge

Real-time, multi-tenant enterprise data synchronization and conflict-resolution
engine. SyncForge synchronizes entities bidirectionally between external
systems (CRM A ↔ SyncForge ↔ CRM B) while handling distributed events, duplicate
delivery, out-of-order events, retries, partial failures, rate limits, schema
evolution, synchronization loops, conflict detection/resolution, dead-letter
events, reconciliation and tenant isolation.

> **Status: Phase 4 (Reliability) — build in progress.** Bidirectional
> synchronization plus durable retries with exponential backoff + jitter, a
> dead-letter queue with operator replay, client-side rate limiting, and
> resumable initial full-sync jobs. See
> [docs/architecture.md](docs/architecture.md) and
> [docs/failure-recovery.md](docs/failure-recovery.md).

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
- **engine** — worker host (event processor, sync workers, retry engine, sync
  job runner, reconciliation land in Phase 2+).
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

## 6. What Phase 4 delivers

Reliability on top of the bidirectional apply path (`internal/retry`,
`internal/syncjob`, `internal/backoff`, `internal/connectors/ratelimit.go`).
Full detail in [docs/failure-recovery.md](docs/failure-recovery.md).

1. **Failure classification** — every connector error is classified
   (`internal/connectors/classify.go`) as *transient* (network, 5xx, 429 → may
   retry), *permanent* (`PERMANENT` custom errors, auth, schema errors → never
   retried), or *rate-limited* (honor `Retry-After`). `ShouldRetry` decides the
   path.
2. **Durable retries** — a failed apply does not lose the event: the idempotency
   claim is released, `source_events` is marked `failed`, and a `retry_queue`
   row is written (`EnqueueRetry`, idempotent via a unique
   `(tenant_id, event_id)` index). The retry engine (`internal/retry`) claims due
   rows (`FOR UPDATE SKIP LOCKED` on the admin pool), re-applies via the same
   idempotent worker, and re-queues with **exponential backoff + jitter**
   (`internal/backoff`: `base × 2^(attempt-1)`, capped at max, ±30% jitter).
   Success deletes the row and resolves any pending DLQ entry.
3. **Dead-letter queue** — attempts exhaust (`MaxAttempts`, default 8) or the
   error is permanent → the event is parked in `dead_letter` (`status='dlq'`)
   with the error class and serialized canonical payload. Operators can
   `GET /api/v1/dlq`, `POST /api/v1/dlq/{id}/retry` (replay) and
   `POST /api/v1/dlq/{id}/discard` through the API; stale events can be
   inspected via `GET /api/v1/sync-events`.
4. **Client-side rate limiting** — token-bucket `Limiter` on the shared HTTP
   client (Salesforce 100/min, HubSpot 50/min by default) so workers slow down
   instead of hammering providers; callers can `Wait` before building requests.
5. **Resumable initial full-sync** — `sync_jobs` keep a cursor + processed page
   count, persisted every batch (`ClaimNextSyncJob` adopts jobs stale >60s).
   On crash the runner resumes from the last committed cursor; records already
   applied are skipped by worker idempotency, so the full sync is
   **exactly-once-effect** end to end.

### Phase 3 (bidirectional sync)

Bidirectional synchronization. Every apply runs:

1. **Identity resolution** — map the incoming provider record to a canonical
   entity by provider id, then by email (a HubSpot contact created
   independently that shares an email with a Salesforce customer merges into
   the same canonical record instead of duplicating).
2. **Ordering** — per-source version check: an event with
   `source_version <= source_versions[source]` is an out-of-order replay and is
   dropped (`sync_stale_events_total`).
3. **Loop prevention** — every write SyncForge makes to a destination is
   fingerprinted into `outbound_writes`. When a destination echoes that change
   back, the incoming event normalizes to the same fingerprint → recognized as
   SyncForge's own write → dropped (`sync_loop_events_prevented_total`). Deletes
   are guarded by the canonical tombstone. Proven by
   `TestLoopPrevention` / `TestBidirectionalSync` (versions settle, no
   oscillation).
4. **Propagation** — the change is written to every configured destination and
   the canonical state (fields, per-source versions, provider-id map, tombstones)
   is persisted.

Policies are bidirectional: `salesforce→hubspot` and `hubspot→salesforce`.
The adapter layer owns provider→canonical naming (`CanonicalEntityType`, so
HubSpot's `contact` maps to the canonical `customer`).

### Phase 2 (one-way sync)

```text
provider mutation ─▶ signed webhook ─▶ webhook gateway (HMAC verify)
   ─▶ source_events (durable, unique tenant+source+event_id)
   ─▶ ingestion processor (drains source_events, publishes to Redpanda)
   ─▶ sync worker (idempotency claim ─▶ normalize ─▶ map ─▶ write destination)
   ─▶ canonical_records (provider-id mapping, source versions, tombstones)
```

- **Event bus**: `internal/eventbus` with a Redpanda transport (franz-go,
  manual offset commits = at-least-once) and an in-memory transport for tests.
  Partition key = `tenant:entity_type:entity_id` gives per-entity ordering.
- **Ingestion processor** (`internal/ingestion`): polls `source_events` for
  `received` events, publishes to `sync.events`, transitions to `validated`.
- **Sync worker** (`internal/syncworker`): claims events in the `processed_events`
  idempotency log, resolves policy + connections, maps and writes, persists
  canonical state.
- **Idempotency (exactly-once effect)**: `processed_events` PK
  `(tenant_id, source, event_id)` is claimed *before* any work; duplicate
  deliveries are no-ops. Proven by `TestWorkerDedupes100Duplicates`
  (100 duplicates → 1 destination mutation).
- **Delete propagation**: tombstones retained, deletes propagated per policy.

### Phase 1 (foundation)

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
- **Pipeline (integration, in-process)**: webhook → ingestion → bus → worker →
  destination create/update/delete; **100 duplicate events → exactly 1
  destination mutation**; canonical provider-id mapping + source versions;
  delete propagation + tombstones.
- **Bidirectional (integration, in-process)**: SF→HS and HS→SF propagation with
  settlement (no oscillation); **loop prevention** (echo recognized as own
  write, not re-propagated); **out-of-order** (v3 then v2 → v2 dropped);
  **identity resolution by email** (independent HubSpot contact merges into the
  existing canonical instead of duplicating).
- **Reliability (integration, in-process)**: provider outage is durable and
  recovers exactly-once; worker crash releases its claim and redelivery stays
  safe (no duplicates); schema errors go straight to DLQ and can be discarded;
  retry exhaustion → DLQ → operator replay → resolved; full-sync job crashed
  mid-page resumes from its committed cursor with exactly-once effect.

> Integration tests require `postgres` running (`make test-integration` starts
> it) and connect with the two service roles.

## 9. Known limitations (Phase 4)

- Conflict *detection* is not yet built: a concurrent edit of the same field in
  both systems still silently applies (Phase 5 adds conflicts + resolution
  strategies; field provenance is tracked from now on).
- Identity resolution is email-only and single-match; ambiguous or missing
  emails fall back to a new canonical record.
- Retries use fixed configured bounds (base/max/max-attempts); adaptive retry
  honoring provider hints is limited to `Retry-After` shrinking, not dynamic
  growth.
- Tenant management is gated by a fixed bootstrap key until full RBAC (Phase 7).
- Simulated providers keep state in memory (intentional: they are external
  systems; their durability isn't SyncForge's concern).

## 10. Next phases

Phase 5 — conflict detection + resolution strategies. Then reconciliation
(Phase 6), RBAC (Phase 7), observability, failure injection, and benchmarking.
