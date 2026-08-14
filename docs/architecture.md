# SyncForge Architecture

This document is kept in sync with the implementation. It describes the system
as it exists today and marks where later phases extend it.

## Status

Phase 2 (One-way sync) implemented: Salesforce → SyncForge → HubSpot flows
through Redpanda with exactly-once-effect idempotency. Phases 3–10 build on
this structure.

## Process model

| Process | Role | Why a separate process |
|---|---|---|
| `cmd/api` | REST API + webhook gateway | Webhook ingestion must stay up while workers are backlogged; API is latency-sensitive |
| `cmd/engine` | Ingestion processor + sync worker (retry/reconciliation in Phases 4–6) | Scales independently of the API |
| `cmd/sim-salesforce`, `cmd/sim-hubspot` | Simulated external systems | They represent third-party systems, not SyncForge |

Infrastructure: PostgreSQL 17 (durable state + RLS), Redis 7 (rate limiting,
cache), Redpanda (durable event bus), Prometheus + Grafana (metrics), Next.js
dashboard.

## Event pipeline (Phase 2)

```text
provider mutation ─▶ signed webhook ─▶ gateway (HMAC verify)
   ─▶ source_events (durable; unique tenant_id+source+event_id)
   ─▶ ingestion processor (admin pool: SELECT ... FOR UPDATE SKIP LOCKED)
   ─▶ publish to Redpanda topic "sync.events" (key = tenant:entity_type:entity_id)
   ─▶ sync worker (consumer group, manual offset commit)
        ├─ ClaimProcessedEvent  (idempotency log; duplicates → no-op)
        ├─ resolve policy + source/destination connections
        ├─ normalize (source adapter) → canonical model
        ├─ denormalize (destination adapter) → write create/update/delete
        └─ upsert canonical_records (fields, source_versions, provider_ids, tombstone)
```

### Consistency model

- **Delivery**: at-least-once. Redpanda redelivers uncommitted messages on
  rebalance/restart.
- **Processing**: exactly-once *effect*. `processed_events` is claimed before
  work; duplicate deliveries (and the 100-duplicate case) collapse into one
  destination mutation.
- **Ordering**: per-entity ordering via the partition key
  `tenant:entity_type:entity_id`. Cross-source ordering + version checks are
  Phase 3.
- **Residual window**: a crash between claiming and persisting the canonical
  record can leave a claimed event that is skipped on redelivery; drift is
  repaired by reconciliation (Phase 6). This is the documented tradeoff for
  duplicate-suppression.

### Loop containment (Phase 2, one-way)

Destination writes cause the destination to emit webhooks back to SyncForge.
With one-way policies those events resolve to no reverse policy and are
released as no-ops. Phase 3 adds provenance + fingerprint matching for real
loop prevention in bidirectional mode.

## Key packages

- `internal/connectors` — `Connector` / `Adapter` interfaces, typed error
  classes (`TRANSIENT`, `PERMANENT`, `RATE_LIMITED`, `AUTHENTICATION`,
  `SCHEMA_ERROR`, `CONFLICT`, `NOT_FOUND`), shared HTTP client that classifies
  responses (429 → `RATE_LIMITED` with Retry-After, 5xx → `TRANSIENT`, …).
- `internal/connectors/{salesforce,hubspot}` — schema-mapping adapters;
  `internal/connectors/registry` builds them by name (the only provider switch
  in the core).
- `internal/simulator` — provider simulator core: in-memory store, cursor
  pagination, token-bucket rate limiter, HMAC-signed webhook dispatch, and a
  fault-injection manager (`/admin/faults`).
- `internal/eventbus` — `Bus` interface; Redpanda transport (franz-go, manual
  offset commit) and in-memory transport for tests.
- `internal/ingestion` — processor that drains `source_events` → topic.
- `internal/syncworker` — idempotent apply logic (claim → map → write →
  persist).
- `internal/db` — pgx pools (app/engine), embedded migrations, and the
  `WithTenant` helper that scopes every query to a tenant via
  `SET LOCAL app.tenant_id`.
- `internal/store` — data-access layer: tenants, connections, api keys,
  source events, processed events, canonical records, sync policies.
- `internal/api` — HTTP handlers, auth middleware (API keys + bootstrap),
  webhook gateway.
- `internal/events` — immutable canonical event contract + partition key.
- `internal/observability` — OpenTelemetry SDK wiring, Prometheus exporter,
  per-service HTTP + synchronization metrics.

## Security model

Two Postgres roles. `syncforge_app` is subject to Row-Level Security; every
tenant-scoped table has `FORCE ROW LEVEL SECURITY` so even the table owner
cannot bypass isolation. The API authenticates via API keys (SHA-256 hash at
rest) and runs each request inside `WithTenant`, which sets the GUC `app.tenant_id`
for the transaction. Reads with no tenant context return zero rows (fail-closed).
`syncforge_engine` has `BYPASSRLS` and is used for cross-tenant administration
(tenant management, key verification, internal workers). Worker tenant-scoped
operations still go through `WithTenant` so RLS remains active for them too.
