# Failure Recovery

How SyncForge keeps the *at-least-once + idempotent* promise when providers
fail, crash, or time out. Retries and dead-lettering ride on top of the worker
apply path described in [architecture.md](architecture.md).

## Delivery semantics

- The event bus is **at-least-once** (Redpanda manual offset commits): the
  broker ack happens only *after* the worker has durably handed off the event to
  either the retry queue or the dead-letter queue.
- Idempotent processing turns at-least-once into **exactly-once effect**:
  `processed_events (tenant_id, source, event_id)` is the claim log, and a
  retried apply that already succeeded is a no-op.
- We never claim exactly-once delivery; we guarantee exactly-once *effect*.

## Failure classification

Every connector/seam error is classified on the way out
(`internal/connectors/classify.go`):

| Error kind | Sources | May retry? | Action |
|---|---|---|---|
| transient | network dial/timeout, HTTP 5xx, malformed response | yes | re-queue with backoff |
| rate limit | HTTP 429, provider throttle | yes | honor `Retry-After` (only shortens the delay) |
| permanent | `PERMANENT` custom errors | no | straight to DLQ |
| authentication | HTTP 401/403 | no | straight to DLQ (operator intervention) |
| schema | adapter `Validate` failures | no | straight to DLQ with serialized canonical payload |

`ShouldRetry(err)` consults the same classifier, so retry policy and error
reporting can never disagree.

## Durable retry pipeline

```
apply fails
  ├─ classification
  │   ├─ permanent ──▶ dead_letter (status='dlq'), source_events status='dlq'
  │   └─ transient  ──▶ release processed_events claim (worker can re-run)
  │                     source_events status='failed'
  └─ enqueue retry_queue (attempt + count, next_attempt_at = now + backoff(k))

retry engine (polls every 250ms)
  ├─ claim due rows: SELECT ... WHERE next_attempt_at <= now()
  │                    FOR UPDATE SKIP LOCKED   (admin pool, cross-tenant)
  ├─ re-apply through the same idempotent worker
  │   ├─ success      ──▶ delete retry row; mark source_events 'applied'
  │   │                    resolve pending dead_letter entry (status='resolved')
  │   └─ transient    ──▶ attempt exhausted (>= MaxAttempts) ──▶ DLQ
  │                       else backoff(attempt+1), next_attempt_at updated
  └─ permanent       ──▶ DLQ immediately, retry row deleted
```

Only one engine claimer can hold a row at a time (`FOR UPDATE SKIP LOCKED`), so
multiple engine replicas can run concurrently without double-apply.

### Backoff

`internal/backoff` computes `base × 2^(attempt-1)` capped at `MaxDelay`,
multiplied by jitter in [0.7, 1.3]. Defaults: base 1s, max 60s, 8 attempts.
These are configurable via `SYNCFORGE_RETRY_*` (see `internal/config`).

### Idempotency under retry

- `EnqueueRetry` / `InsertDeadLetter` use unique partial indexes on
  `(tenant_id, event_id)`, so a concurrent redelivery vs. retry-enqueue race
  converges instead of double-writing (migrations/0004_retry_dlq_keys.sql).
- A retried apply that already succeeded earlier is a no-op because the claim
  was never released on success.

## Dead-letter queue

An entry stores: tenant, source, event id, provider error class + message, the
serialized canonical event payload (so it can be replayed without the original
webhook), and a status lifecycle.

| Status | Meaning |
|---|---|
| `open` | awaiting operator action |
| `retrying` | replay in-flight (a retry row exists) |
| `resolved` | replay applied successfully |
| `discarded` | operator chose to drop it |

Operator API (`X-API-Key`):

- `GET /api/v1/dlq?status=&limit=` — list
- `GET /api/v1/dlq/{id}` — single entry
- `POST /api/v1/dlq/{id}/retry` — enqueue immediate retry
- `POST /api/v1/dlq/{id}/discard` — acknowledge & drop

Related read surface: `GET /api/v1/sync-events` filters source events by status
(`failed`, `dlq`) to find what is stuck.

## Initial full sync (resumable)

`sync_jobs` persists a cursor and processed-page count every batch
(`sync_job_batch_size`, default 1000). A job stuck in `running` for >60s is
adopted by any runner via `ClaimNextSyncJob`. On resume it continues from the
committed cursor; previously applied records are skipped by worker idempotency
(the `jobsync:<job>:<record>` event id), so a crashed full sync never duplicates
already-synced payloads.

## Metrics

- `sync_retry_scheduled_total` — retry rows enqueued
- `sync_dlq_events_total` — dead letters written
- plus existing `sync_worker_failures_total`, `sync_consumer_errors_total`,
  and the idempotency counters on the worker.

## Fault injection (Phase 10)

The simulator exposes `GET/POST /admin/faults` to script provider failure:

| Fault | Effect |
|---|---|
| `failure_rate` (0..1) | N% of API calls return 500 → TRANSIENT → retry |
| `latency_ms` | artificial latency on every call |
| `hang_ms` + `hang_percent` | N% of calls sleep past the connector timeout → client timeout → TRANSIENT → retry |
| `auth_failure` | all calls return 401 → AUTHENTICATION → DLQ |
| `malformed` | responses are broken JSON → SCHEMA_ERROR → DLQ |
| `drop_field` / `corrupt_field_type` | list/get responses omit a required field or set it to a wrong type → fails `Validate` (SCHEMA_ERROR) |
| `rate_limit_per_min` | throttles to N req/min (429 + Retry-After) |
| `drop_webhooks` / `duplicate_webhooks` / `webhook_delay_ms` / `out_of_order` | webhook delivery mutations |

A scripted chaos integration test (`TestChaosScriptedScenario`) walks healthy →
hard outage → recover → rate-limit → recover, asserting zero data loss and no
duplicate destination records at every phase.

## Interaction with the rest

- Tenant isolation: all of this runs inside one namespace per tenant
  (RLS-guarded tables); the retry engine claims across tenants only via the
  BYPASSRLS `syncforge_engine` role.
- Loop prevention still applies on retried applies: an echo of our own write is
  recognized by its fingerprint and dropped even when it arrives through the
  retry path.