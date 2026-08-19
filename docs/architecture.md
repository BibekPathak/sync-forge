# SyncForge Architecture

This document is kept in sync with the implementation. It describes the system
as it exists today and marks where later phases extend it.

## Status

Phase 5 (Conflict detection + resolution) implemented: concurrent cross-source
edits are detected via field provenance and resolved by policy
(`last_write_wins` / `source_priority` / `field_merge` / `manual`), with a
durable audit trail and an operator API.

Phase 6 (Reconciliation) implemented: resumable sweeps classify every provider
record against the canonical model (drift / missed / deleted / missing) and
repair or park it. `auto` runs fix divergences inline (canonical wins by
default); `manual` runs park findings for operator approval via the API.

Phase 7 (RBAC) implemented: every tenant-scoped endpoint is gated by a role
rank (VIEWER < DEVELOPER < OPERATOR < ADMIN), API keys can be minted, listed,
and revoked via the API (raw key shown once), and tenant management moved from
a fixed bootstrap key to the ADMIN role.

Phase 8 (Observability) implemented: a Grafana dashboard for the full sync
pipeline (events, processing latency, destination writes, retries, DLQ,
duplicates/stale/loop drops, conflicts, reconciliation), Prometheus alerting
rules, and optional OpenTelemetry tracing of the API and worker apply path
(OTLP → collector → Jaeger, disabled without an endpoint).

Phase 9 (Benchmarking & load testing) implemented: Go benchmarks for the hot
pure-leaf paths (classification, conflict detect/resolve/merge, backoff,
error classification), a reusable `loadtest` webhook generator, a `cmd/loadgen`
CLI for driving a running stack, and integration load tests asserting sustained
throughput with zero data loss plus a burst-under-outage scenario that proves
the retry machinery converges without duplicate destination records.

Phase 10 (Failure injection & chaos) implemented: the simulator fault suite
gains transient hangs (timeout → durable retry → recovery, with a configurable
connector timeout so tests trip it quickly) and partial payload corruption
(dropped fields / wrong-typed values that pass JSON parsing but fail schema
validation). A scripted chaos integration test walks healthy → outage →
recover → rate-limit → recover asserting zero data loss and no duplicates, and
`scripts/bench.sh` benchmarks the full compose stack (real Redpanda + Postgres)
at increasing concurrency with metric sampling.

Phase 11 (Audit trail) implemented: the documented durable audit trail is now
real. `audit_log` records every operator/security action (conflict resolve /
dismiss, reconciliation finding apply / dismiss, DLQ retry / discard, API key
and user management, login success and failure) with the acting identity, and
`sync_operations` is a per-write ledger of every destination mutation the
engine applies, backing loop-prevention forensics and the "every write is
auditable" guarantee. Both are tenant-scoped (RLS) and read via the API.

Phase 12 (Session lifecycle) implemented: user sessions are now server-side
and revocable. Every login records a `sessions` row keyed by a `jti` embedded
in the HMAC token; verification checks signature, expiry, and that a live
(unrevoked) session row exists, so `POST /api/v1/auth/logout` revokes the
token and `POST /api/v1/auth/refresh` rotates it (old token dies immediately).
Sessions are tenant-scoped via RLS, listable, and revocable per user.

Phase 13 (Multi-factor auth) implemented: users can enroll a TOTP secret
(RFC 6238, dependency-free `internal/totp`) via `POST /api/v1/auth/mfa/enroll`,
confirm it with a code to enable it, and disable it with a code. When enabled,
`login` requires a valid 6-digit code in addition to the password. The dashboard
re-authenticates on a 401 so an expired session self-heals.

Phase 14 (MFA recovery codes) implemented: users may generate a set of
single-use backup codes (`POST /api/v1/auth/mfa/backup-codes`) for logging in
when their authenticator is unavailable. Raw codes are shown once; only their
SHA-256 hashes are stored (`users.backup_codes text[]`), and `login` consumes a
matching code atomically so reuse is impossible. This closes the lockout hole a
lost device would otherwise create.

Phase 15 (Account management) implemented: users can change their own password
(`POST /api/v1/auth/change-password`, current password verified) and ADMINs can
reset a password or change a role (`POST /api/v1/users/{id}/reset-password`,
`POST /api/v1/users/{id}/role`, subject to the caller's rank). Every password
change/reset revokes all of that user's sessions, so a leaked token cannot
outlive the change, and each action is audit-logged.

Phase 16 (Login brute-force protection) implemented: every login failure is
recorded durably in `login_attempts` (RLS-scoped, per tenant+email+ip), and
login enforces an account lockout — after `SYNCFORGE_LOGIN_MAX_FAILURES`
(default 5) failures within `SYNCFORGE_LOGIN_LOCKOUT_MIN` (default 15) minutes,
further attempts are rejected with 429 until a successful login (or an ADMIN
reset) clears the history. A per-IP Redis fixed-window throttle
(`SYNCFORGE_LOGIN_THROTTLE_PER_MIN`, default 30/min) additionally slows
distributed guessing; it is best-effort when Redis is unavailable.

Phase 17 (Active sessions) implemented: the planned-but-unwired admin session
surface is live. `GET /api/v1/sessions` (ADMIN) lists the tenant's live
sessions (user, role, created/expiry), and
`POST /api/v1/users/{id}/revoke-sessions` (ADMIN) signs a user out everywhere
by revoking all of their sessions at once, audit-logged.

Phase 18 (OIDC SSO) implemented: `POST /api/v1/auth/oidc/login` accepts an ID
token from a configured issuer and verifies it against the issuer's JWKS
(signature, issuer, audience, expiry) via the dependency-free `internal/oidc`
client. The user is resolved by email in the tenant or auto-provisioned as
VIEWER (`SYNCFORGE_OIDC_AUTO_PROVISION`), and a normal SyncForge session is
issued so SSO logins use the same RBAC/session surface. The compose stack ships
a mock IdP (`cmd/sim-oidc`, `internal/simulator.OIDCProvider`) serving
discovery, JWKS, and token endpoints.

## Process model

| Process | Role | Why a separate process |
|---|---|---|
| `cmd/api` | REST API + webhook gateway | Webhook ingestion must stay up while workers are backlogged; API is latency-sensitive |
| `cmd/engine` | Ingestion processor + sync worker (retry/reconciliation in Phases 4–6) | Scales independently of the API |
| `cmd/sim-salesforce`, `cmd/sim-hubspot`, `cmd/sim-oidc` | Simulated external systems | They represent third-party systems (incl. the SSO IdP), not SyncForge |

Infrastructure: PostgreSQL 17 (durable state + RLS), Redis 7 (rate limiting,
cache), Redpanda (durable event bus), Prometheus + Grafana (metrics), Next.js
dashboard.

## Event pipeline

```text
provider mutation ─▶ signed webhook ─▶ gateway (HMAC verify)
   ─▶ source_events (durable; unique tenant_id+source+event_id)
   ─▶ ingestion processor (admin pool: SELECT ... FOR UPDATE SKIP LOCKED)
   ─▶ publish to Redpanda topic "sync.events" (key = tenant:entity_type:entity_id)
   ─▶ sync worker (consumer group, manual offset commit)
        ├─ ClaimProcessedEvent  (idempotency log; duplicates → no-op)
        ├─ canonical entity type (provider "contact" → canonical "customer")
        ├─ resolve policy + source/destination connections
        ├─ identity resolution (provider id → email)
        ├─ version check (drop out-of-order) → loop check (drop own echoes)
        ├─ conflict detect + resolve (field provenance vs incoming)
        │    └─ manual → park CONFLICT_PENDING; auto → merge + AUTO_RESOLVED audit
        ├─ normalize → canonical → denormalize → write create/update/delete
        └─ upsert canonical_records + outbound_writes fingerprint
         └─ operator resolve (POST /conflicts/{id}/resolve) → synthetic
             resolution event (ResolvedConflictID marker) → retry queue →
             worker applies the chosen side exactly-once
        └─ reconciliation (Phase 6, sync_jobs type='reconcile'):
             claim reconcile job → sweep provider List (cursor checkpointed)
             → classify (drift/missed/deleted/missing) → record finding
             (idempotent per run+kind+provider id)
             ├─ auto: repair inline via deterministic reconcile event
             │    (ReconcileFindingID marker) → worker applies direction
             │    (push_canonical / adopt_provider / delete) exactly-once
             └─ manual: park finding (pending) → operator
                  POST /reconciliations/{id}/findings/{fid}/apply|dismiss
```

### Consistency model

- **Delivery**: at-least-once. Redpanda redelivers uncommitted messages on
  rebalance/restart.
- **Processing**: exactly-once *effect*. `processed_events` is claimed before
  work; duplicate deliveries (and the 100-duplicate case) collapse into one
  destination mutation.
- **Ordering**: per-entity ordering via the partition key
  `tenant:entity_type:entity_id`; per-source version checks drop stale events
  (`source_versions[source]`), so out-of-order delivery cannot overwrite newer
  state.
- **Loop prevention**: `outbound_writes` stores the fingerprint of what was last
  written to each destination. An incoming event that normalizes to the same
  fingerprint is SyncForge's own echo and is dropped. Deletes are guarded by the
  canonical tombstone (a tombstoned entity's delete events are no-ops).
- **Conflicts**: every apply stores field provenance (last writer per field) in
  `canonical_records.field_provenance`. An incoming event that changes a field
  another source last wrote is a concurrent edit → detected (`internal/conflict`)
  and resolved per the policy's `conflict_strategy`. Auto strategies merge
  transparently (with an `AUTO_RESOLVED` audit row); `manual` parks the conflict
  until an operator resolves it. All conflicts are durable + idempotent
  (`conflicts` table, unique on the source/version pair).
- **Identity**: provider id lookup first; email fallback links independently
  created records to the same canonical entity.
- **Reconciliation**: reconcile sweeps re-derive truth from the provider's live
  records. Canonical wins for drift (default `push_canonical`); a record the
  provider lost (`missing`) is only re-created when the tenant's delete policy
  does not treat external deletions as authoritative (`ignore` /
  `tombstone_only`); a tombstoned canonical that still has a live provider
  record is deleted there only when deletes propagate; provider records we
  never ingested (`missed`) are adopted and propagated. All repairs run through
  the worker with deterministic event ids (exactly-once) and record outbound
  fingerprints so their own echoes are dropped. `manual` runs park findings for
  operator apply/dismiss.
- **Residual window**: a crash between claiming and persisting the canonical
  record can leave a claimed event that is skipped on redelivery; drift is
  repaired by reconciliation (Phase 6). This is the documented tradeoff for
  duplicate-suppression.

## Key packages

- `internal/connectors` — `Connector` / `Adapter` interfaces, typed error
  classes (`TRANSIENT`, `PERMANENT`, `RATE_LIMITED`, `AUTHENTICATION`,
  `SCHEMA_ERROR`, `CONFLICT`, `NOT_FOUND`), shared HTTP client that classifies
  responses (429 → `RATE_LIMITED` with Retry-After, 5xx → `TRANSIENT`, …).
  Adapters expose `CanonicalEntityType` so provider naming (HubSpot `contact`)
  maps to canonical types (`customer`).
- `internal/connectors/{salesforce,hubspot}` — schema-mapping adapters;
  `internal/connectors/registry` builds them by name.
- `internal/simulator` — provider simulator core: in-memory store, cursor
  pagination, token-bucket rate limiter, HMAC-signed webhook dispatch, and a
  fault-injection manager (`/admin/faults`) covering failure rate, latency,
  auth, malformed payloads, rate limits, webhook drop/duplicate/out-of-order,
  transient hangs (timeout) and partial payload corruption.
- `internal/eventbus` — `Bus` interface; Redpanda transport (franz-go, manual
  offset commit) and in-memory transport for tests.
- `internal/ingestion` — processor that drains `source_events` → topic.
- `internal/syncworker` — idempotent bidirectional apply: identity resolution,
  version checks, echo detection, conflict detection/resolution (including
  operator resolution events via `Provenance.ResolvedConflictID`), propagation,
  canonical persistence, and reconcile repair application (via
  `Provenance.ReconcileFindingID`).
- `internal/reconcile` — Phase 6 engine: classifies provider records against
  the canonical model, records findings, and repairs divergences in auto mode
  (deterministic reconcile events through the worker) or parks them for the
  operator API. Classification is a pure, unit-tested leaf.
- `internal/conflict` — pure leaf package for field-level conflict detection and
  strategy resolution (`last_write_wins`, `source_priority`, `field_merge`,
  `manual`); unit-tested in isolation.
- `internal/db` — pgx pools (app/engine), embedded migrations, and the
  `WithTenant` helper that scopes every query to a tenant via
  `SET LOCAL app.tenant_id`.
- `internal/store` — data-access layer: tenants, connections, api keys, users,
  sessions, login attempts, source events, processed events, canonical records,
  sync policies, outbound writes, conflicts, reconciliation runs and findings,
  audit log and sync operations ledger.
- `internal/totp` — dependency-free RFC 6238 TOTP (HMAC-SHA1, 6 digits, 30s
  window) used for multi-factor login; unit-tested against the RFC vectors.
- `internal/oidc` — dependency-free OIDC client: discovery, JWKS-based RS256
  ID-token verification (signature, issuer, audience, expiry), and claim
  extraction; paired with `internal/simulator.OIDCProvider` (mock IdP).
- `internal/api` — HTTP handlers, auth middleware (role-ranked API keys and
  per-user sessions), webhook gateway.
- `internal/events` — immutable canonical event contract + partition key.
- `internal/observability` — OpenTelemetry SDK wiring: Prometheus-exporter
  metrics (`http_*`, `sync_*`), optional OTLP tracing (`InitTracing`), and
  per-service HTTP + synchronization metrics. `deploy/` ships a Grafana
  dashboard, Prometheus alert rules, and an OTel collector + Jaeger.
- `load_test` — reusable webhook load generator (throughput/latency reporting)
  used by the integration load tests and the `cmd/loadgen` CLI.

## Security model

Two Postgres roles. `syncforge_app` is subject to Row-Level Security; every
tenant-scoped table has `FORCE ROW LEVEL SECURITY` so even the table owner
cannot bypass isolation. The API authenticates via API keys (SHA-256 hash at
rest) and runs each request inside `WithTenant`, which sets the GUC `app.tenant_id`
for the transaction. Reads with no tenant context return zero rows (fail-closed).
`syncforge_engine` has `BYPASSRLS` and is used for cross-tenant administration
(tenant management, key verification, internal workers). Worker tenant-scoped
operations still go through `WithTenant` so RLS remains active for them too.

Each API key carries a role (`VIEWER` < `DEVELOPER` < `OPERATOR` < `ADMIN`);
`requireRole(min)` ranks keys and rejects anything below the endpoint's
requirement (403). Role gates are layered on top of `authenticate`, which tries
API-key auth first and then falls back to a signed user session token — so
service-to-service (API key) and human (login) callers reach the same role
gate. `authenticate` injects `tenant_id`, `role`, `actor`, and (for keys)
`key_id` into the request context. Tenant
management and key minting/revocation are `ADMIN`-only; key creation is
tenant-scoped via `CreateTenantAPIKey` so RLS enforces the `WITH CHECK`.
A key cannot revoke itself, and no key can mint another above its own rank.
Raw keys are shown exactly once at creation; only the hash is stored.

Users log in via `POST /api/v1/auth/login` (tenant slug + email + password).
Passwords are bcrypt-hashed (`users.password_hash`); login records a
server-side `sessions` row (RLS-scoped, keyed by a `jti`) and returns a signed
HMAC token (12h TTL, `SYNCFORGE_AUTH_SECRET`) carrying `tenant_id`, `role`, and
`jti`. `requireRole` verifies the signature and then checks the session row is
live, so logout (`POST /api/v1/auth/logout`) and rotation
(`POST /api/v1/auth/refresh`) take effect immediately — a revoked or rotated
token cannot authenticate even before its TTL elapses.
Users are tenant-scoped and ADMIN-created (`POST/GET /api/v1/users`), and RLS
enforces the `WITH CHECK` exactly like API keys. Multi-factor: an enrolled user
(`totp_secret`, RFC 6238 via `internal/totp`) must supply a 6-digit code at
login; enroll/confirm/disable endpoints are self-service behind a user session.
Users may also generate single-use backup codes (`users.backup_codes text[]`,
SHA-256-hashed) that `login` accepts in place of a TOTP code and consumes
atomically — a code can never be replayed. Account management: a user can change
their own password (`POST /api/v1/auth/change-password`, current password
verified); an ADMIN can reset a password or change a role
(`POST /api/v1/users/{id}/reset-password`, `POST /api/v1/users/{id}/role`).
Password changes and resets revoke all of the target user's sessions, so a
compromised token cannot outlive a credential change. Login is further
protected against brute force: failures are recorded in `login_attempts` and
drive a per-account lockout (429 after N failures within a window, reset by a
successful login), while a per-IP Redis throttle bounds the attempt rate. Both
are configurable and best-effort where a backend is unavailable. ADMINs can
inspect live sessions (`GET /api/v1/sessions`) and sign a user out everywhere
(`POST /api/v1/users/{id}/revoke-sessions`). OIDC SSO (`POST
/api/v1/auth/oidc/login`) verifies an external ID token against the issuer's
JWKS (RS256) before resolving or auto-provisioning the tenant user, so an SSO
login issues the same session surface as password login.
