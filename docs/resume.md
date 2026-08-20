# Resume / Project Summary

A concise project description and bullets for an FDE (Founding / Full-stack /
Distributed Engineer) resume. **Every number is measured** on the local
reference environment (see [benchmarking.md](benchmarking.md)); no figure is
claimed beyond what was actually observed.

## Project

**SyncForge — Real-Time Enterprise Data Synchronization & Conflict-Resolution
Engine** · Go, PostgreSQL, Redpanda, Redis, Next.js, Prometheus, OpenTelemetry

## Bullets

**Architecture** — Built a multi-tenant, event-driven synchronization engine in
Go for bidirectional CRM synchronization: a durable Redpanda pipeline,
PostgreSQL Row-Level-Security tenant isolation, resumable reconciliation, a
pluggable connector architecture, and a full auth stack (RBAC API keys, sessions,
TOTP MFA, backup codes, OIDC SSO, account management, brute-force protection).

**Distributed systems** — Implemented at-least-once delivery with idempotent,
exactly-once-effect destination writes; per-entity ordering with per-source
version checks; durable retries and a dead-letter queue with operator replay;
checkpointed resumable full syncs; client-side rate-limit backoff; and
fingerprint-based loop prevention. Formalized in a documented consistency model.

**Consistency** — Engineered field-level conflict detection with
last-write-wins, source-priority, field-merge, and manual operator-resolution
strategies; added reconciliation sweeps that detect and repair missed, deleted,
and drifted records (auto or operator-approved).

**Reliability** — Built a seeded, probabilistic failure-injection mechanism
(duplicate/out-of-order/malformed webhooks, transient failures, latency ranges,
rate limits) and validated recovery under stochastic faults: on the local
reference environment, a faulted burst converged with **0 data loss** and all
surviving events applied. Benchmarked the stack end-to-end (see below).

**Performance (local reference)** — Measured with a reproducible harness:
**~3,700 events/sec ingestion** (webhook→queue, p99 ~16ms) and ~47 events/sec
end-to-end on a 12-core laptop; diagnosed PostgreSQL (90% CPU) as the dominant
bottleneck (per-event round-trips), applied one targeted fix (transactional
write path, +6% e2e), and documented the read path as the next bottleneck.
Methodology is reproducible on any hardware.

## Honest framing

- Numbers are **local reference**, not universal system throughput.
- The system provides at-least-once delivery with exactly-once *effect*, not
  exactly-once delivery.
- End-to-end throughput (~50 ev/s) reflects the serial, single-connection
  worker on a laptop with an in-memory simulator destination; the bottleneck
  diagnosis and the harness are the point, not a headline number.
