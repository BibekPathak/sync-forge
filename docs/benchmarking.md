# Benchmarking

How SyncForge is measured, what the numbers mean, and how to reproduce them.

## Consistency claim (read this first)

> **At-least-once event delivery with idempotent, exactly-once-effect
> destination writes under the engine's supported failure model.**

SyncForge does **not** claim exactly-once *delivery* — that is impossible to
guarantee in a distributed system. It guarantees exactly-once *effect* at the
destination: every logical event produces exactly one destination mutation,
even when Redpanda redelivers it (at-least-once delivery) or the worker crashes
mid-apply. See [consistency-model.md](consistency-model.md).

Every number in this document must be interpreted against that claim.

## Measured results (local reference)

| Test | Result |
|---|---|
| Ingestion throughput | ~3,700 events/sec @ concurrency 32 |
| End-to-end throughput | ~47 events/sec |
| End-to-end after targeted fix | ~50 events/sec |
| E2E improvement | +6% |
| Latency p50 / p95 / p99 | 7.9 ms / 12.7 ms / 15.9 ms (ingestion) |
| Processing latency p50/p95/p99 | 25 ms / 50 ms / 50 ms (engine histogram) |
| Data loss | 0 |
| Destination writes | 15,000 / 15,000 |
| Duplicates suppressed | 0 |
| DLQ / retries / conflicts | 0 / 0 / 0 |

Workload: 15,000 create events @ concurrency 32, 1 topic partition, on the
12-core reference machine recorded below. These are **local reference** numbers,
not universal system throughput; the harness reproduces the same workload on any
hardware.

## Reference environment

Results are labeled **local reference benchmark**: they describe *this*
hardware and configuration, not universal system throughput. The identical
workload can be re-run on any machine via the harness below; record the
environment with each run.

```
scripts/record-env.sh
```

Captured per run:

| Category | Values recorded |
|---|---|
| Hardware | CPU model, cores, RAM |
| OS / toolchain | OS, Go version, Docker version |
| Infra | PostgreSQL, Redis, Redpanda versions |
| Limits | per-container CPU shares / memory limits |
| Workload | event payload size, concurrency, batch size, warm-up, duration |
| Topic | partitions, replication, consumers |
| Bus | producer compression, consumer offset mode, consumer group |

## What is measured

Two surfaces are measured independently:

1. **Ingestion** — webhook → `source_events` accepted. Reported as
   events/sec and p50/p95/p99 HTTP latency from the load generator's own
   measurements.
2. **End-to-end sync** — event → canonical record + destination converged.
   Reported as time-to-converge and the engine's
   `sync_processing_duration_seconds` histogram (p50/p95/p99).

Reliability counters are captured as **deltas per run** from the engine's
Prometheus endpoint: success, failed, duplicates suppressed, destination
writes, DLQ entries, retries scheduled, loop-events prevented, conflicts
detected/resolved.

Data-loss rate is derived as `accepted − eventually-canonical` at convergence
(0 in a healthy run).

## Running the harness

```bash
# 100K events, concurrency sweep to 64, 1 partition
./scripts/bench.sh 100000 64 1

# 1M events, 4 partitions
./scripts/bench.sh 1000000 64 4
```

The harness:

- brings up the full compose stack,
- **recreates `sync.events` with the requested partition count** via `rpk`,
- truncates sync state,
- lifts the simulators' rate limits (so the benchmark measures the pipeline,
  not the sims' documented caps),
- runs a concurrency sweep (1,2,4,8,16,32,… up to the max),
- waits for end-to-end convergence between runs,
- samples per-service CPU/memory during the run,
- writes a timestamped report to `$REPORT_DIR` (default `/tmp/syncforge-bench`).

## Results

*Local reference benchmark on the machine recorded above. 1 partition is the
canonical baseline. Results are NOT universal system throughput.*

### Baseline — 15,000 events @ concurrency 32, 1 partition

Measured with `scripts/bench-one.sh 15000 32` on the untouched codebase.

| Surface | Value |
|---|---|
| Ingestion (webhook→source_events) | **3,727 ev/s**, p50 7.9ms / p95 12.7ms / p99 15.9ms |
| End-to-end sync (→ canonical) | **47 ev/s**, converged in 322s for 15,000 |
| Processing latency (engine histogram) | p50 25ms / p95 50ms / p99 50ms |
| Success / failed | 15,000 / 0 |
| Duplicates suppressed | 0 |
| Destination writes | 15,000 |
| DLQ / retries / conflicts | 0 / 0 / 0 |
| Data loss (received − canonical) | 0 |

### Bottleneck diagnosis (Phase 3)

`docker stats` during the run showed **PostgreSQL at ~90% CPU** while the engine
was ~10%. The worker's `Process` issues ~10 sequential `store.*` round-trips
per event (claim, policies, connection lookup, canonical lookup, echo check,
outbound write, sync-operation ledger, canonical upsert, status update), each
its own `WithTenant` transaction, on a single consumer goroutine. Per-event
serial DB round-trips are the dominant cost — not Redpanda, not the network.

### One targeted fix + rerun (identical workload)

**Fix:** collapsed the worker's end-of-apply *write path* — the destination
outbound-write fingerprints, the `sync_operations` ledger rows, and the
canonical upsert — into **one `PersistApplyState` transaction**
(`internal/store/applytx.go`) instead of ~3 separate `WithTenant`
transactions per event.

| Metric | Baseline | After fix | Δ |
|---|---|---|---|
| End-to-end | 47 ev/s | **50 ev/s** | +6% |
| Processing p50/p95/p99 | 25/50/50ms | 25/50/50ms | — |
| Data loss | 0 | 0 | — |
| DLQ / retries / failures | 0/0/0 | 0/0/0 | — |

**Honest conclusion:** the write-path consolidation helped modestly (+6%).
The measured evidence points to the per-event **read path** (connection +
canonical + echo lookups, one transaction each) as the next dominant cost.
Per the benchmark discipline (one bottleneck → one targeted fix → one rerun),
no further optimization was performed in this pass; the read path is a
documented follow-up.

### Reproducibility

To re-run on any machine:

```bash
./scripts/record-env.sh                     # capture environment
./scripts/bench.sh 100000 64 1 20000         # full harness (curve + headline)
./scripts/bench-one.sh 15000 32              # one-shot at a single concurrency
```

The workload is deterministic (fixed payload template, no randomness). Record
the environment block from each run; compare only runs with the same
environment and partition count.

<!-- run results appended here -->

