# Consistency Model

How SyncForge orders, delivers, and applies events, and what it does and does
not guarantee.

## The one-sentence answer

> **At-least-once event delivery with idempotent, exactly-once-effect
> destination writes under the engine's supported failure model.**

SyncForge does **not** provide exactly-once *delivery*. Distributed systems
cannot reliably guarantee that a broker never redelivers a message. Instead,
SyncForge makes redelivery harmless: every logical event is applied **exactly
once** at the destination, no matter how many times it is delivered.

## Delivery: at-least-once

- The event bus is Redpanda. The consumer uses **manual offset commits**: a
  message's offset is committed only after its handler returns `nil`.
- If the worker crashes mid-apply, the offset is not committed and Redpanda
  redelivers the message on the next start/rebalance.
- Therefore a single logical event may be **delivered more than once**.

## Effect: exactly-once (idempotency claim)

`processed_events (tenant_id, source, event_id)` is the **idempotency log**. The
worker claims a row before doing any work:

```
INSERT ... ON CONFLICT DO NOTHING
  -> if a row already exists, this delivery is a duplicate -> no-op
```

- The claim is made **before** applying the event, so a crash between claim and
  apply leaves the claim; the retry path explicitly **releases** the claim on
  failure so the event can be retried.
- Duplicate deliveries collapse into one destination mutation. A benchmark
  delivers the same event 100 times and observes exactly one destination write.

## Ordering

- The partition key is `tenant:entity_type:entity_id`, so all events for one
  entity land in one partition and are processed in order.
- Per-source version checks (`canonical_records.source_versions[source]`) drop
  out-of-order events: an older version arriving late is discarded, so an old
  event cannot overwrite a newer one.
- Ordering is **per-entity**, not global. Different entities are independent.

## Loop prevention

`outbound_writes` stores the fingerprint of what SyncForge last wrote to each
destination. An incoming event that normalizes to the same fingerprint is
recognized as SyncForge's own echo and dropped, so A→B→A→B… oscillation does
not occur. Deletes are additionally guarded by the canonical tombstone.

## Conflicts

`canonical_records.field_provenance` records the last writer of every field. An
incoming event that changes a field another source last wrote is a concurrent
edit, detected and resolved per the policy's `conflict_strategy`:
`last_write_wins`, `source_priority`, `field_merge`, or `manual` (parked for an
operator). Every resolution is durable and idempotent (unique on the source/version
pair).

## The residual window

There is a documented tradeoff: if the worker claims an event, applies it, and
crashes **before** persisting the canonical record, the claim exists but the
destination write may not. On redelivery the event is skipped as "already
processed" — a residual inconsistency window.

**How it is closed:** reconciliation (Phase 6) re-derives truth from the
provider and repairs drift/missed/deleted/missing records, so the residual
window does not become permanent data loss. This is the honest answer to "can a
crash lose an update?": a single crash can leave a stale destination for a
short window; reconciliation converges it.

## What the benchmark observes

Reliability counters from the load/chaos runs are recorded in
[benchmarking.md](benchmarking.md): duplicates suppressed, retries, DLQ
entries, loop-events prevented, and data-loss rate (received − eventually
canonical). In a healthy benchmark, data loss is 0 and duplicate suppression
accounts for all redeliveries.

## Summary table

| Property | Guarantee |
|---|---|
| Delivery | at-least-once |
| Destination effect | exactly-once (idempotency claim) |
| Ordering | per-entity (partition key) + version checks |
| Duplicate delivery | collapsed to one mutation |
| Loop prevention | outbound fingerprint + tombstone |
| Conflicts | detected + resolved per policy, durable |
| Crash residual window | bounded; closed by reconciliation |
