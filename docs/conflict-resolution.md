# Conflict Resolution

How SyncForge detects concurrent edits across sources and resolves them per
policy.

## Detection

`canonical_records.field_provenance` records, per field, the last writer:
`{source, version, occurred_at, priority}`. When an incoming event changes a
field that a *different* source last wrote, that field is a concurrent edit
(`internal/conflict.Detect`).

A field does **not** conflict when:

- the incoming source is the same as the last writer (it is simply editing its
  own data — ordering rules apply, not conflicts), or
- the incoming value equals the stored value (no effective difference).

## Strategies

Each sync policy selects one strategy via `conflict_strategy`:

| Strategy | Rule |
|---|---|
| `last_write_wins` | the side with the later `occurred_at` wins; ties break to the higher-priority source |
| `source_priority` | the side with the lower priority number wins; ties break by occurrence time |
| `field_merge` | each conflicted field resolves independently (per-field winner by occurrence time); merged result is written to both sides |
| `manual` | never auto-wins; the event is parked as `CONFLICT_PENDING` for an operator |

Every resolution updates `field_provenance` so the winning writer owns the
field going forward.

## Manual resolution (operator API)

With the `manual` strategy (or any pending conflict), the conflict is parked
with both sides' payload snapshots. An operator resolves via
`POST /api/v1/conflicts/{id}/resolve` (choose side `a` or `b`) or dismisses via
`POST /api/v1/conflicts/{id}/dismiss`.

The chosen side is applied through the worker as a deterministic event
(`resolve:<conflict-id>`), so it is exactly-once even across retries. The
resolution is recorded on the conflict row (`resolved_by`, `resolved_at`) and
in the audit log.

## Idempotency

Conflicts are unique on `(tenant, entity, source pair, versions)`: a redelivery
of the same logical pair is a no-op. A previously resolved/dismissed conflict
is never re-applied — the operator's decision stands.

## Where resolution happens

Resolution runs inside the idempotent worker apply path
(`internal/syncworker`), so conflict resolution composes with ordering, echo
loop-prevention, and the exactly-once destination write guarantee.

## Metrics / audit

- `sync_conflicts_detected_total`, `sync_conflicts_resolved_total`
- Every resolve/dismiss is written to `audit_log` (actor, side, winner).

---

See [diagrams.md](diagrams.md) for the architecture diagrams.
