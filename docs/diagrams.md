# SyncForge Architecture Diagrams

Five diagrams that describe the system. Each has a **Mermaid** version (renders
on GitHub) and an **ASCII** fallback (portable anywhere).

1. [High-level architecture](#1-high-level-architecture)
2. [Event lifecycle](#2-event-lifecycle)
3. [Conflict resolution](#3-conflict-resolution)
4. [Failure / retry / DLQ](#4-failure--retry--dlq)
5. [Acme customer deployment](#5-acme-customer-deployment)

---

## 1. High-level architecture

```mermaid
flowchart LR
  subgraph External
    SF[Salesforce sim] -->|signed webhook| API
    HS[HubSpot sim] -->|signed webhook| API
    OIDC[OIDC IdP sim]
  end

  subgraph SyncForge
    API[cmd/api<br/>REST + webhook gateway]
    ING[ingestion processor]
    BUS[(Redpanda<br/>sync.events)]
    WRK[sync worker<br/>retry / reconcile]
    DB[(PostgreSQL<br/>RLS)]
    RED[(Redis<br/>throttle / cache)]
  end

  subgraph Observability
    PROM[Prometheus]
    GRA[Grafana]
    JAE[Jaeger]
  end

  API -->|source_events| ING
  ING --> BUS
  BUS --> WRK
  WRK --> DB
  API --> DB
  API --> RED
  WRK -->|create/update/delete| SF
  WRK -->|create/update/delete| HS
  API -->|ID token| OIDC
  WRK --> PROM
  API --> PROM
  PROM --> GRA
  WRK --> JAE
  API --> JAE
```

```
                Salesforce sim ──signed webhook──▶ cmd/api (REST + gateway)
                HubSpot sim    ──signed webhook──▶ cmd/api
                OIDC IdP sim   ──id token────────▶ cmd/api

   cmd/api ──source_events──▶ ingestion ──▶ Redpanda (sync.events) ──▶ sync worker
      │                          │                                  │
      ▼                          ▼                                  ▼
   PostgreSQL (RLS)          Redis (throttle)          create/update/delete ─▶ sims
      │
      └── Prometheus ── Grafana     Jaeger ◀── traces
```

---

## 2. Event lifecycle

```mermaid
sequenceDiagram
  participant P as Provider (SF)
  participant G as Gateway (api)
  participant SE as source_events
  participant I as Ingestion
  participant B as Redpanda
  participant W as Worker
  participant C as canonical_records
  participant D as Destination (HS)

  P->>G: signed webhook
  G->>SE: insert (status=received, dedup on event_id)
  I->>SE: claim pending (FOR UPDATE SKIP LOCKED)
  I->>B: publish (key = tenant:entity:entity_id)
  B->>W: deliver (manual offset commit)
  W->>W: claim processed_events (idempotency log)
  W->>W: version check + echo (fingerprint) check
  W->>W: conflict detect + resolve
  W->>D: create/update/delete
  W->>C: upsert canonical + source_versions + provider_ids
  W->>W: record outbound fingerprint + sync_operations ledger
```

```
Provider ─webhook─▶ Gateway ─▶ source_events ─▶ Ingestion ─▶ Redpanda ─▶ Worker
                                                                         │
   claim (idempotency) → version check → echo check → conflict resolve    │
                                                                         ▼
                                   destination (create/update/delete) ◀──┘
                                   canonical_records (fields, versions,
                                   provenance, provider ids)
                                   outbound_writes (fingerprint)
                                   sync_operations (ledger)
```

---

## 3. Conflict resolution

```mermaid
flowchart TD
  E[Incoming event] --> D{Changes a field<br/>another source last wrote?}
  D -- no --> OK[Apply normally]
  D -- yes --> C[Conflict detected]
  C --> S{policy.conflict_strategy}
  S -- last_write_wins --> L[later occurred_at wins]
  S -- source_priority --> R[lower priority number wins]
  S -- field_merge --> M[per-field winner, merged result]
  S -- manual --> P[Park CONFLICT_PENDING]
  P --> OP[Operator: POST /conflicts/id/resolve or dismiss]
  L --> W[Apply winner via worker, exactly-once]
  R --> W
  M --> W
  OP --> W
  W --> A[Update field_provenance + audit_log]
```

```
incoming event
   │
   ├─ same source / no effective change ─▶ apply normally
   └─ different source changed the field ─▶ CONFLICT
        │
        ├─ last_write_wins ─▶ later timestamp wins
        ├─ source_priority ─▶ lower priority number wins
        ├─ field_merge     ─▶ per-field winner, merged
        └─ manual          ─▶ park → operator resolve/dismiss
        │
        └─▶ worker applies winner exactly-once → updates provenance + audit
```

---

## 4. Failure / retry / DLQ

```mermaid
flowchart TD
  A[Worker apply fails] --> C{Classify error}
  C -- permanent (schema/auth) --> D[DLQ]
  C -- transient / rate-limit --> R[retry_queue + backoff]
  R --> A
  R -- attempts exhausted --> D
  D --> OP[Operator]
  OP -- POST /dlq/id/retry --> R
  OP -- POST /dlq/id/discard --> X[Discarded]
  D -- replay success --> RES[Resolved]
```

```
apply fails
  ├─ permanent (schema/auth) ─▶ DLQ ─▶ operator: retry | discard
  └─ transient / rate-limit  ─▶ retry_queue + exponential backoff
        └─ exhausted ─▶ DLQ ─▶ operator replay
```

---

## 5. Acme customer deployment

```mermaid
flowchart LR
  subgraph Acme
    SALES[Sales team] -->|accounts, sales data| SF[Salesforce]
    SUPP[Support team] -->|support metadata| HS[HubSpot]
  end

  SF <-->|bidirectional sync| SYNC[SyncForge]
  HS <-->|bidirectional sync| SYNC

  subgraph SyncForge config
    MAP[Field mapping:<br/>canonical customer model]
    POL[Conflict: field_merge<br/>both directions]
    RATE[Rate limits:<br/>SF 100/min, HS 50/min]
    REC[Reconciliation: auto,<br/>delete policy propagate]
    AUD[Audit: sync_operations + audit_log]
  end
```

```
   Sales team ── Salesforce        HubSpot ── Support team
                   │   ▲              ▲   │
                   ▼   │              │   ▼
                     SyncForge
        field mapping | field_merge both ways | rate limits
        | auto reconciliation (propagate deletes) | durable audit
```

---

See [architecture.md](architecture.md), [consistency-model.md](consistency-model.md),
[conflict-resolution.md](conflict-resolution.md), [failure-recovery.md](failure-recovery.md),
and [customer-deployment.md](customer-deployment.md) for the written detail.
