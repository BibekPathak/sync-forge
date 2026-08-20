# Customer Deployment: Acme Corp

A worked example of deploying SyncForge for a real (fictional) customer. This is
the "customer-specific deployment" angle: SyncForge isn't just a system — it's
a solution configured for how *this* business operates.

## The customer

**Acme Corp** is a mid-size company with two teams that each live in their own
CRM:

- **Sales** uses **Salesforce** as the system of record for accounts and
  opportunities.
- **Support** uses **HubSpot** to manage contacts and support metadata.

Their requirement:

> Customer records must synchronize bidirectionally. Salesforce owns account
> information, HubSpot owns support metadata, and **neither system can lose
> updates**.

Acme needs: no lost updates, deterministic conflict rules (Salesforce wins on
account fields, HubSpot wins on support fields), tolerance for Salesforce/HubSpot
outages, and a durable audit of every change.

## The SyncForge configuration

### Topology

```
Acme Corp
│
├── Salesforce  ── account / customer data (system of record for accounts)
│
├── HubSpot     ── support / contact metadata (system of record for support)
│
└── SyncForge
     ├── Field mapping        (canonical "customer" model)
     ├── Conflict policies    (field_merge: per-field winner)
     ├── Rate limits          (respect each provider's API caps)
     ├── Reconciliation       (auto sweep repairs drift)
     └── Audit                (every apply + every operator action)
```

### Canonical model

SyncForge maps both CRMs onto one canonical `customer` entity:

| Canonical field | Salesforce field | HubSpot field | Owner |
|---|---|---|---|
| `first_name` | `first_name` | `firstName` | — |
| `last_name` | `last_name` | `lastName` | — |
| `email` | `email` | `emailAddress` | — |
| `phone` | `phone` | `phoneNumber` | — |
| `company` | `company` | `organization` | — |

### Bidirectional policies

Two sync policies, one per direction:

| Policy | Source → Dest | Conflict strategy | Delete policy |
|---|---|---|---|
| `salesforce → hubspot` | Salesforce → HubSpot | `field_merge` | `propagate` |
| `hubspot → salesforce` | HubSpot → Salesforce | `field_merge` | `propagate` |

`field_merge` is the key choice: a concurrent edit on `company` in Salesforce
and `phone` in HubSpot **both** survive (per-field winner), because neither
system should lose an update.

### Rate limits

The connector registry enforces each provider's documented API cap
(Salesforce 100 req/min, HubSpot 50 req/min) client-side, so Acme's bursty
backfills don't trigger provider throttles.

### Reconciliation

An auto-mode reconcile sweep runs over Salesforce, repairing drift/missed/
deleted/missing records. Acme's delete policy is `propagate`, so an external
delete is respected (not resurrected).

### Audit

Every destination write is recorded in `sync_operations`, and every operator
action (conflict resolution, DLQ replay, reconcile apply/dismiss) in
`audit_log` — Acme's compliance team can answer "who changed what, when."

## Deploying Acme

`scripts/customer-demo.sh` provisions SyncForge exactly as above and exercises
the happy path:

1. Bring up the stack + seed the Acme tenant.
2. Create a customer in Salesforce → propagates to HubSpot.
3. Edit HubSpot support metadata → propagates back to Salesforce (bidirectional).
4. Verify both systems agree, no duplicate records, and the audit trail shows
   the writes.

See [operations-runbook.md](operations-runbook.md) for day-2 procedures.

## Why this is the FDE story

This configuration demonstrates the actual value: a customer with a messy
two-CRM problem gets a system that is **deployed and operated inside their
environment** — field mapping, conflict rules, rate limits, reconciliation, and
audit — rather than a generic "distributed system" demo.
