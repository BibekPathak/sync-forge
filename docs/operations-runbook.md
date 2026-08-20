# Operations Runbook

Procedures for operating a SyncForge deployment.

## Stack

```bash
make up          # bring up the full compose stack (api, engine, sims, broker, obs)
make down        # tear it down
make logs        # follow all service logs
```

| Service | URL | Purpose |
|---|---|---|
| API | http://localhost:8080 | REST control plane + webhook gateway |
| Dashboard | http://localhost:3001 | operational console (read-only) |
| Prometheus | http://localhost:9090 | metrics |
| Grafana | http://localhost:3000 | dashboards (admin/admin) |
| Jaeger | http://localhost:16686 | traces |
| Salesforce sim | http://localhost:9081 | external system A |
| HubSpot sim | http://localhost:9082 | external system B |
| OIDC sim | http://localhost:9083 | SSO identity provider |

## Health

- `GET /health` returns `{status, checks:{database, redis}}`; non-`ok` →
  `503`.
- Engine health: `GET http://localhost:8081/health`.

## Common operations

### Find stuck events

```bash
# Events that are queued, failed, or dead-lettered
curl -H "X-API-Key: $KEY" "$API/api/v1/sync-events?limit=100" \
  | jq '.events[] | select(.status != "processed")'
```

### Replay the dead-letter queue

```bash
curl -X POST -H "X-API-Key: $KEY" "$API/api/v1/dlq/{id}/retry"   # replay one
curl -X POST -H "X-API-Key: $KEY" "$API/api/v1/dlq/{id}/discard" # drop one
```

### Resolve a conflict

```bash
curl -X POST -H "X-API-Key: $KEY" \
  -H "Content-Type: application/json" \
  -d '{"side":"a"}' "$API/api/v1/conflicts/{id}/resolve"
```

### Run a reconciliation sweep

```bash
curl -X POST -H "X-API-Key: $KEY" \
  -H "Content-Type: application/json" \
  -d '{"source":"salesforce","mode":"manual"}' "$API/api/v1/reconciliations"
# poll the run, then apply/dismiss findings:
curl -X POST -H "X-API-Key: $KEY" "$API/api/v1/reconciliations/{run}/findings/{fid}/apply"
curl -X POST -H "X-API-Key: $KEY" "$API/api/v1/reconciliations/{run}/findings/{fid}/dismiss"
```

### Sign a user out everywhere

```bash
curl -X POST -H "X-API-Key: $KEY" "$API/api/v1/users/{id}/revoke-sessions"
```

### Check who is logged in

```bash
curl -H "X-API-Key: $KEY" "$API/api/v1/sessions"
```

## Failure scenarios

| Symptom | Check | Action |
|---|---|---|
| Events stuck `failed` | retry queue growing? | provider down or rate-limited; recover provider, retries drain |
| Events in DLQ | `GET /api/v1/dlq` | permanent failure (schema/auth); inspect error_class, retry or discard |
| Conflict pile-up | `sync_conflicts_detected_total` rising | strategy `manual`; resolve via API |
| Divergence | reconcile findings | run a sweep, review/apply findings |
| Login lockout | `login_attempts` | wait for window or clear via ADMIN password reset |
| Traces missing | Jaeger empty | check `OTEL_EXPORTER_OTLP_ENDPOINT` + collector |

## Load / chaos

```bash
./scripts/bench.sh 100000 32 1   # production-scale benchmark (see benchmarking.md)
./scripts/chaos-demo.sh          # failure demonstration (probabilistic faults)
./scripts/demo.sh                # full-stack walkthrough
```
