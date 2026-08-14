#!/usr/bin/env bash
# SyncForge demo script (Phase 1: foundation).
# Brings up the stack and exercises: tenant seed, connections API, and a
# signed provider webhook flowing into the ingestion gateway.
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE="docker compose -f deploy/compose/docker-compose.yml"
API=http://localhost:8080
KEY="sfk_acme_dev"
SF=http://localhost:9081

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

log "Starting SyncForge stack"
$COMPOSE up -d
# Recreate api/engine so idempotent seeding re-runs (integration tests may have
# truncated the dev database).
$COMPOSE restart api engine >/dev/null
echo "Waiting for API to become healthy..."
for i in $(seq 1 60); do
  if curl -fsS "$API/health" >/dev/null 2>&1; then break; fi
  sleep 2
done
curl -fsS "$API/health" | python3 -m json.tool

log "Tenants (bootstrap key)"
curl -fsS -H "X-Bootstrap-Key: syncforge-admin-dev" "$API/api/v1/tenants" | python3 -m json.tool

log "Connections for Acme (API key)"
curl -fsS -H "X-API-Key: $KEY" "$API/api/v1/connections" | python3 -m json.tool

log "Simulators seeded records"
echo -n "Salesforce: "; curl -fsS "$SF/api/v1/customers?limit=1" | python3 -c "import sys,json;d=json.load(sys.stdin);print(len(d['records']),'page of',d.get('has_more'),'more')"
echo -n "HubSpot:    "; curl -fsS "http://localhost:9082/api/v1/contacts?limit=1" | python3 -c "import sys,json;d=json.load(sys.stdin);print(len(d['records']),'page of',d.get('has_more'),'more')"

log "Emitting a webhook: update a Salesforce record"
ID=$(curl -fsS "$SF/api/v1/customers?limit=1" | python3 -c "import sys,json;print(json.load(sys.stdin)['records'][0]['id'])")
BEFORE=$(curl -fsS "http://localhost:9082/api/v1/contacts?limit=1000" | python3 -c "import sys,json;print(len(json.load(sys.stdin)['records']))")
curl -fsS -X PATCH "$SF/api/v1/customers/$ID" -H "Content-Type: application/json" -d '{"email":"demo@example.com"}' >/dev/null
echo "Waiting for the pipeline (webhook -> Redpanda -> worker -> HubSpot)..."
sleep 4

log "Ingested source events (durable, signed, idempotent)"
docker exec syncforge-postgres-1 psql -U postgres -d syncforge -c \
  "SELECT source, entity_type, entity_id, event_type, source_version, status FROM source_events ORDER BY received_at;"

log "Canonical records (provider-id mapping, source versions)"
docker exec syncforge-postgres-1 psql -U postgres -d syncforge -c \
  "SELECT entity_id, version, provider_ids, fields->>'email' AS email FROM canonical_records;"

log "HubSpot contacts (before=$BEFORE after=$(curl -fsS 'http://localhost:9082/api/v1/contacts?limit=1000' | python3 -c "import sys,json;print(len(json.load(sys.stdin)['records']))"))"
curl -fsS "http://localhost:9082/api/v1/contacts?limit=1000" | python3 -c "
import sys,json
recs=json.load(sys.stdin)['records']
demo=[c for c in recs if c.get('emailAddress')=='demo@example.com']
print('synced record:', demo[0]['contact_id'], demo[0]['emailAddress'] if demo else 'NOT FOUND')
"

log "Dashboard:  http://localhost:3001"
log "Prometheus: http://localhost:9090"
log "Grafana:    http://localhost:3000 (admin/admin)"
log "Demo complete."
