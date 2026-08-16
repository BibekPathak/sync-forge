#!/usr/bin/env bash
# SyncForge demo script (full-stack walkthrough).
# Brings up the stack and exercises the whole feature set end to end:
#   1. seeded tenant + connections, 2. per-user login (RBAC),
#   3. signed webhook -> pipeline -> HubSpot (bidirectional propagation),
#   4. reconciliation sweep + findings, 5. audit log + writes ledger + DLQ reads.
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE="docker compose -f deploy/compose/docker-compose.yml"
API=http://localhost:8080
KEY="sfk_acme_dev"
SF=http://localhost:9081
USER_EMAIL="admin@acme.dev"
USER_PASS="syncforge-demo"

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

# wait_for_contact polls the provider until a record with the given email and
# id-field appears (the pipeline settles asynchronously: webhook -> Redpanda ->
# worker -> destination). The full stack has real latency, so we poll instead
# of sleeping a fixed amount.
wait_for_contact() { # base_url list_path id_field email
  local base="$1" list="$2" idfield="$3" email="$4"
  for i in $(seq 1 30); do
    local id
    id=$(curl -fsS "$base$list" 2>/dev/null | python3 -c "
import sys,json
try:
    recs=json.load(sys.stdin)['records']
except Exception:
    recs=[]
for c in recs:
    if c.get('emailAddress')=='$email' or c.get('email')=='$email':
        print(c.get('$idfield') or c.get('id')); break
")
    if [ -n "$id" ]; then printf '%s' "$id"; return 0; fi
    sleep 2
  done
  printf ''
}

log "Starting SyncForge stack"
$COMPOSE up -d
# Recreate api/engine so idempotent seeding re-runs, and reset the in-memory
# simulators + sync state so the demo is deterministic regardless of prior runs.
$COMPOSE restart api engine sim-salesforce sim-hubspot >/dev/null
docker exec syncforge-postgres-1 psql -U postgres -d syncforge -c \
  "TRUNCATE canonical_records, source_events, processed_events, outbound_writes,
           retry_queue, dead_letter, conflicts RESTART IDENTITY CASCADE;" >/dev/null
echo "Waiting for API to become healthy..."
for i in $(seq 1 60); do
  if curl -fsS "$API/health" >/dev/null 2>&1; then break; fi
  sleep 2
done
curl -fsS "$API/health" | python3 -m json.tool

log "Tenants (ADMIN API key)"
curl -fsS -H "X-API-Key: $KEY" "$API/api/v1/tenants" | python3 -m json.tool

log "Per-user login (RBAC): POST /api/v1/auth/login"
LOGIN=$(curl -fsS -X POST "$API/api/v1/auth/login" -H "Content-Type: application/json" \
  -d "{\"tenant_slug\":\"acme\",\"email\":\"$USER_EMAIL\",\"password\":\"$USER_PASS\"}")
UTOKEN=$(echo "$LOGIN" | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])")
echo "$LOGIN" | python3 -c "import sys,json;d=json.load(sys.stdin);print('logged in as', d['user']['email'], 'role', d['user']['role'], '(token', d['token'][:12]+'...', ')')"
echo "Reading connections with the user session token:"
curl -fsS -H "Authorization: Bearer $UTOKEN" "$API/api/v1/connections" | python3 -c "import sys,json;print('  connections:', len(json.load(sys.stdin)['connections']))"

log "Connections for Acme (API key)"
curl -fsS -H "X-API-Key: $KEY" "$API/api/v1/connections" | python3 -m json.tool

log "Simulators seeded records"
echo -n "Salesforce: "; curl -fsS "$SF/api/v1/customers?limit=1" | python3 -c "import sys,json;d=json.load(sys.stdin);print(len(d['records']),'page of',d.get('has_more'),'more')"
echo -n "HubSpot:    "; curl -fsS "http://localhost:9082/api/v1/contacts?limit=1" | python3 -c "import sys,json;d=json.load(sys.stdin);print(len(d['records']),'page of',d.get('has_more'),'more')"

log "Creating a fresh customer in Salesforce (webhook -> pipeline -> HubSpot)"
EMAIL="demo-$(date +%s)@example.com"
REC=$(curl -fsS -X POST "$SF/api/v1/customers" -H "Content-Type: application/json" -d "{\"first_name\":\"Demo\",\"last_name\":\"Sync\",\"email\":\"$EMAIL\",\"phone\":\"+1-555-0000\",\"company\":\"Acme\"}")
ID=$(echo "$REC" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
echo "created Salesforce customer: $ID ($EMAIL)"
echo "Waiting for the pipeline (webhook -> Redpanda -> worker -> HubSpot)..."
HUB=$(wait_for_contact "http://localhost:9082" "/api/v1/contacts?limit=1000" "contact_id" "$EMAIL")
if [ -z "$HUB" ]; then echo "ERROR: customer never reached HubSpot"; exit 1; fi
echo "synced record: $HUB ($EMAIL)"

log "Ingested source events (durable, signed, idempotent)"
docker exec syncforge-postgres-1 psql -U postgres -d syncforge -c \
  "SELECT source, entity_type, entity_id, event_type, source_version, status FROM source_events ORDER BY received_at DESC LIMIT 10;"

log "Canonical records (provider-id mapping, source versions)"
docker exec syncforge-postgres-1 psql -U postgres -d syncforge -c \
  "SELECT entity_id, version, provider_ids, fields->>'email' AS email FROM canonical_records WHERE fields->>'email'='$EMAIL';"

log "Bidirectional: edit the HubSpot contact -> propagates back to Salesforce"
curl -fsS -X PATCH "http://localhost:9082/api/v1/contacts/$HUB" -H "Content-Type: application/json" -d '{"phoneNumber":"+1-555-9999"}' >/dev/null
echo "Waiting for reverse propagation..."
for i in $(seq 1 30); do
  PH=$(curl -fsS "$SF/api/v1/customers/$ID" 2>/dev/null | python3 -c "import sys,json;print(json.load(sys.stdin).get('phone',''))" 2>/dev/null || true)
  if [ "$PH" = "+1-555-9999" ]; then break; fi
  sleep 2
done
curl -fsS "$SF/api/v1/customers/$ID" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print('Salesforce', d['id'], 'phone now:', d['phone'], '(should be +1-555-9999)')
"
echo "Loop prevention check (echo webhooks recognized & dropped):"
curl -fsS "http://localhost:8081/metrics" | grep -E '^sync_loop_events_prevented_total' | head -1

log "Conflicts: switch to manual strategy, then a concurrent edit parks a CONFLICT_PENDING"
docker exec syncforge-postgres-1 psql -U postgres -d syncforge -c \
  "UPDATE sync_policies SET conflict_strategy='manual' WHERE entity='customer' AND source='hubspot';" >/dev/null
CONF_EMAIL="conflict-$(date +%s)@example.com"
REC=$(curl -fsS -X POST "$SF/api/v1/customers" -H "Content-Type: application/json" -d "{\"first_name\":\"Conf\",\"last_name\":\"Torn\",\"email\":\"$CONF_EMAIL\",\"phone\":\"+1-555-0000\",\"company\":\"Acme\"}")
CFID=$(echo "$REC" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
echo "created Salesforce customer: $CFID ($CONF_EMAIL)"
echo "Waiting for initial propagation to HubSpot..."
CHUB=$(wait_for_contact "http://localhost:9082" "/api/v1/contacts?limit=1000" "contact_id" "$CONF_EMAIL")
if [ -z "$CHUB" ]; then echo "ERROR: conflict customer never reached HubSpot"; exit 1; fi
echo "hubspot copy: $CHUB"

# Concurrent edit: Salesforce rewrites last_name, then HubSpot rewrites the
# same field before the Salesforce write settles -> a field-level conflict.
curl -fsS -X PATCH "$SF/api/v1/customers/$CFID" -H "Content-Type: application/json" -d '{"last_name":"Side-A"}' >/dev/null
curl -fsS -X PATCH "http://localhost:9082/api/v1/contacts/$CHUB" -H "Content-Type: application/json" -d '{"lastName":"Side-B"}' >/dev/null
echo "Waiting for the concurrent edits to collide..."
for i in $(seq 1 30); do
  CONFLICTS=$(curl -fsS -H "X-API-Key: $KEY" "$API/api/v1/conflicts?status=CONFLICT_PENDING&limit=10")
  CN=$(echo "$CONFLICTS" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])")
  if [ "$CN" -gt 0 ]; then break; fi
  sleep 2
done
CONFLICTS=$(curl -fsS -H "X-API-Key: $KEY" "$API/api/v1/conflicts?status=CONFLICT_PENDING&limit=10")
echo "$CONFLICTS" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print('  pending conflicts:', d['count'])
for c in d['items'][:5]:
    print('   -', c['id'], c['entity_id'], c['source_a'], 'vs', c['source_b'], '['+c['status']+']')
"
CID=$(echo "$CONFLICTS" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d['items'][0]['id'] if d['items'] else '')")
if [ -n "$CID" ]; then
  echo "Resolving conflict $CID (side b = the HubSpot edit)..."
  curl -fsS -X POST "$API/api/v1/conflicts/$CID/resolve" -H "Content-Type: application/json" -H "X-API-Key: $KEY" \
    -d '{"side":"b"}' | python3 -m json.tool
  echo "Conflict resolution audited:"
  curl -fsS -H "X-API-Key: $KEY" "$API/api/v1/audit?action=conflict.resolve&limit=5" | python3 -c "
import sys,json
for a in json.load(sys.stdin)['items']:
    print('   -', a['actor'], a['action'], a['resource_id'], a.get('metadata',{}))
"
  # Restore the demo policy for the remainder of the script.
  docker exec syncforge-postgres-1 psql -U postgres -d syncforge -c \
    "UPDATE sync_policies SET conflict_strategy='field_merge' WHERE entity='customer' AND source='hubspot';" >/dev/null
fi

log "Reconciliation: run a sweep over Salesforce and wait for it to complete"
RUN=$(curl -fsS -X POST "$API/api/v1/reconciliations" -H "Content-Type: application/json" -H "X-API-Key: $KEY" \
  -d '{"source":"salesforce","mode":"manual"}')
RUN_ID=$(echo "$RUN" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
echo "reconciliation run: $RUN_ID"
for i in $(seq 1 30); do
  ST=$(curl -fsS -H "X-API-Key: $KEY" "$API/api/v1/reconciliations/$RUN_ID" | python3 -c "import sys,json;print(json.load(sys.stdin)['status'])")
  if [ "$ST" = "completed" ] || [ "$ST" = "failed" ]; then break; fi
  sleep 2
done
curl -fsS -H "X-API-Key: $KEY" "$API/api/v1/reconciliations/$RUN_ID" | python3 -m json.tool
echo "Findings (if any diverged):"
curl -fsS -H "X-API-Key: $KEY" "$API/api/v1/reconciliations/$RUN_ID/findings?limit=20" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print('  findings:', d['count'])
for f in d['items'][:5]:
    print('   -', f['kind'], f['provider_id'], '->', f['direction'], '['+f['status']+']')
"

log "Audit trail (operator + security actions, incl. the login above)"
curl -fsS -H "X-API-Key: $KEY" "$API/api/v1/audit?limit=10" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print('  events:', d['count'])
for a in d['items'][:10]:
    print('   -', a['created_at'][:19], a['actor'], a['action'], a['resource'], a.get('resource_id',''))
"

log "Applied-writes ledger (every destination mutation)"
curl -fsS -H "X-API-Key: $KEY" "$API/api/v1/operations?limit=10" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print('  writes:', d['count'])
for o in d['items'][:10]:
    print('   -', o['created_at'][:19], o['entity_id'], '->', o['target_source'], 'v'+str(o['applied_version']), o['fingerprint'][:16])
"

log "Dead-letter queue (should be empty in a healthy demo)"
curl -fsS -H "X-API-Key: $KEY" "$API/api/v1/dlq?limit=10" | python3 -c "
import sys,json
d=json.load(sys.stdin)
print('  dlq entries:', d['count'])
"

log "Dashboard:  http://localhost:3001"
log "Prometheus: http://localhost:9090"
log "Grafana:    http://localhost:3000 (admin/admin)"
log "Demo complete."
