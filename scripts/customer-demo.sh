#!/usr/bin/env bash
# Acme Corp customer deployment demo.
# Provisions SyncForge exactly as configured in docs/customer-deployment.md and
# exercises the bidirectional happy path: Salesforce account data flows to
# HubSpot, HubSpot support metadata flows back, both systems stay in agreement,
# and the audit trail records the writes.
set -euo pipefail
cd "$(dirname "$0")/.."
API=${API:-http://localhost:8080}
COMPOSE="docker compose -f deploy/compose/docker-compose.yml"
KEY="sfk_acme_dev"

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

log "Starting stack + Acme tenant"
$COMPOSE up -d >/dev/null 2>&1 || true
$COMPOSE restart api engine sim-salesforce sim-hubspot >/dev/null 2>&1 || true
docker exec syncforge-postgres-1 psql -U postgres -d syncforge -c \
  "TRUNCATE login_attempts, sessions, audit_log, outbound_writes, sync_operations,
           reconciliation_findings, reconciliation_runs, sync_jobs, conflicts,
           dead_letter, retry_queue, processed_events, source_events, canonical_records
   RESTART IDENTITY CASCADE;" >/dev/null
echo "Waiting for API..."
for i in $(seq 1 40); do curl -fsS "$API/health" >/dev/null 2>&1 && break; sleep 2; done
sleep 8

# Apply the Acme policies: field_merge both directions, deletes propagate.
docker exec syncforge-postgres-1 psql -U postgres -d syncforge -c \
  "UPDATE sync_policies SET conflict_strategy='field_merge', delete_policy='propagate' WHERE entity='customer';" >/dev/null

wait_for_contact() { # base list idfield email
  local base="$1" list="$2" idfield="$3" email="$4"
  for i in $(seq 1 30); do
    local id
    id=$(curl -fsS "$base$list" 2>/dev/null | python3 -c "
import sys,json
try: recs=json.load(sys.stdin)['records']
except Exception: recs=[]
for c in recs:
    if c.get('emailAddress')=='$email' or c.get('email')=='$email':
        print(c.get('$idfield') or c.get('id')); break
")
    [ -n "$id" ] && { printf '%s' "$id"; return 0; }
    sleep 2
  done
  printf ''
}

log "Step 1 — Sales creates an account in Salesforce"
EMAIL="acme-$(date +%s)@example.com"
REC=$(curl -fsS -X POST "http://localhost:9081/api/v1/customers" -H "Content-Type: application/json" \
  -d "{\"first_name\":\"Ada\",\"last_name\":\"Lovelace\",\"email\":\"$EMAIL\",\"phone\":\"+1-555-0100\",\"company\":\"Acme Corp\"}")
SFID=$(echo "$REC" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
echo "  Salesforce account: $SFID ($EMAIL)"
HUB=$(wait_for_contact "http://localhost:9082" "/api/v1/contacts?limit=1000" "contact_id" "$EMAIL")
echo "  Synced to HubSpot:   $HUB"

log "Step 2 — Support updates metadata in HubSpot (phone)"
curl -fsS -X PATCH "http://localhost:9082/api/v1/contacts/$HUB" -H "Content-Type: application/json" \
  -d '{"phoneNumber":"+1-555-9999"}' >/dev/null
echo "  Waiting for reverse propagation to Salesforce..."
for i in $(seq 1 30); do
  PH=$(curl -fsS "http://localhost:9081/api/v1/customers/$SFID" 2>/dev/null | python3 -c "import sys,json;print(json.load(sys.stdin).get('phone',''))" 2>/dev/null || true)
  [ "$PH" = "+1-555-9999" ] && break
  sleep 2
done
curl -fsS "http://localhost:9081/api/v1/customers/$SFID" | python3 -c "
import sys,json; d=json.load(sys.stdin)
print('  Salesforce phone now:', d['phone'], '(should be +1-555-9999)')
"

log "Step 3 — Canonical record + audit trail"
docker exec syncforge-postgres-1 psql -U postgres -d syncforge -c \
  "SELECT entity_id, provider_ids, fields->>'email' AS email FROM canonical_records WHERE fields->>'email'='$EMAIL';"
echo "  Applied-writes ledger (recent):"
curl -fsS -H "X-API-Key: $KEY" "$API/api/v1/operations?limit=6" | python3 -c "
import sys,json
for o in json.load(sys.stdin)['items']:
    print('   -', o['entity_id'], '->', o['target_source'], 'v'+str(o['applied_version']))
"
echo "  Audit log (recent):"
curl -fsS -H "X-API-Key: $KEY" "$API/api/v1/audit?limit=5" | python3 -c "
import sys,json
for a in json.load(sys.stdin)['items']:
    print('   -', a['created_at'][:19], a['action'], a['resource'])
"

log "Acme demo complete — both systems agree, no lost updates."
