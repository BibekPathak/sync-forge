#!/usr/bin/env bash
# SyncForge failure demonstration.
#
# Drives events at the live stack while injecting a MIX of probabilistic faults
# into the destination provider (HubSpot) — duplicates, out-of-order, transient
# failures, rate limiting, latency — plus a mid-run worker restart. The system
# must converge with zero data loss despite all of it.
#
# This is the "measure -> break -> recover" story: a failure-injection mechanism
# that models probabilistic failures in external dependencies, and a validation
# of recovery behavior under stochastic faults.
#
# Usage: scripts/chaos-demo.sh [events] [seed]
set -euo pipefail
cd "$(dirname "$0")/.."
API=${API:-http://localhost:8080}
ENGINE=${ENGINE:-http://localhost:8081}
COMPOSE="docker compose -f deploy/compose/docker-compose.yml"
EVENTS="${1:-10000}"
SEED="${2:-42}"
LG=/tmp/syncforge-bench/loadgen
[ -x "$LG" ] || go build -o "$LG" ./cmd/loadgen

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

log "Starting stack"
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

# Lift the destination rate limit far above default so the *injected*
# rate-limit probability is the only throttle.
curl -fsS -X POST "http://localhost:9081/admin/faults" -H "Content-Type: application/json" \
  -d '{"rate_limit_per_min":100000}' >/dev/null || true
curl -fsS -X POST "http://localhost:9082/admin/faults" -H "Content-Type: application/json" \
  -d '{"rate_limit_per_min":100000}' >/dev/null || true

log "Injecting probabilistic faults into HubSpot (seed=$SEED)"
curl -fsS -X POST "http://localhost:9082/admin/faults" -H "Content-Type: application/json" -d "{
  \"seed\": $SEED,
  \"failure_rate\": 0.20,
  \"latency_min_ms\": 100,
  \"latency_max_ms\": 1500,
  \"duplicate_webhook_rate\": 0.10,
  \"out_of_order_rate\": 0.10,
  \"malformed_rate\": 0.02,
  \"rate_limit_probability\": 0.05,
  \"rate_limit_per_min\": 100000
}" | python3 -c "import sys,json;d=json.load(sys.stdin);print('  active faults:', {k:v for k,v in d.items() if v not in ('',0,False)})"

log "Driving $EVENTS events @ concurrency 32 (destination is faulted)"
OUT=$($LG -n "$EVENTS" -c 32 -source salesforce -url "$API" 2>&1 | tail -1)
echo "INGEST $OUT"
acc=$(echo "$OUT" | grep -oE 'accepted=[0-9]+' | cut -d= -f2)

log "Mid-run: restarting the worker while events are in flight"
$COMPOSE restart engine >/dev/null 2>&1
sleep 6

log "Waiting for recovery (processable events converge; malformed ones land in DLQ)..."
# Convergence target: canonical >= accepted - dead-lettered. Malformed-payload
# events legitimately DLQ (schema error), so they are not "lost" — they are
# parked for an operator.
for i in $(seq 1 500); do
  counts=$(docker exec syncforge-postgres-1 psql -U postgres -d syncforge -tAc \
    "SELECT (SELECT count(*) FROM canonical_records),(SELECT count(*) FROM dead_letter)" 2>/dev/null || echo "0|0")
  canon=$(echo "$counts" | cut -d'|' -f1)
  dlq=$(echo "$counts" | cut -d'|' -f2)
  [ $((canon + dlq)) -ge "$acc" ] && break
  sleep 2
done

log "Final state (FDE summary)"
src=$(docker exec syncforge-postgres-1 psql -U postgres -d syncforge -tAc "SELECT count(*) FROM source_events" 2>/dev/null || echo 0)
canon=$(docker exec syncforge-postgres-1 psql -U postgres -d syncforge -tAc "SELECT count(*) FROM canonical_records" 2>/dev/null || echo 0)
pending=$(docker exec syncforge-postgres-1 psql -U postgres -d syncforge -tAc "SELECT count(*) FROM source_events WHERE status='received'" 2>/dev/null || echo 0)
dlq=$(docker exec syncforge-postgres-1 psql -U postgres -d syncforge -tAc "SELECT count(*) FROM dead_letter" 2>/dev/null || echo 0)

M() { curl -fsS "$ENGINE/metrics" 2>/dev/null | awk -v m="$1" '$0 ~ "^"m"{" { n=split($0,a," "); sum+=a[n]+0 } END{print sum}'; }

# Data loss = events that are neither canonical nor in the DLQ. DLQ events are
# not lost: they are parked for operator replay.
dataloss=$((src - canon - dlq))
[ "$dataloss" -lt 0 ] && dataloss=0

echo ""
echo "----------------------------------------"
echo "  Events received (source_events):  $src"
echo "  Events eventually processed:      $canon"
echo "  Events dead-lettered (parked):    $dlq"
echo "  Data loss:                        $dataloss   (target: 0)"
echo "  Still pending (received):         $pending"
echo "  Duplicates suppressed:            $(M sync_duplicates_total)"
echo "  Loop-events prevented:            $(M sync_loop_events_prevented_total)"
echo "  Retries scheduled:                $(M sync_retry_scheduled_total)"
echo "  DLQ entries (engine counter):     $(M sync_dlq_events_total)"
echo "  Conflicts detected:               $(M sync_conflicts_detected_total)"
echo "  Destination writes:               $(M sync_destination_writes_total)"
echo "----------------------------------------"
