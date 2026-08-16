#!/usr/bin/env bash
# SyncForge production-scale benchmark.
# Brings up the full stack (real Redpanda + Postgres + worker), resets sync
# state, seeds the simulators, then drives signed webhooks through the real
# network path at increasing concurrency, reporting throughput + latency and
# sampling the engine's sync_* metrics between runs.
#
# Usage: scripts/bench.sh [events_per_run] [max_concurrency]
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE="docker compose -f deploy/compose/docker-compose.yml"
API=http://localhost:8080
ENGINE=http://localhost:8081
EVENTS="${1:-2000}"
MAX_CONC="${2:-64}"

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

log "Starting SyncForge stack"
$COMPOSE up -d
$COMPOSE restart api engine sim-salesforce sim-hubspot >/dev/null
docker exec syncforge-postgres-1 psql -U postgres -d syncforge -c \
  "TRUNCATE canonical_records, source_events, processed_events, outbound_writes,
           retry_queue, dead_letter, conflicts RESTART IDENTITY CASCADE;" >/dev/null

echo "Waiting for API to become healthy..."
for i in $(seq 1 60); do
  if curl -fsS "$API/health" >/dev/null 2>&1; then break; fi
  sleep 2
done
curl -fsS "$API/health" >/dev/null

# Sample a sync counter from the engine's Prometheus endpoint.
metric() { # metric_name
  curl -fsS "$ENGINE/metrics" 2>/dev/null | awk -v m="$1" '$0 ~ "^"m"{" { split($0,a," "); gsub(/[{}]/,"",a[1]); print a[2]; exit }'
}

log "Seeding simulators with $EVENTS records"
curl -fsS -X POST "http://localhost:9081/admin/seed" -H "Content-Type: application/json" -d "{\"count\": $EVENTS}" >/dev/null
curl -fsS -X POST "http://localhost:9082/admin/seed" -H "Content-Type: application/json" -d "{\"count\": $EVENTS}" >/dev/null

# Lift the providers' rate limits so the benchmark measures the pipeline, not
# the simulators' documented per-minute caps (compose defaults SF 100 / HS 50).
curl -fsS -X POST "http://localhost:9081/admin/faults" -H "Content-Type: application/json" \
  -d '{"rate_limit_per_min": 100000}' >/dev/null
curl -fsS -X POST "http://localhost:9082/admin/faults" -H "Content-Type: application/json" \
  -d '{"rate_limit_per_min": 100000}' >/dev/null

log "Benchmark: $EVENTS events/run, concurrency 1 -> $MAX_CONC"
printf '%-10s %-12s %-12s %-12s %-12s\n' "conc" "accepted" "ev/s" "p95(ms)" "p99(ms)"
for CONC in 1 2 4 8 16 32 "$MAX_CONC"; do
  OUT=$(go run ./cmd/loadgen -n "$EVENTS" -c "$CONC" -source salesforce -url "$API" 2>&1 | tail -1)
  # OUT: sent=N accepted=N rejected=N errors=N elapsed=... throughput=... ev/s p50=.. p95=.. p99=..
  ACC=$(echo "$OUT" | grep -oE 'accepted=[0-9]+' | cut -d= -f2)
  EVS=$(echo "$OUT" | grep -oE 'throughput=[0-9.]+' | cut -d= -f2)
  P95=$(echo "$OUT" | grep -oE 'p95=[0-9.]+' | cut -d= -f2)
  P99=$(echo "$OUT" | grep -oE 'p99=[0-9.]+' | cut -d= -f2)
  printf '%-10s %-12s %-12s %-12s %-12s\n' "$CONC" "$ACC" "$EVS" "$P95" "$P99"

  # Give the worker a moment to drain the in-flight page before the next run
  # so destinations converge and the metrics reflect a steady pipeline.
  sleep 3
done

# Let the retry queue drain so the final metrics reflect a converged pipeline.
echo "Waiting for the pipeline to settle (retry queue draining)..."
for i in $(seq 1 60); do
  N=$(docker exec syncforge-postgres-1 psql -U postgres -d syncforge -tAc \
    "SELECT count(*) FROM retry_queue" 2>/dev/null || echo 0)
  if [ "$N" = "0" ]; then break; fi
  sleep 2
done

log "Sync engine metrics after the run"
for M in sync_events_total sync_events_success_total sync_events_failed_total \
         sync_destination_writes_total sync_dlq_events_total sync_retry_scheduled_total \
         sync_duplicates_total sync_loop_events_prevented_total; do
  echo "  $M = $(metric "$M")"
done

log "Canonical records synced:"
docker exec syncforge-postgres-1 psql -U postgres -d syncforge -c \
  "SELECT count(*) AS canonical FROM canonical_records;"

log "Benchmark complete."
