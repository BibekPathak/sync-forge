#!/usr/bin/env bash
# SyncForge production-scale benchmark harness.
#
# Measures, on the local Docker Compose stack:
#   - ingestion throughput: webhook -> source_events (ev/s, p50/p95/p99)
#   - end-to-end sync throughput: event -> canonical + destination converged
#   - reliability: success rate, duplicates suppressed, data loss, retries,
#     DLQ, conflicts
#   - resource usage: per-service CPU/memory (docker stats), Postgres + Redpanda
#     behavior, queue depth
#
# Results are labeled "local reference benchmark" and are NOT universal system
# throughput. Every run records environment + configuration so the identical
# workload can be re-run on larger infrastructure.
#
# Usage: scripts/bench.sh [events] [max_concurrency] [partitions] [sweep_events]
#   events          total webhooks for the headline run at peak concurrency
#                   (default 100000)
#   max_concurrency max loadgen concurrency (default 64)
#   partitions      topic partitions to benchmark (default 1)
#   sweep_events    events per concurrency-curve level (default 20000)
#
# The concurrency curve (1..max at sweep_events each) maps throughput vs.
# concurrency; the headline number is one full-size run at max concurrency.
#
# Output goes to stdout and a timestamped report under /tmp/syncforge-bench/.
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE="docker compose -f deploy/compose/docker-compose.yml"
API=${API:-http://localhost:8080}
ENGINE=${ENGINE:-http://localhost:8081}
EVENTS="${1:-100000}"
MAX_CONC="${2:-64}"
PARTITIONS="${3:-1}"
SWEEP_EVENTS="${4:-20000}"
REPORT_DIR="${REPORT_DIR:-/tmp/syncforge-bench}"
STAMP=$(date +%Y%m%d-%H%M%S)
REPORT="$REPORT_DIR/report-$STAMP.txt"
mkdir -p "$REPORT_DIR"

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
rpt() { printf '%s\n' "$*" | tee -a "$REPORT"; }

# ---- Environment + configuration recorder ------------------------------
record_env() {
  rpt "=== Environment ==="
  rpt "timestamp:        $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  rpt "os:               $(uname -sr)"
  rpt "cpu:              $(lscpu 2>/dev/null | awk -F: '/Model name/{print $2}' | xargs) ($(nproc) cores)"
  rpt "ram:              $(free -h | awk '/Mem:/{print $2}') total / $(free -h | awk '/Mem:/{print $7}') available"
  rpt "go:               $(go version 2>/dev/null | awk '{print $3}')"
  rpt "docker:           $(docker --version 2>/dev/null | awk '{print $3}')"
  rpt "postgres:         $(docker exec syncforge-postgres-1 psql -U postgres -tAc 'SHOW server_version' 2>/dev/null || echo '?')"
  rpt "redis:            $(docker exec syncforge-redis-1 redis-cli --version 2>/dev/null || echo '?')"
  rpt "redpanda:         $(docker exec syncforge-redpanda-1 rpk version 2>/dev/null | grep -oE 'v[0-9.]+' | head -1 || echo '?')"

  rpt ""
  rpt "=== Benchmark configuration ==="
  rpt "events:           $EVENTS"
  rpt "max_concurrency:  $MAX_CONC"
  rpt "partitions:       $PARTITIONS"
  rpt "replication:      1"
  rpt "consumers:        1 (single sync worker, serial apply)"
  rpt "payload_size:     ~260 bytes (JSON customer record)"
  rpt "batch_size:       $(docker exec syncforge-postgres-1 psql -U postgres -d syncforge -tAc "SELECT coalesce(batch_size,'n/a') FROM sync_jobs LIMIT 1" 2>/dev/null || echo 'n/a')"
  rpt "producer:         franz-go, LZ4 compression, sync produce"
  rpt "consumer:         franz-go, manual offset commit, at-start reset"
  rpt "warmup:           10s after stack healthy"
  rpt "duration:         measured (varies by workload)"
  rpt ""
}

# ---- Topic partition control via rpk ------------------------------------
setup_topic() {
  local n="$1"
  log "Topic control: delete + recreate sync.events with $n partition(s)"
  docker exec syncforge-redpanda-1 rpk topic delete sync.events >/dev/null 2>&1 || true
  docker exec syncforge-redpanda-1 rpk topic create sync.events -p "$n" -r 1 >/dev/null
  rpt "topic:            sync.events / partitions=$n / replication=1"
}

# ---- DB + state reset ----------------------------------------------------
reset_state() {
  log "Resetting sync state"
  $COMPOSE restart api engine sim-salesforce sim-hubspot sim-oidc >/dev/null 2>&1 || true
  docker exec syncforge-postgres-1 psql -U postgres -d syncforge -c \
    "TRUNCATE login_attempts, sessions, audit_log, outbound_writes, sync_operations,
             reconciliation_findings, reconciliation_runs, sync_jobs, conflicts,
             dead_letter, retry_queue, processed_events, source_events,
             canonical_records RESTART IDENTITY CASCADE;" >/dev/null
  echo "Waiting for API to become healthy..."
  for i in $(seq 1 60); do
    if curl -fsS "$API/health" >/dev/null 2>&1; then break; fi
    sleep 2
  done
  curl -fsS "$API/health" >/dev/null
  echo "Waiting for worker to subscribe (warm-up)..."
  sleep 10

  # Lift the providers' rate limits so the benchmark measures the pipeline, not
  # the simulators' documented per-minute caps (compose defaults SF 100 / HS 50).
  curl -fsS -X POST "http://localhost:9081/admin/faults" -H "Content-Type: application/json" \
    -d '{"rate_limit_per_min": 100000}' >/dev/null || true
  curl -fsS -X POST "http://localhost:9082/admin/faults" -H "Content-Type: application/json" \
    -d '{"rate_limit_per_min": 100000}' >/dev/null || true
}

# ---- Metric helpers -------------------------------------------------------
metric() { # metric_name — sums across all label values
  curl -fsS "$ENGINE/metrics" 2>/dev/null | awk -v m="$1" '
    $0 ~ "^"m"{" { n=split($0,a," "); v=a[n]+0; sum+=v }
    END { print sum }
  '
}
# histogram_percentile(metric_base, p) — bucket-based estimate.
# Picks the smallest bucket boundary whose cumulative count reaches target.
hist_p() { # metric_base p
  local base="$1" p="$2"
  curl -fsS "$ENGINE/metrics" 2>/dev/null | awk -v b="$base" -v p="$p" '
    $0 ~ "^"b"_bucket{" {
      match($0, /le="([^"]+)"/, m)
      le = m[1]
      n = split($0, a, " ")
      cnt = a[n] + 0
      if (le != "+Inf") buckets[le] = cnt
      total = cnt
    }
    END {
      target = p * total
      best = "+Inf"
      for (k in buckets) if (buckets[k] >= target && (best == "+Inf" || k+0 < best+0)) best = k
      print best
    }'
}

# Snapshot of all reliability counters (for delta computation).
snapshot_metrics() {
  for M in sync_events_total sync_events_success_total sync_events_failed_total \
           sync_duplicates_total sync_destination_writes_total sync_dlq_events_total \
           sync_retry_scheduled_total sync_loop_events_prevented_total \
           sync_conflicts_detected_total sync_conflicts_resolved_total; do
    echo "$M=$(metric "$M")"
  done
}

# ---- Stats sampler (background) -------------------------------------------
SAMPLE_FILE="$REPORT_DIR/stats-$STAMP.txt"
sample_stats() {
  # docker stats loop for CPU/mem during the run
  (
    for i in $(seq 1 120); do
      docker stats --no-stream --format "{{.Name}} {{.CPUPerc}} {{.MemUsage}}" >> "$SAMPLE_FILE" 2>/dev/null || true
      sleep 2
    done
  ) &
  SAMPLER_PID=$!
}

db_counts() {
  docker exec syncforge-postgres-1 psql -U postgres -d syncforge -tAc \
    "SELECT (SELECT count(*) FROM source_events), (SELECT count(*) FROM canonical_records), (SELECT count(*) FROM retry_queue), (SELECT count(*) FROM dead_letter)" | tr -d ' '
}

# ---- Run one workload + measure -------------------------------------------
CUM_EXPECTED=0   # cumulative accepted events across runs (for e2e convergence)
CUM_METRICS=""   # cumulative counter baseline before the run

run_workload() {
  local conc="$1"
  local n_events="$2"
  log "Workload: $n_events events @ concurrency $conc"

  # Baseline counters before this run (for deltas).
  local before; before=$(snapshot_metrics)

  local t0 t1
  t0=$(date +%s.%N)
  if [ -x /tmp/syncforge-bench/loadgen ]; then
    OUT=$(/tmp/syncforge-bench/loadgen -n "$n_events" -c "$conc" -source salesforce -url "$API" 2>&1 | tail -1)
  else
    OUT=$(go run ./cmd/loadgen -n "$n_events" -c "$conc" -source salesforce -url "$API" 2>&1 | tail -1)
  fi
  t1=$(date +%s.%N)
  local ingest_dur ingest_evs
  ingest_dur=$(echo "$t1 $t0" | awk '{printf "%.2f", $1-$2}')
  ingest_evs=$(echo "$n_events $ingest_dur" | awk '{printf "%.0f", $1/$2}')

  local acc rejected errs
  acc=$(echo "$OUT" | grep -oE 'accepted=[0-9]+' | cut -d= -f2)
  rejected=$(echo "$OUT" | grep -oE 'rejected=[0-9]+' | cut -d= -f2)
  errs=$(echo "$OUT" | grep -oE 'errors=[0-9]+' | cut -d= -f2)
  local i_p50 i_p95 i_p99
  i_p50=$(echo "$OUT" | grep -oE 'p50=[0-9.]+' | cut -d= -f2)
  i_p95=$(echo "$OUT" | grep -oE 'p95=[0-9.]+' | cut -d= -f2)
  i_p99=$(echo "$OUT" | grep -oE 'p99=[0-9.]+' | cut -d= -f2)

  CUM_EXPECTED=$((CUM_EXPECTED + acc))
  rpt "INGEST conc=$conc accepted=$acc rejected=$rejected errors=$errs ev/s=$ingest_evs p50=${i_p50}ms p95=${i_p95}ms p99=${i_p99}ms (cumulative expected=$CUM_EXPECTED)"

  # Wait for end-to-end convergence: canonical == cumulative expected.
  log "Waiting for end-to-end convergence ($CUM_EXPECTED canonical)..."
  local e2e_t0 e2e_t1 e2e_dur e2e_evs
  e2e_t0=$(date +%s.%N)
  for i in $(seq 1 300); do
    local counts; counts=$(db_counts)
    local canonical
    canonical=$(echo "$counts" | cut -d'|' -f2)
    if [ "${canonical:-0}" -ge "${CUM_EXPECTED:-0}" ]; then break; fi
    sleep 2
  done
  e2e_t1=$(date +%s.%N)
  e2e_dur=$(echo "$e2e_t1 $e2e_t0" | awk '{printf "%.2f", $1-$2}')
  e2e_evs=$(echo "$acc $e2e_dur" | awk '{printf "%.0f", $1/$2}')
  rpt "END2END conc=$conc converged_in=${e2e_dur}s ev/s=$e2e_evs"

  # E2E processing latency from the engine histogram (overall, not per-run).
  local p50 p95 p99
  p50=$(hist_p sync_processing_duration_seconds 0.50)
  p95=$(hist_p sync_processing_duration_seconds 0.95)
  p99=$(hist_p sync_processing_duration_seconds 0.99)
  rpt "PROC-LAT (overall) p50=${p50}s p95=${p95}s p99=${p99}s"

  # Reliability deltas for this run.
  local after; after=$(snapshot_metrics)
  rpt "RELIABILITY conc=$conc (deltas this run)"
  local M mname
  for line in $before; do
    mname=$(echo "$line" | cut -d= -f1)
    local bval aoval delta
    bval=$(echo "$line" | cut -d= -f2)
    aoval=$(echo "$after" | grep "^$mname=" | cut -d= -f2)
    delta=$(awk -v b="${bval:-0}" -v a="${aoval:-0}" 'BEGIN{printf "%d", a-b}')
    rpt "  $mname = $delta"
  done

  # Drain retries before next run
  echo "Draining retry queue..."
  for i in $(seq 1 60); do
    local n; n=$(docker exec syncforge-postgres-1 psql -U postgres -d syncforge -tAc "SELECT count(*) FROM retry_queue" 2>/dev/null || echo 0)
    [ "$n" = "0" ] && break
    sleep 2
  done
}

# ---- Main -------------------------------------------------------------------
log "SyncForge local reference benchmark"
log "Starting stack"
$COMPOSE up -d postgres redis redpanda sim-salesforce sim-hubspot sim-oidc api engine >/dev/null 2>&1 || true
$COMPOSE up -d >/dev/null 2>&1 || true

record_env
setup_topic "$PARTITIONS"
reset_state
sample_stats

log "Benchmark: headline=$EVENTS, curve levels=$SWEEP_EVENTS, partitions=$PARTITIONS, concurrency -> $MAX_CONC"
rpt "=== Results ==="
rpt "concurrency curve (${SWEEP_EVENTS} events/level):"
# Concurrency sweep: 1,2,4,8,16,32 ... capped at MAX_CONC, deduped.
CONC_LIST=""
for c in 1 2 4 8 16 32; do
  if [ "$c" -le "$MAX_CONC" ]; then
    case " $CONC_LIST " in *" $c "*) ;; *) CONC_LIST="$CONC_LIST $c" ;; esac
  fi
done
case " $CONC_LIST " in *" $MAX_CONC "*) ;; *) CONC_LIST="$CONC_LIST $MAX_CONC" ;; esac
for CONC in $CONC_LIST; do
  run_workload "$CONC" "$SWEEP_EVENTS"
  sleep 3
done

log "Headline run: $EVENTS events @ concurrency $MAX_CONC"
rpt "headline run ($EVENTS events @ conc $MAX_CONC):"
run_workload "$MAX_CONC" "$EVENTS"

log "Resource usage sample (docker stats during run):"
awk '{name=$1; cpu=$2; mem=$3" "$4; sum[name]+=gsub(/%/,"",cpu); } END{}' "$SAMPLE_FILE" 2>/dev/null || true
grep -E "$(docker ps --format '{{.Names}}' | tr '\n' '|' | sed 's/|$//')" "$SAMPLE_FILE" 2>/dev/null | awk '{print $1, $2, $3" "$4}' | sort -u | head -20 || true

log "Queue depth + DB behavior at end:"
rpt "END source_events canonical retry_queue dead_letter: $(db_counts)"

log "Full report: $REPORT"
