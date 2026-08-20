#!/usr/bin/env bash
# One-shot benchmark: fire N events at concurrency C, measure ingest (loadgen)
# and end-to-end convergence (canonical == N). Designed to fit in one run.
#
# Usage: scripts/bench-one.sh <events> <concurrency>
set -euo pipefail
cd "$(dirname "$0")/.."
API=${API:-http://localhost:8080}
ENGINE=${ENGINE:-http://localhost:8081}
N="${1:-20000}"
C="${2:-32}"
LG=/tmp/syncforge-bench/loadgen
[ -x "$LG" ] || go build -o "$LG" ./cmd/loadgen

echo "==> one-shot: $N events @ concurrency $C"

t0=$(date +%s.%N)
OUT=$($LG -n "$N" -c "$C" -source salesforce -url "$API" 2>&1 | tail -1)
t1=$(date +%s.%N)
echo "INGEST $OUT"
acc=$(echo "$OUT" | grep -oE 'accepted=[0-9]+' | cut -d= -f2)
ingest_dur=$(echo "$t1 $t0" | awk '{printf "%.1f", $1-$2}')
echo "ingest_dur=${ingest_dur}s ingest_evs=$(echo "$N $ingest_dur" | awk '{printf "%.0f", $1/$2}')"

# End-to-end convergence
echo "==> waiting for $acc canonical..."
t2=$(date +%s.%N)
for i in $(seq 1 400); do
  canon=$(docker exec syncforge-postgres-1 psql -U postgres -d syncforge -tAc "SELECT count(*) FROM canonical_records" 2>/dev/null || echo 0)
  [ "$canon" -ge "$acc" ] && break
  sleep 2
done
t3=$(date +%s.%N)
dur=$(echo "$t3 $t2" | awk '{printf "%.1f", $1-$2}')
echo "END2END canon=$canon converged_in=${dur}s e2e_evs=$(echo "$acc $dur" | awk '{printf "%.0f", $1/$2}')"

# Engine histogram percentiles
curl -fsS "$ENGINE/metrics" | awk -v b="sync_processing_duration_seconds" '
  $0 ~ "^"b"_bucket{" {
    match($0, /le="([^"]+)"/, m); le=m[1]
    n=split($0,a," "); cnt=a[n]+0
    if (le!="+Inf") buckets[le]=cnt
    total=cnt
  }
  END {
    split("0.50 0.95 0.99", ps, " ")
    for (j=1;j<=3;j++) {
      target=ps[j]*total; best="+Inf"
      for (k in buckets) if (buckets[k]>=target && (best=="+Inf" || k+0<best+0)) best=k
      print "p"ps[j]"="best"s"
    }
  }'

# Reliability counters
echo "RELIABILITY"
for M in sync_events_total sync_events_success_total sync_events_failed_total \
         sync_duplicates_total sync_destination_writes_total sync_dlq_events_total \
         sync_retry_scheduled_total sync_loop_events_prevented_total sync_conflicts_detected_total; do
  v=$(curl -fsS "$ENGINE/metrics" | awk -v m="$M" '$0 ~ "^"m"{" { n=split($0,a," "); sum+=a[n]+0 } END{print sum}')
  echo "  $M = $v"
done

# Final data-loss check
sleep 5
canon2=$(docker exec syncforge-postgres-1 psql -U postgres -d syncforge -tAc "SELECT count(*) FROM canonical_records" 2>/dev/null || echo 0)
src=$(docker exec syncforge-postgres-1 psql -U postgres -d syncforge -tAc "SELECT count(*) FROM source_events WHERE status='received'" 2>/dev/null || echo 0)
echo "DATA-LOSS received_pending=$src canonical=$canon2"
