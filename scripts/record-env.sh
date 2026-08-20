#!/usr/bin/env bash
# Records the SyncForge local reference environment for benchmark reports.
# Prints a key: value block describing the host, toolchain, and container
# stack. Used by scripts/bench.sh and runnable standalone:
#
#   ./scripts/record-env.sh
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE="docker compose -f deploy/compose/docker-compose.yml"

echo "os:               $(uname -sr)"
echo "cpu:              $(lscpu 2>/dev/null | awk -F: '/Model name/{print $2}' | xargs) ($(nproc) cores)"
echo "ram:              $(free -h | awk '/Mem:/{print $2}') total / $(free -h | awk '/Mem:/{print $7}') available"
echo "go:               $(go version 2>/dev/null | awk '{print $3}')"
echo "docker:           $(docker --version 2>/dev/null | awk '{print $3}')"
echo "postgres:         $(docker exec syncforge-postgres-1 psql -U postgres -tAc 'SHOW server_version' 2>/dev/null || echo '?')"
echo "redis:            $(docker exec syncforge-redis-1 redis-cli --version 2>/dev/null || echo '?')"
echo "redpanda:         $(docker exec syncforge-redpanda-1 rpk version 2>/dev/null | grep -oE 'v[0-9.]+' | head -1 || echo '?')"

echo ""
echo "=== container resource limits (compose) ==="
$COMPOSE ps --format '{{.Name}}' 2>/dev/null | while read -r c; do
  cpu=$($COMPOSE inspect -f '{{.HostConfig.CpuShares}}' "$c" 2>/dev/null || echo "default")
  mem=$($COMPOSE inspect -f '{{.HostConfig.Memory}}' "$c" 2>/dev/null || echo "0")
  printf '%-40s cpushares=%-6s mem=%s\n' "$c" "$cpu" "$mem"
done
