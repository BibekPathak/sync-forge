SHELL := /bin/bash
COMPOSE := docker compose -f deploy/compose/docker-compose.yml
DB_URL ?= postgres://syncforge_app:syncforge_app@localhost:5432/syncforge_test?sslmode=disable
ADMIN_URL ?= postgres://syncforge_engine:syncforge_engine@localhost:5432/syncforge_test?sslmode=disable

.PHONY: build test test-unit test-integration bench loadgen up down logs demo fmt vet migrate

build:
	go build ./...

fmt:
	gofmt -l -w cmd internal tests

vet:
	go vet ./...

test-unit:
	go test -count=1 ./internal/...

test-integration:
	$(COMPOSE) up -d postgres redis
	@echo "waiting for postgres..."
	@sleep 5
	DATABASE_URL="$(DB_URL)" ADMIN_DATABASE_URL="$(ADMIN_URL)" go test -count=1 -tags integration -v ./tests/integration/...

test-all: test-unit test-integration

bench:
	go test -bench=. -benchmem -run=xxx ./internal/reconcile/ ./internal/conflict/ ./internal/backoff/ ./internal/connectors/

# loadgen fires a burst of webhooks at a running stack (make up first).
loadgen:
	go run ./cmd/loadgen -n 500 -c 32 -source salesforce -url "$(shell echo $${SYNCFORGE_API_URL:-http://localhost:8080})"

up:
	$(COMPOSE) up --build -d
	@echo "=== SyncForge started ==="
	@echo "API:        http://localhost:8080/health"
	@echo "Dashboard:  http://localhost:3001"
	@echo "Prometheus: http://localhost:9090"
	@echo "Grafana:    http://localhost:3000 (admin/admin)"

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

demo:
	./scripts/demo.sh
