-- SyncForge initial schema (Phase 1: foundation, multi-tenancy, RLS)
BEGIN;

-- =========================================================
-- Tenancy & identity
-- =========================================================
CREATE TABLE tenants (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name        text NOT NULL,
    slug        text UNIQUE NOT NULL,
    status      text NOT NULL DEFAULT 'active' CHECK (status IN ('active','suspended')),
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE users (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    email         text NOT NULL,
    password_hash text NOT NULL,
    role          text NOT NULL DEFAULT 'VIEWER'
                  CHECK (role IN ('ADMIN','OPERATOR','DEVELOPER','VIEWER')),
    created_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, email)
);

CREATE TABLE api_keys (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id    uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name         text NOT NULL,
    key_hash     text NOT NULL UNIQUE,
    role         text NOT NULL DEFAULT 'VIEWER'
                 CHECK (role IN ('ADMIN','OPERATOR','DEVELOPER','VIEWER')),
    enabled      boolean NOT NULL DEFAULT true,
    created_at   timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz
);

-- =========================================================
-- Connections & sync policies
-- =========================================================
CREATE TABLE connections (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name             text NOT NULL,
    provider         text NOT NULL CHECK (provider IN ('salesforce','hubspot','postgres')),
    base_url         text NOT NULL,
    status           text NOT NULL DEFAULT 'disconnected'
                     CHECK (status IN ('disconnected','connecting','healthy','unhealthy','error')),
    webhook_secret   text NOT NULL DEFAULT '',
    config           jsonb NOT NULL DEFAULT '{}',
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    last_health_check timestamptz
);
CREATE INDEX idx_connections_tenant ON connections (tenant_id);

CREATE TABLE sync_policies (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id         uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    entity            text NOT NULL,
    source            text NOT NULL,
    destination       text NOT NULL,
    mode              text NOT NULL DEFAULT 'bidirectional'
                      CHECK (mode IN ('one_way','bidirectional')),
    conflict_strategy  text NOT NULL DEFAULT 'last_write_wins'
                      CHECK (conflict_strategy IN ('last_write_wins','source_priority','field_merge','manual')),
    delete_policy     text NOT NULL DEFAULT 'propagate'
                      CHECK (delete_policy IN ('propagate','tombstone_only','ignore')),
    retry_policy      text NOT NULL DEFAULT 'exponential_backoff',
    source_priority   int NOT NULL DEFAULT 100,
    config            jsonb NOT NULL DEFAULT '{}',
    enabled           boolean NOT NULL DEFAULT true,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, entity, source, destination)
);
CREATE INDEX idx_policies_tenant ON sync_policies (tenant_id);

-- =========================================================
-- Canonical records
-- =========================================================
CREATE TABLE canonical_records (
    sync_id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    entity_type      text NOT NULL,
    entity_id        text NOT NULL,
    fields           jsonb NOT NULL DEFAULT '{}',
    version          bigint NOT NULL DEFAULT 1,
    source_versions  jsonb NOT NULL DEFAULT '{}',
    field_provenance jsonb NOT NULL DEFAULT '{}',
    tombstone        boolean NOT NULL DEFAULT false,
    origin_source    text,
    origin_event_id  text,
    sync_operation_id text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, entity_type, entity_id)
);
CREATE INDEX idx_records_tenant ON canonical_records (tenant_id, entity_type);

-- =========================================================
-- Event pipeline: ingestion, idempotency, retry, DLQ
-- =========================================================
CREATE TABLE source_events (
    id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id      uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    source         text NOT NULL,
    event_id       text NOT NULL,
    entity_type    text NOT NULL,
    entity_id      text NOT NULL,
    event_type     text NOT NULL,
    source_version bigint NOT NULL DEFAULT 0,
    occurred_at    timestamptz,
    received_at    timestamptz NOT NULL DEFAULT now(),
    correlation_id text,
    provenance     jsonb NOT NULL DEFAULT '{}',
    raw            jsonb NOT NULL,
    status         text NOT NULL DEFAULT 'received'
                   CHECK (status IN ('received','validated','processed','failed','stale','duplicate','dlq')),
    UNIQUE (tenant_id, source, event_id)
);
CREATE INDEX idx_events_tenant ON source_events (tenant_id, received_at);
CREATE INDEX idx_events_status ON source_events (status);

-- Idempotency log: a logical event is processed at most once.
CREATE TABLE processed_events (
    tenant_id     uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    source        text NOT NULL,
    event_id      text NOT NULL,
    entity_type   text NOT NULL,
    entity_id     text NOT NULL,
    processed_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, source, event_id)
);
CREATE INDEX idx_processed_entity ON processed_events (tenant_id, entity_type, entity_id);

CREATE TABLE retry_queue (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    event_id        text NOT NULL,
    attempt         int NOT NULL DEFAULT 1,
    max_attempts    int NOT NULL DEFAULT 8,
    next_attempt_at timestamptz NOT NULL,
    last_error      text,
    error_class     text,
    state           jsonb NOT NULL DEFAULT '{}',
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_retry_due ON retry_queue (next_attempt_at);
CREATE INDEX idx_retry_tenant ON retry_queue (tenant_id);

CREATE TABLE dead_letter (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id    uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    event_id     text NOT NULL,
    reason       text NOT NULL,
    error_class  text,
    payload      jsonb NOT NULL,
    status       text NOT NULL DEFAULT 'open'
                 CHECK (status IN ('open','retrying','discarded','resolved')),
    created_at   timestamptz NOT NULL DEFAULT now(),
    resolved_at  timestamptz
);
CREATE INDEX idx_dlq_tenant ON dead_letter (tenant_id, status);

-- =========================================================
-- Conflicts
-- =========================================================
CREATE TABLE conflicts (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    entity_type         text NOT NULL,
    entity_id           text NOT NULL,
    source_a            text NOT NULL,
    version_a           bigint NOT NULL,
    payload_a           jsonb NOT NULL,
    source_b            text NOT NULL,
    version_b           bigint NOT NULL,
    payload_b           jsonb NOT NULL,
    detected_at         timestamptz NOT NULL DEFAULT now(),
    status              text NOT NULL DEFAULT 'CONFLICT_PENDING'
                        CHECK (status IN ('CONFLICT_PENDING','RESOLVED','AUTO_RESOLVED','DISMISSED')),
    resolution_strategy text,
    resolved_by         text,
    resolved_at         timestamptz
);
CREATE INDEX idx_conflicts_tenant ON conflicts (tenant_id, status);

-- =========================================================
-- Sync jobs (initial full sync, reconciliation)
-- =========================================================
CREATE TABLE sync_jobs (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id    uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    entity       text NOT NULL,
    source       text NOT NULL,
    destination  text NOT NULL,
    type         text NOT NULL DEFAULT 'initial'
                 CHECK (type IN ('initial','reconcile')),
    status       text NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending','running','paused','completed','failed','cancelled')),
    cursor       text,
    processed    bigint NOT NULL DEFAULT 0,
    failed       bigint NOT NULL DEFAULT 0,
    total        bigint NOT NULL DEFAULT 0,
    batch_size   int NOT NULL DEFAULT 1000,
    started_at   timestamptz,
    finished_at  timestamptz,
    last_error   text,
    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_jobs_tenant ON sync_jobs (tenant_id, status);

CREATE TABLE reconciliation_runs (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    entity      text NOT NULL,
    source      text NOT NULL,
    status      text NOT NULL DEFAULT 'running'
                CHECK (status IN ('running','completed','failed')),
    total       bigint NOT NULL DEFAULT 0,
    drift       bigint NOT NULL DEFAULT 0,
    missed      bigint NOT NULL DEFAULT 0,
    deleted     bigint NOT NULL DEFAULT 0,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz
);
CREATE INDEX idx_recon_tenant ON reconciliation_runs (tenant_id);

-- =========================================================
-- Provenance & loop prevention
-- =========================================================
CREATE TABLE sync_operations (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    entity_type   text NOT NULL,
    entity_id     text NOT NULL,
    source        text NOT NULL,
    target_source text NOT NULL,
    event_id      text,
    applied_version bigint NOT NULL,
    fingerprint   text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_ops_entity ON sync_operations (tenant_id, entity_type, entity_id, target_source);

CREATE TABLE outbound_writes (
    tenant_id      uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    entity_type    text NOT NULL,
    entity_id      text NOT NULL,
    target_source  text NOT NULL,
    fingerprint    text NOT NULL,
    applied_version bigint NOT NULL,
    written_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, entity_type, entity_id, target_source)
);

-- =========================================================
-- Audit
-- =========================================================
CREATE TABLE audit_log (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id  uuid REFERENCES tenants(id) ON DELETE CASCADE,
    actor      text,
    action     text NOT NULL,
    resource   text NOT NULL,
    resource_id text,
    metadata   jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_audit_tenant ON audit_log (tenant_id, created_at);

-- =========================================================
-- Row-Level Security
-- =========================================================
-- Helper returning the current tenant context. NULL when not set => no rows
-- visible (fail-closed).
CREATE OR REPLACE FUNCTION syncforge_tenant_id() RETURNS text
LANGUAGE sql STABLE AS
$$ SELECT current_setting('app.tenant_id', true) $$;

ALTER TABLE tenants             ENABLE ROW LEVEL SECURITY;
ALTER TABLE users               ENABLE ROW LEVEL SECURITY;
ALTER TABLE api_keys            ENABLE ROW LEVEL SECURITY;
ALTER TABLE connections         ENABLE ROW LEVEL SECURITY;
ALTER TABLE sync_policies       ENABLE ROW LEVEL SECURITY;
ALTER TABLE canonical_records   ENABLE ROW LEVEL SECURITY;
ALTER TABLE source_events       ENABLE ROW LEVEL SECURITY;
ALTER TABLE processed_events    ENABLE ROW LEVEL SECURITY;
ALTER TABLE retry_queue         ENABLE ROW LEVEL SECURITY;
ALTER TABLE dead_letter         ENABLE ROW LEVEL SECURITY;
ALTER TABLE conflicts           ENABLE ROW LEVEL SECURITY;
ALTER TABLE sync_jobs           ENABLE ROW LEVEL SECURITY;
ALTER TABLE reconciliation_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE sync_operations     ENABLE ROW LEVEL SECURITY;
ALTER TABLE outbound_writes     ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_log           ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenants_self ON tenants
    FOR ALL USING (id::text = syncforge_tenant_id())
    WITH CHECK (id::text = syncforge_tenant_id());

CREATE POLICY tenant_isolation ON users
    FOR ALL USING (tenant_id::text = syncforge_tenant_id())
    WITH CHECK (tenant_id::text = syncforge_tenant_id());

CREATE POLICY tenant_isolation ON api_keys
    FOR ALL USING (tenant_id::text = syncforge_tenant_id())
    WITH CHECK (tenant_id::text = syncforge_tenant_id());

CREATE POLICY tenant_isolation ON connections
    FOR ALL USING (tenant_id::text = syncforge_tenant_id())
    WITH CHECK (tenant_id::text = syncforge_tenant_id());

CREATE POLICY tenant_isolation ON sync_policies
    FOR ALL USING (tenant_id::text = syncforge_tenant_id())
    WITH CHECK (tenant_id::text = syncforge_tenant_id());

CREATE POLICY tenant_isolation ON canonical_records
    FOR ALL USING (tenant_id::text = syncforge_tenant_id())
    WITH CHECK (tenant_id::text = syncforge_tenant_id());

CREATE POLICY tenant_isolation ON source_events
    FOR ALL USING (tenant_id::text = syncforge_tenant_id())
    WITH CHECK (tenant_id::text = syncforge_tenant_id());

CREATE POLICY tenant_isolation ON processed_events
    FOR ALL USING (tenant_id::text = syncforge_tenant_id())
    WITH CHECK (tenant_id::text = syncforge_tenant_id());

CREATE POLICY tenant_isolation ON retry_queue
    FOR ALL USING (tenant_id::text = syncforge_tenant_id())
    WITH CHECK (tenant_id::text = syncforge_tenant_id());

CREATE POLICY tenant_isolation ON dead_letter
    FOR ALL USING (tenant_id::text = syncforge_tenant_id())
    WITH CHECK (tenant_id::text = syncforge_tenant_id());

CREATE POLICY tenant_isolation ON conflicts
    FOR ALL USING (tenant_id::text = syncforge_tenant_id())
    WITH CHECK (tenant_id::text = syncforge_tenant_id());

CREATE POLICY tenant_isolation ON sync_jobs
    FOR ALL USING (tenant_id::text = syncforge_tenant_id())
    WITH CHECK (tenant_id::text = syncforge_tenant_id());

CREATE POLICY tenant_isolation ON reconciliation_runs
    FOR ALL USING (tenant_id::text = syncforge_tenant_id())
    WITH CHECK (tenant_id::text = syncforge_tenant_id());

CREATE POLICY tenant_isolation ON sync_operations
    FOR ALL USING (tenant_id::text = syncforge_tenant_id())
    WITH CHECK (tenant_id::text = syncforge_tenant_id());

CREATE POLICY tenant_isolation ON outbound_writes
    FOR ALL USING (tenant_id::text = syncforge_tenant_id())
    WITH CHECK (tenant_id::text = syncforge_tenant_id());

CREATE POLICY tenant_isolation ON audit_log
    FOR ALL USING (tenant_id::text = syncforge_tenant_id())
    WITH CHECK (tenant_id::text = syncforge_tenant_id());

-- =========================================================
-- Internal role access (BYPASSRLS engine role)
-- =========================================================
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO syncforge_engine;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO syncforge_engine;
GRANT USAGE ON SCHEMA public TO syncforge_engine;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO syncforge_engine;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO syncforge_engine;

COMMIT;
