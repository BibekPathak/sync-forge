-- Phase 16: login brute-force protection. Every login attempt is recorded so
-- the API can enforce an account lockout (N consecutive failures within a
-- window) and a per-IP throttle. The table is durable and RLS-scoped; the
-- global throttle additionally uses Redis.
BEGIN;

CREATE TABLE login_attempts (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id    uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    email        text NOT NULL,
    ip           text NOT NULL DEFAULT '',
    success      boolean NOT NULL DEFAULT false,
    attempted_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_login_attempts_identity ON login_attempts (tenant_id, email, attempted_at DESC);
CREATE INDEX idx_login_attempts_ip ON login_attempts (tenant_id, ip, attempted_at DESC);

ALTER TABLE login_attempts ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON login_attempts
    FOR ALL USING (tenant_id::text = syncforge_tenant_id())
    WITH CHECK (tenant_id::text = syncforge_tenant_id());

ALTER TABLE login_attempts FORCE ROW LEVEL SECURITY;

COMMIT;
