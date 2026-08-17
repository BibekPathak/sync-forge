-- Phase 12: server-side user sessions. Every login records a session row
-- (keyed by a unique jti embedded in the HMAC token), so a session can be
-- revoked on logout, on refresh (rotation), or on demand. The token keeps its
-- fast HMAC path; verification additionally checks the row is live.
BEGIN;

CREATE TABLE sessions (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    jti        uuid NOT NULL UNIQUE,
    user_id    uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tenant_id  uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    role       text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz
);

CREATE INDEX idx_sessions_user ON sessions (user_id);
CREATE INDEX idx_sessions_tenant ON sessions (tenant_id, created_at);

ALTER TABLE sessions ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON sessions
    FOR ALL USING (tenant_id::text = syncforge_tenant_id())
    WITH CHECK (tenant_id::text = syncforge_tenant_id());

ALTER TABLE sessions FORCE ROW LEVEL SECURITY;

COMMIT;
