-- PostgreSQL RLS policies do NOT apply to a table's owner unless the table
-- has FORCE ROW LEVEL SECURITY. All tenant tables are owned by syncforge_app
-- (it runs migrations and is the API runtime role), so RLS must be FORCED or
-- the app role would silently bypass tenant isolation.
BEGIN;

ALTER TABLE tenants             FORCE ROW LEVEL SECURITY;
ALTER TABLE users               FORCE ROW LEVEL SECURITY;
ALTER TABLE api_keys            FORCE ROW LEVEL SECURITY;
ALTER TABLE connections         FORCE ROW LEVEL SECURITY;
ALTER TABLE sync_policies       FORCE ROW LEVEL SECURITY;
ALTER TABLE canonical_records   FORCE ROW LEVEL SECURITY;
ALTER TABLE source_events       FORCE ROW LEVEL SECURITY;
ALTER TABLE processed_events    FORCE ROW LEVEL SECURITY;
ALTER TABLE retry_queue         FORCE ROW LEVEL SECURITY;
ALTER TABLE dead_letter         FORCE ROW LEVEL SECURITY;
ALTER TABLE conflicts           FORCE ROW LEVEL SECURITY;
ALTER TABLE sync_jobs           FORCE ROW LEVEL SECURITY;
ALTER TABLE reconciliation_runs FORCE ROW LEVEL SECURITY;
ALTER TABLE sync_operations     FORCE ROW LEVEL SECURITY;
ALTER TABLE outbound_writes     FORCE ROW LEVEL SECURITY;
ALTER TABLE audit_log           FORCE ROW LEVEL SECURITY;

COMMIT;
