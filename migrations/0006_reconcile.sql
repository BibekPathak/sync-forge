-- Phase 6: reconciliation runs carry an operating mode (auto vs operator
-- approval) and link to their scheduling sync job, and per-record findings
-- give operators a review-and-apply surface for drift, missed, and
-- deleted-on-provider records.
BEGIN;

ALTER TABLE reconciliation_runs
    ADD COLUMN mode   text NOT NULL DEFAULT 'auto'
                   CHECK (mode IN ('auto','manual'));

ALTER TABLE reconciliation_runs
    ADD COLUMN job_id uuid REFERENCES sync_jobs(id) ON DELETE SET NULL;

ALTER TABLE reconciliation_runs
    ADD COLUMN error text;

CREATE TABLE reconciliation_findings (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id            uuid NOT NULL REFERENCES reconciliation_runs(id) ON DELETE CASCADE,
    tenant_id         uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    kind              text NOT NULL
                      CHECK (kind IN ('missed','drift','deleted','missing')),
    provider_id       text NOT NULL,
    canonical_id      text,
    canonical_fields  jsonb NOT NULL DEFAULT '{}',
    provider_fields   jsonb NOT NULL DEFAULT '{}',
    provider_version  bigint NOT NULL DEFAULT 0,
    direction         text NOT NULL DEFAULT 'push_canonical'
                      CHECK (direction IN ('push_canonical','adopt_provider','delete')),
    status            text NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending','applied','dismissed','skipped','failed')),
    error             text,
    created_at        timestamptz NOT NULL DEFAULT now(),
    applied_at        timestamptz
);

CREATE UNIQUE INDEX idx_findings_dedupe
    ON reconciliation_findings (run_id, kind, provider_id);

CREATE INDEX idx_findings_run_status ON reconciliation_findings (run_id, status);
CREATE INDEX idx_findings_tenant ON reconciliation_findings (tenant_id);

ALTER TABLE reconciliation_findings ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON reconciliation_findings
    FOR ALL USING (tenant_id::text = syncforge_tenant_id())
    WITH CHECK (tenant_id::text = syncforge_tenant_id());

ALTER TABLE reconciliation_findings FORCE ROW LEVEL SECURITY;

COMMIT;