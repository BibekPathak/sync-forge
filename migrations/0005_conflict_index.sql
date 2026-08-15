-- Phase 5: conflict detection + resolution.
-- The conflicts table (0001) already exists; this adds the indexes needed for
-- idempotent detection (a concurrent re-delivery of the same logical pair must
-- not create a duplicate conflict) and efficient operator listing.
BEGIN;

-- Idempotency key for conflict detection: the same (tenant, entity, source
-- pair, both versions) can only ever be recorded once. Detection is a
-- no-op when an identical pair arrives again.
CREATE UNIQUE INDEX IF NOT EXISTS idx_conflicts_dedupe
    ON conflicts (tenant_id, entity_type, entity_id, source_a, source_b, version_a, version_b);

-- Operator listing: find conflicts for an entity, optionally by status.
CREATE INDEX IF NOT EXISTS idx_conflicts_entity
    ON conflicts (tenant_id, entity_id, status);

COMMIT;