-- Phase 4: idempotent retry/DLQ enqueue. A retry or DLQ entry is unique per
-- logical event (tenant_id, event_id), so concurrent redeliveries and retry
-- cycles cannot create duplicate work.
BEGIN;

CREATE UNIQUE INDEX IF NOT EXISTS idx_retry_tenant_event ON retry_queue (tenant_id, event_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dlq_tenant_event ON dead_letter (tenant_id, event_id);

COMMIT;