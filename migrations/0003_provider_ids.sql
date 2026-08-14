-- Phase 2: canonical records track which provider id maps to each logical
-- entity, enabling cross-system create/update/delete resolution.
BEGIN;

ALTER TABLE canonical_records ADD COLUMN provider_ids jsonb NOT NULL DEFAULT '{}';

COMMIT;
