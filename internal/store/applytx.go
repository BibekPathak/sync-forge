package store

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"syncforge/internal/db"
)

// PersistApplyState commits, in ONE transaction, everything the worker writes
// at the end of an event apply: the destination outbound-write fingerprints
// (loop prevention), the sync_operations ledger rows, and the canonical record
// upsert. This collapses the previous ~3 separate WithTenant transactions per
// event into a single round trip, which is the dominant cost of the worker's
// serial apply path (PostgreSQL was the measured bottleneck at ~90% CPU).
func PersistApplyState(ctx context.Context, pool *pgxpool.Pool, canonical CanonicalRecord, outbound []OutboundWrite, ops []SyncOperation) (CanonicalRecord, error) {
	var out CanonicalRecord
	_, err := db.WithTenant[struct{}](ctx, pool, canonical.TenantID, func(tx pgx.Tx) (struct{}, error) {
		for _, w := range outbound {
			if _, err := tx.Exec(ctx,
				`INSERT INTO outbound_writes (tenant_id, entity_type, entity_id, target_source, fingerprint, applied_version)
				 VALUES ($1,$2,$3,$4,$5,$6)
				 ON CONFLICT (tenant_id, entity_type, entity_id, target_source) DO UPDATE SET
				   fingerprint=EXCLUDED.fingerprint, applied_version=EXCLUDED.applied_version, written_at=now()`,
				w.TenantID, w.EntityType, w.EntityID, w.TargetSource, w.Fingerprint, w.AppliedVersion); err != nil {
				return struct{}{}, err
			}
		}
		for _, o := range ops {
			if _, err := tx.Exec(ctx,
				`INSERT INTO sync_operations (tenant_id, entity_type, entity_id, source, target_source, event_id, applied_version, fingerprint)
				 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
				o.TenantID, o.EntityType, o.EntityID, o.Source, o.TargetSource, o.EventID, o.AppliedVersion, o.Fingerprint); err != nil {
				return struct{}{}, err
			}
		}
		return struct{}{}, tx.QueryRow(ctx,
			`INSERT INTO canonical_records
			   (tenant_id, entity_type, entity_id, fields, version, source_versions, field_provenance,
			    tombstone, origin_source, origin_event_id, sync_operation_id, provider_ids)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
			 ON CONFLICT (tenant_id, entity_type, entity_id) DO UPDATE SET
			   fields=EXCLUDED.fields, version=EXCLUDED.version,
			   source_versions=EXCLUDED.source_versions, field_provenance=EXCLUDED.field_provenance,
			   tombstone=EXCLUDED.tombstone, origin_source=EXCLUDED.origin_source,
			   origin_event_id=EXCLUDED.origin_event_id, sync_operation_id=EXCLUDED.sync_operation_id,
			   provider_ids=EXCLUDED.provider_ids, updated_at=now()
			 RETURNING sync_id, tenant_id, entity_type, entity_id, fields, version, source_versions,
			           field_provenance, tombstone, origin_source, origin_event_id, sync_operation_id,
			           provider_ids, created_at, updated_at`,
			canonical.TenantID, canonical.EntityType, canonical.EntityID, canonical.Fields, canonical.Version,
			canonical.SourceVersions, canonical.FieldProvenance, canonical.Tombstone, canonical.OriginSource,
			canonical.OriginEventID, canonical.SyncOperationID, canonical.ProviderIDs,
		).Scan(&out.SyncID, &out.TenantID, &out.EntityType, &out.EntityID, &out.Fields, &out.Version,
			&out.SourceVersions, &out.FieldProvenance, &out.Tombstone, &out.OriginSource, &out.OriginEventID,
			&out.SyncOperationID, &out.ProviderIDs, &out.CreatedAt, &out.UpdatedAt)
	})
	if err != nil {
		return CanonicalRecord{}, err
	}
	return out, nil
}
