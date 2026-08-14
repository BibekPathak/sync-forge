package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"syncforge/internal/db"
)

// CanonicalRecord is the persisted canonical model plus per-provider id
// mapping (provider_ids) used to resolve records across systems.
type CanonicalRecord struct {
	SyncID          string
	TenantID        string
	EntityType      string
	EntityID        string
	Fields          map[string]any
	Version         int64
	SourceVersions  map[string]int64
	FieldProvenance map[string]any
	Tombstone       bool
	OriginSource    string
	OriginEventID   string
	SyncOperationID string
	ProviderIDs     map[string]string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

func GetCanonical(ctx context.Context, pool *pgxpool.Pool, tenant, entityType, entityID string) (CanonicalRecord, error) {
	out, err := db.WithTenant[CanonicalRecord](ctx, pool, tenant, func(tx pgx.Tx) (CanonicalRecord, error) {
		var c CanonicalRecord
		err := tx.QueryRow(ctx,
			`SELECT sync_id, tenant_id, entity_type, entity_id, fields, version, source_versions,
			        field_provenance, tombstone, origin_source, origin_event_id, sync_operation_id,
			        provider_ids, created_at, updated_at
			 FROM canonical_records WHERE entity_type=$1 AND entity_id=$2`,
			entityType, entityID,
		).Scan(&c.SyncID, &c.TenantID, &c.EntityType, &c.EntityID, &c.Fields, &c.Version,
			&c.SourceVersions, &c.FieldProvenance, &c.Tombstone, &c.OriginSource, &c.OriginEventID,
			&c.SyncOperationID, &c.ProviderIDs, &c.CreatedAt, &c.UpdatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return CanonicalRecord{}, ErrNotFound
		}
		return c, err
	})
	return out, err
}

// GetCanonicalByProvider finds the canonical record owning a given provider
// record id (e.g. which canonical customer has salesforce id "sf-000001").
func GetCanonicalByProvider(ctx context.Context, pool *pgxpool.Pool, tenant, entityType, provider, providerID string) (CanonicalRecord, error) {
	out, err := db.WithTenant[CanonicalRecord](ctx, pool, tenant, func(tx pgx.Tx) (CanonicalRecord, error) {
		var c CanonicalRecord
		err := tx.QueryRow(ctx,
			`SELECT sync_id, tenant_id, entity_type, entity_id, fields, version, source_versions,
			        field_provenance, tombstone, origin_source, origin_event_id, sync_operation_id,
			        provider_ids, created_at, updated_at
			 FROM canonical_records WHERE entity_type=$1 AND provider_ids->>$2 = $3`,
			entityType, provider, providerID,
		).Scan(&c.SyncID, &c.TenantID, &c.EntityType, &c.EntityID, &c.Fields, &c.Version,
			&c.SourceVersions, &c.FieldProvenance, &c.Tombstone, &c.OriginSource, &c.OriginEventID,
			&c.SyncOperationID, &c.ProviderIDs, &c.CreatedAt, &c.UpdatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return CanonicalRecord{}, ErrNotFound
		}
		return c, err
	})
	return out, err
}

// GetCanonicalByEmail finds a live (non-tombstoned) canonical record by the
// canonical email field. This is the identity-resolution fallback when a
// provider id has not been mapped yet (e.g. a HubSpot contact created
// independently that matches an existing Salesforce customer).
func GetCanonicalByEmail(ctx context.Context, pool *pgxpool.Pool, tenant, entityType, email string) (CanonicalRecord, error) {
	out, err := db.WithTenant[CanonicalRecord](ctx, pool, tenant, func(tx pgx.Tx) (CanonicalRecord, error) {
		var c CanonicalRecord
		err := tx.QueryRow(ctx,
			`SELECT sync_id, tenant_id, entity_type, entity_id, fields, version, source_versions,
			        field_provenance, tombstone, origin_source, origin_event_id, sync_operation_id,
			        provider_ids, created_at, updated_at
			 FROM canonical_records
			 WHERE entity_type=$1 AND tombstone=false AND fields->>'email' = $2
			 ORDER BY created_at ASC LIMIT 1`,
			entityType, email,
		).Scan(&c.SyncID, &c.TenantID, &c.EntityType, &c.EntityID, &c.Fields, &c.Version,
			&c.SourceVersions, &c.FieldProvenance, &c.Tombstone, &c.OriginSource, &c.OriginEventID,
			&c.SyncOperationID, &c.ProviderIDs, &c.CreatedAt, &c.UpdatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return CanonicalRecord{}, ErrNotFound
		}
		return c, err
	})
	return out, err
}

// AddProviderID links an additional provider record id to an existing canonical
// record (jsonb merge). Used by identity resolution when an independently
// created record is matched by email.
func AddProviderID(ctx context.Context, pool *pgxpool.Pool, tenant, entityType, entityID, provider, providerID string) error {
	return db.WithTenantTx(ctx, pool, tenant, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`UPDATE canonical_records
			 SET provider_ids = provider_ids || jsonb_build_object($4::text, $5::text), updated_at=now()
			 WHERE tenant_id=$1 AND entity_type=$2 AND entity_id=$3`,
			tenant, entityType, entityID, provider, providerID)
		return err
	})
}

// UpsertCanonical inserts or updates a canonical record and returns its
// persisted version. All fields except the identity tuple are overwritten.
func UpsertCanonical(ctx context.Context, pool *pgxpool.Pool, c CanonicalRecord) (CanonicalRecord, error) {
	var out CanonicalRecord
	_, err := db.WithTenant[struct{}](ctx, pool, c.TenantID, func(tx pgx.Tx) (struct{}, error) {
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
			c.TenantID, c.EntityType, c.EntityID, c.Fields, c.Version, c.SourceVersions, c.FieldProvenance,
			c.Tombstone, c.OriginSource, c.OriginEventID, c.SyncOperationID, c.ProviderIDs,
		).Scan(&out.SyncID, &out.TenantID, &out.EntityType, &out.EntityID, &out.Fields, &out.Version,
			&out.SourceVersions, &out.FieldProvenance, &out.Tombstone, &out.OriginSource, &out.OriginEventID,
			&out.SyncOperationID, &out.ProviderIDs, &out.CreatedAt, &out.UpdatedAt)
	})
	if err != nil {
		return CanonicalRecord{}, err
	}
	return out, nil
}
