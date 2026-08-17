package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"syncforge/internal/db"
)

// Session is a server-side record of a user login. The HMAC token carries the
// session's jti; a live (non-revoked, non-expired) row is required to
// authenticate, which enables logout and revocation.
type Session struct {
	ID        string     `json:"id"`
	JTI       string     `json:"jti"`
	UserID    string     `json:"user_id"`
	TenantID  string     `json:"tenant_id"`
	Role      string     `json:"role"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt time.Time  `json:"expires_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}

// CreateSession records a new login. Called via the admin pool because login
// happens before a tenant context exists; RLS does not apply to BYPASSRLS.
func CreateSession(ctx context.Context, pool *pgxpool.Pool, s Session) (Session, error) {
	var out Session
	err := pool.QueryRow(ctx,
		`INSERT INTO sessions (jti, user_id, tenant_id, role, expires_at)
		 VALUES ($1,$2,$3,$4,$5)
		 RETURNING id, jti, user_id, tenant_id, role, created_at, expires_at, revoked_at`,
		s.JTI, s.UserID, s.TenantID, s.Role, s.ExpiresAt,
	).Scan(&out.ID, &out.JTI, &out.UserID, &out.TenantID, &out.Role,
		&out.CreatedAt, &out.ExpiresAt, &out.RevokedAt)
	return out, err
}

// GetLiveSession loads a session by jti inside the caller's tenant context,
// returning it only when it is not revoked and has not expired.
func GetLiveSession(ctx context.Context, pool *pgxpool.Pool, tenantID, jti string) (Session, error) {
	var s Session
	_, err := db.WithTenant[struct{}](ctx, pool, tenantID, func(tx pgx.Tx) (struct{}, error) {
		err := tx.QueryRow(ctx,
			`SELECT id, jti, user_id, tenant_id, role, created_at, expires_at, revoked_at
			 FROM sessions WHERE jti=$1 AND tenant_id=$2`, jti, tenantID,
		).Scan(&s.ID, &s.JTI, &s.UserID, &s.TenantID, &s.Role,
			&s.CreatedAt, &s.ExpiresAt, &s.RevokedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return struct{}{}, ErrNotFound
		}
		return struct{}{}, err
	})
	if err != nil {
		return Session{}, err
	}
	if s.RevokedAt != nil || s.ExpiresAt.Before(time.Now()) {
		return Session{}, ErrNotFound
	}
	return s, nil
}

// RevokeSession marks a session revoked. Used by logout.
func RevokeSession(ctx context.Context, pool *pgxpool.Pool, tenantID, jti string) error {
	return db.WithTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE sessions SET revoked_at=now()
			 WHERE jti=$1 AND tenant_id=$2 AND revoked_at IS NULL`, jti, tenantID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// RevokeUserSessions revokes every live session of a user (used on password
// change / "sign out everywhere").
func RevokeUserSessions(ctx context.Context, pool *pgxpool.Pool, tenantID, userID string) error {
	return db.WithTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`UPDATE sessions SET revoked_at=now()
			 WHERE user_id=$1 AND tenant_id=$2 AND revoked_at IS NULL`, userID, tenantID)
		return err
	})
}

// ListSessions lists the tenant's live sessions (for an admin "active logins"
// surface).
func ListSessions(ctx context.Context, pool *pgxpool.Pool, tenantID string, limit int) ([]Session, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	out, err := db.WithTenant[[]Session](ctx, pool, tenantID, func(tx pgx.Tx) ([]Session, error) {
		rows, err := tx.Query(ctx,
			`SELECT id, jti, user_id, tenant_id, role, created_at, expires_at, revoked_at
			 FROM sessions WHERE revoked_at IS NULL AND expires_at > now()
			 ORDER BY created_at DESC LIMIT $1`, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var res []Session
		for rows.Next() {
			var s Session
			if err := rows.Scan(&s.ID, &s.JTI, &s.UserID, &s.TenantID, &s.Role,
				&s.CreatedAt, &s.ExpiresAt, &s.RevokedAt); err != nil {
				return nil, err
			}
			res = append(res, s)
		}
		return res, rows.Err()
	})
	return out, err
}
