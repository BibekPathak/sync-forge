package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"syncforge/internal/db"
)

// User is a tenant-scoped login account. Passwords are stored as bcrypt
// hashes; the raw credential is never persisted. TOTPSecret is the base32 MFA
// secret (empty until enrolled); TOTPEnabled gates whether login requires a
// code.
type User struct {
	ID           string    `json:"id"`
	TenantID     string    `json:"tenant_id"`
	Email        string    `json:"email"`
	PasswordHash string    `json:"-"`
	Role         string    `json:"role"`
	TOTPSecret   string    `json:"-"`
	TOTPEnabled  bool      `json:"totp_enabled"`
	CreatedAt    time.Time `json:"created_at"`
}

// CreateUser creates a tenant user inside the caller's tenant context so RLS
// enforces the WITH CHECK on tenant_id. Returns ErrExists on a duplicate email.
func CreateUser(ctx context.Context, pool *pgxpool.Pool, tenantID, email, passwordHash, role string) (User, error) {
	var u User
	_, err := db.WithTenant[struct{}](ctx, pool, tenantID, func(tx pgx.Tx) (struct{}, error) {
		err := tx.QueryRow(ctx,
			`INSERT INTO users (tenant_id, email, password_hash, role)
			 VALUES ($1,$2,$3,$4)
			 RETURNING id, tenant_id, email, password_hash, role, COALESCE(totp_secret,''), totp_enabled, created_at`,
			tenantID, email, passwordHash, role,
		).Scan(&u.ID, &u.TenantID, &u.Email, &u.PasswordHash, &u.Role, &u.TOTPSecret, &u.TOTPEnabled, &u.CreatedAt)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return struct{}{}, ErrExists
			}
		}
		return struct{}{}, err
	})
	return u, err
}

// GetUserByEmail looks up a tenant user by email.
func GetUserByEmail(ctx context.Context, pool *pgxpool.Pool, tenantID, email string) (User, error) {
	var u User
	_, err := db.WithTenant[struct{}](ctx, pool, tenantID, func(tx pgx.Tx) (struct{}, error) {
		err := tx.QueryRow(ctx,
			`SELECT id, tenant_id, email, password_hash, role, COALESCE(totp_secret,''), totp_enabled, created_at
			 FROM users WHERE tenant_id=$1 AND email=$2`, tenantID, email,
		).Scan(&u.ID, &u.TenantID, &u.Email, &u.PasswordHash, &u.Role, &u.TOTPSecret, &u.TOTPEnabled, &u.CreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return struct{}{}, ErrNotFound
		}
		return struct{}{}, err
	})
	return u, err
}

// ListUsers lists the tenant's users.
func ListUsers(ctx context.Context, pool *pgxpool.Pool, tenantID string) ([]User, error) {
	out, err := db.WithTenant[[]User](ctx, pool, tenantID, func(tx pgx.Tx) ([]User, error) {
		rows, err := tx.Query(ctx,
			`SELECT id, tenant_id, email, password_hash, role, COALESCE(totp_secret,''), totp_enabled, created_at
			 FROM users ORDER BY created_at`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var res []User
		for rows.Next() {
			var u User
			if err := rows.Scan(&u.ID, &u.TenantID, &u.Email, &u.PasswordHash, &u.Role, &u.TOTPSecret, &u.TOTPEnabled, &u.CreatedAt); err != nil {
				return nil, err
			}
			res = append(res, u)
		}
		return res, rows.Err()
	})
	return out, err
}

// GetUserTOTPSecret returns a user's enrolled MFA secret ("" when none).
func GetUserTOTPSecret(ctx context.Context, pool *pgxpool.Pool, tenantID, userID string) (string, error) {
	var secret string
	_, err := db.WithTenant[struct{}](ctx, pool, tenantID, func(tx pgx.Tx) (struct{}, error) {
		err := tx.QueryRow(ctx,
			`SELECT COALESCE(totp_secret,'') FROM users WHERE id=$1 AND tenant_id=$2`, userID, tenantID,
		).Scan(&secret)
		if errors.Is(err, pgx.ErrNoRows) {
			return struct{}{}, ErrNotFound
		}
		return struct{}{}, err
	})
	if err != nil {
		return "", err
	}
	return secret, nil
}

// SetTOTPSecret stores a user's MFA secret (tenant-scoped). Returns ErrNotFound
// when the user does not exist in the tenant.
func SetTOTPSecret(ctx context.Context, pool *pgxpool.Pool, tenantID, userID, secret string) error {
	return db.WithTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE users SET totp_secret=$1 WHERE id=$2 AND tenant_id=$3`, secret, userID, tenantID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// SetTOTPEnabled toggles whether a user must supply a code at login.
func SetTOTPEnabled(ctx context.Context, pool *pgxpool.Pool, tenantID, userID string, enabled bool) error {
	return db.WithTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE users SET totp_enabled=$1 WHERE id=$2 AND tenant_id=$3`, enabled, userID, tenantID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
}
