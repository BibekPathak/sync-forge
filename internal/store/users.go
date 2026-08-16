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
// hashes; the raw credential is never persisted.
type User struct {
	ID           string    `json:"id"`
	TenantID     string    `json:"tenant_id"`
	Email        string    `json:"email"`
	PasswordHash string    `json:"-"`
	Role         string    `json:"role"`
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
			 RETURNING id, tenant_id, email, password_hash, role, created_at`,
			tenantID, email, passwordHash, role,
		).Scan(&u.ID, &u.TenantID, &u.Email, &u.PasswordHash, &u.Role, &u.CreatedAt)
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
			`SELECT id, tenant_id, email, password_hash, role, created_at
			 FROM users WHERE tenant_id=$1 AND email=$2`, tenantID, email,
		).Scan(&u.ID, &u.TenantID, &u.Email, &u.PasswordHash, &u.Role, &u.CreatedAt)
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
			`SELECT id, tenant_id, email, password_hash, role, created_at
			 FROM users ORDER BY created_at`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var res []User
		for rows.Next() {
			var u User
			if err := rows.Scan(&u.ID, &u.TenantID, &u.Email, &u.PasswordHash, &u.Role, &u.CreatedAt); err != nil {
				return nil, err
			}
			res = append(res, u)
		}
		return res, rows.Err()
	})
	return out, err
}
