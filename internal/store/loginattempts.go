package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"syncforge/internal/db"
)

// LoginAttempt is one recorded login attempt. Used for account lockout.
type LoginAttempt struct {
	ID          string    `json:"id"`
	TenantID    string    `json:"tenant_id"`
	Email       string    `json:"email"`
	IP          string    `json:"ip"`
	Success     bool      `json:"success"`
	AttemptedAt time.Time `json:"attempted_at"`
}

// RecordLoginAttempt appends a login attempt. Failures drive lockout; successes
// are recorded for audit but do not reset the failure history on their own (a
// fresh lockout window simply starts from the next failure).
func RecordLoginAttempt(ctx context.Context, pool *pgxpool.Pool, a LoginAttempt) error {
	return db.WithTenantTx(ctx, pool, a.TenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO login_attempts (tenant_id, email, ip, success)
			 VALUES ($1,$2,$3,$4)`,
			a.TenantID, a.Email, a.IP, a.Success)
		return err
	})
}

// CountRecentFailures returns how many failed attempts occurred for an account
// within the last window.
func CountRecentFailures(ctx context.Context, pool *pgxpool.Pool, tenantID, email string, window time.Duration) (int, error) {
	var n int
	_, err := db.WithTenant[struct{}](ctx, pool, tenantID, func(tx pgx.Tx) (struct{}, error) {
		err := tx.QueryRow(ctx,
			`SELECT count(*) FROM login_attempts
			 WHERE tenant_id=$1 AND email=$2 AND success=false AND attempted_at > now() - $3::interval`,
			tenantID, email, window.String(),
		).Scan(&n)
		return struct{}{}, err
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// ClearLoginFailures removes an account's failure history after a successful
// login, so the lockout counter starts fresh.
func ClearLoginFailures(ctx context.Context, pool *pgxpool.Pool, tenantID, email string) error {
	return db.WithTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`DELETE FROM login_attempts WHERE tenant_id=$1 AND email=$2 AND success=false`,
			tenantID, email)
		return err
	})
}
