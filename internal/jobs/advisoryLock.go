package jobs

import (
	"context"

	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

// WithAdvisoryLock runs fn only if this process wins a Postgres advisory lock.
//
// Uses pg_try_advisory_xact_lock inside an open transaction on a held connection.
// That stays correct on Supabase/PgBouncer transaction poolers (port 6543), where
// session-level pg_try_advisory_lock does not reliably stick across statements.
//
// fn may use other pool connections; the lock connection stays checked out with
// an open transaction until fn returns, then the lock is released on rollback.
//
// Returns true if fn ran, false if the lock was already held elsewhere.
func WithAdvisoryLock(ctx context.Context, pool *pgxpool.Pool, lockName string, fn func()) (ran bool) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		logger.LogError("[advisory-lock] acquire connection for lock=%q: %v", lockName, err)
		return false
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		conn.Release()
		logger.LogError("[advisory-lock] begin for lock=%q: %v", lockName, err)
		return false
	}

	var acquired bool
	if err := tx.QueryRow(ctx,
		`SELECT pg_try_advisory_xact_lock(hashtext($1))`, lockName,
	).Scan(&acquired); err != nil {
		_ = tx.Rollback(ctx)
		conn.Release()
		logger.LogError("[advisory-lock] pg_try_advisory_xact_lock for lock=%q: %v", lockName, err)
		return false
	}

	if !acquired {
		_ = tx.Rollback(ctx)
		conn.Release()
		logger.LogInfo("[advisory-lock] lock=%q already held by another instance — skipping", lockName)
		return false
	}

	defer func() {
		_ = tx.Rollback(ctx) // ends tx → releases xact advisory lock
		conn.Release()
	}()

	fn()
	return true
}
