package jobs

import (
	"context"

	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

// WithAdvisoryLock acquires a PostgreSQL session-level advisory lock keyed by
// lockName before calling fn. If another connection already holds the lock,
// fn is skipped and the caller returns immediately (non-blocking).
//
// The lock is released in a defer before the dedicated connection is returned
// to the pool, so it is never left held on a recycled pool connection.
//
// Usage — wrap any cron/ticker body:
//
//	err := jobs.WithAdvisoryLock(ctx, pool, "amfi-data-downloader", func() {
//	    // only one pod in the cluster runs this at a time
//	    processNAVData(...)
//	})
//
// Returns true if fn was executed, false if the lock was already held elsewhere.
func WithAdvisoryLock(ctx context.Context, pool *pgxpool.Pool, lockName string, fn func()) (ran bool) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		logger.LogError("[advisory-lock] acquire connection for lock=%q: %v", lockName, err)
		return false
	}
	defer func() {
		// Always release the advisory lock before returning the connection.
		conn.QueryRow(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, lockName).Scan(new(bool)) //nolint:errcheck
		conn.Release()
	}()

	var acquired bool
	if err := conn.QueryRow(ctx,
		`SELECT pg_try_advisory_lock(hashtext($1))`, lockName,
	).Scan(&acquired); err != nil {
		logger.LogError("[advisory-lock] pg_try_advisory_lock for lock=%q: %v", lockName, err)
		return false
	}

	if !acquired {
		logger.LogInfo("[advisory-lock] lock=%q already held by another instance — skipping", lockName)
		return false
	}

	fn()
	return true
}
