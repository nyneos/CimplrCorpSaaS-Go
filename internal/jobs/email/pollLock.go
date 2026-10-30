package emailjobs

import (
	"context"
	"time"

	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

const emailPollCycleLimit = 5 * time.Minute

// withEmailPollLock runs fn only when this process wins the named advisory lock.
// The lock transaction stays open for the whole cycle so a second replica skips
// instead of polling the same mailbox cursor.
func withEmailPollLock(ctx context.Context, pool *pgxpool.Pool, lockName string, fn func(context.Context) error) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var acquired bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext($1))`, lockName).Scan(&acquired); err != nil {
		return err
	}
	if !acquired {
		logger.LogInfo("[email-poll] %s already running on another instance", lockName)
		return nil
	}
	return fn(ctx)
}

func runPollCycle(ctx context.Context, pool *pgxpool.Pool, lockName string, fn func(context.Context) error) error {
	runCtx, cancel := context.WithTimeout(ctx, emailPollCycleLimit)
	defer cancel()
	return withEmailPollLock(runCtx, pool, lockName, fn)
}
