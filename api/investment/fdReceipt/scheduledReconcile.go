package fdReceipt

import (
	"context"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RunScheduledReconciliation executes a reconcile run created by the scheduled worker
// over all pending receipts of its entity, and marks the run FAILED when it errors so
// it never stays RUNNING.
func RunScheduledReconciliation(ctx context.Context, pool *pgxpool.Pool, runID string) error {
	if err := runReconciliation(ctx, pool, runID, nil, nil); err != nil {
		if _, uerr := pool.Exec(context.WithoutCancel(ctx),
			`UPDATE investment.fd_receipt_reconcile_run
			 SET run_status='FAILED', error_message=$1, completed_at=now()
			 WHERE reconcile_run_id=$2`, err.Error(), runID); uerr != nil {
			api.LogError("[FDReceipt] scheduled reconcile mark-failed run=%s: %v", runID, uerr)
		}
		return err
	}
	return nil
}
