package jobs

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

// StartReceiptReconcileWorker runs a daily background job that auto-triggers
// receipt reconciliation for all active entities that have CAPTURED or
// APPROVAL_PENDING receipts which have not yet been reconciled.
func StartReceiptReconcileWorker(db *pgxpool.Pool) {
	logger.LogInfo("[fd_receipt_scheduler] Worker started")
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	// Run immediately on first tick via a manual trigger.
	runAutoReconcile(db)

	for range ticker.C {
		runAutoReconcile(db)
	}
}

func runAutoReconcile(db *pgxpool.Pool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Fetch distinct entity_ids that have un-reconciled receipts.
	rows, err := db.Query(ctx, `
		SELECT DISTINCT entity_id, entity_name,
		       MIN(receipt_date) AS period_start,
		       MAX(receipt_date) AS period_end
		FROM investment.fd_interest_receipt
		WHERE is_deleted=false
		  AND reconcile_status IN ('PENDING','UNMATCHED')
		GROUP BY entity_id, entity_name`)
	if err != nil {
		logger.LogError("[fd_receipt_scheduler] entity query error: %v", err)
		return
	}
	defer rows.Close()

	type entityRow struct {
		EntityID    string
		EntityName  string
		PeriodStart time.Time
		PeriodEnd   time.Time
	}
	var entities []entityRow
	for rows.Next() {
		var e entityRow
		if scanErr := rows.Scan(&e.EntityID, &e.EntityName, &e.PeriodStart, &e.PeriodEnd); scanErr == nil {
			entities = append(entities, e)
		}
	}
	rows.Close()

	for _, e := range entities {
		runID := fmt.Sprintf("RRUN-AUTO-%d", time.Now().UnixMilli())
		_, err := db.Exec(ctx, `
			INSERT INTO investment.fd_receipt_reconcile_run
			  (reconcile_run_id, entity_id, entity_name, period_start, period_end,
			   run_status, trigger_mode, triggered_by, triggered_at)
			VALUES ($1,$2,$3,$4,$5,'RUNNING','MANUAL','system',NOW())
			ON CONFLICT DO NOTHING`,
			runID, e.EntityID, e.EntityName, e.PeriodStart, e.PeriodEnd,
		)
		if err != nil {
			logger.LogError("[fd_receipt_scheduler] insert run row error for %s: %v", e.EntityID, err)
			continue
		}
		logger.LogInfo("[fd_receipt_scheduler] auto-reconcile triggered: run=%s entity=%s", runID, e.EntityID)
	}
	logger.LogInfo("[fd_receipt_scheduler] cycle complete — %d entity(-ies) processed", len(entities))
}
