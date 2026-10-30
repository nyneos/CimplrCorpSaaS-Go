package jobs

import (
	fdReceipt "CimplrCorpSaas/api/investment/fdReceipt"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
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

	// One cycle at a time across replicas and restarts. Transaction-scoped lock on a
	// held connection, which stays correct on the Supabase transaction pooler.
	lockConn, err := db.Acquire(ctx)
	if err != nil {
		logger.LogError("[fd_receipt_scheduler] acquire lock connection: %v", err)
		return
	}
	lockTx, err := lockConn.Begin(ctx)
	if err != nil {
		lockConn.Release()
		logger.LogError("[fd_receipt_scheduler] begin lock transaction: %v", err)
		return
	}
	defer func() {
		_ = lockTx.Rollback(context.WithoutCancel(ctx))
		lockConn.Release()
	}()
	var locked bool
	if err := lockTx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext('fd-receipt-auto-reconcile'))`).Scan(&locked); err != nil {
		logger.LogError("[fd_receipt_scheduler] advisory lock: %v", err)
		return
	}
	if !locked {
		logger.LogInfo("[fd_receipt_scheduler] another instance is reconciling — skipping")
		return
	}

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
		if scanErr := rows.Scan(&e.EntityID, &e.EntityName, &e.PeriodStart, &e.PeriodEnd); scanErr != nil {
			logger.LogError("[fd_receipt_scheduler] entity scan error: %v", scanErr)
			continue
		}
		entities = append(entities, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		logger.LogError("[fd_receipt_scheduler] entity query error: %v", err)
		return
	}

	for _, e := range entities {
		// Do not pile up another run while one for this entity is still running.
		var running bool
		if err := db.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM investment.fd_receipt_reconcile_run
			               WHERE entity_id=$1 AND run_status='RUNNING')`, e.EntityID,
		).Scan(&running); err != nil {
			logger.LogError("[fd_receipt_scheduler] running-run check for %s: %v", e.EntityID, err)
			continue
		}
		if running {
			logger.LogInfo("[fd_receipt_scheduler] entity=%s already has a RUNNING reconcile — skipping", e.EntityID)
			continue
		}
		runID := fmt.Sprintf("RRUN-AUTO-%s", strings.ToUpper(strings.ReplaceAll(uuid.NewString(), "-", "")[:12]))
		pendingCount := 0
		if err := db.QueryRow(ctx, `
			SELECT COUNT(*) FROM investment.fd_interest_receipt
			WHERE entity_id=$1 AND is_deleted=false
			  AND reconcile_status IN ('PENDING','UNMATCHED')`, e.EntityID,
		).Scan(&pendingCount); err != nil {
			logger.LogError("[fd_receipt_scheduler] pending count for %s: %v", e.EntityID, err)
			continue
		}
		if ok, msg := fdReceipt.EnforceReceiptReconcileRun(ctx, db, runID, fdReceipt.ReceiptReconcileEntity{ID: e.EntityID, Name: e.EntityName}, e.PeriodStart, e.PeriodEnd, pendingCount); !ok {
			logger.LogError("[fd_receipt_scheduler] policy blocked entity=%s: %s", e.EntityID, msg)
			continue
		}
		tag, err := db.Exec(ctx, `
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
		if tag.RowsAffected() != 1 {
			logger.LogError("[fd_receipt_scheduler] run row %s was not inserted for %s", runID, e.EntityID)
			continue
		}
		if err := fdReceipt.RunScheduledReconciliation(ctx, db, runID); err != nil {
			logger.LogError("[fd_receipt_scheduler] reconcile failed run=%s entity=%s: %v", runID, e.EntityID, err)
			continue
		}
		logger.LogInfo("[fd_receipt_scheduler] auto-reconcile completed: run=%s entity=%s", runID, e.EntityID)
		notifcatalog.TriggerNotification(ctx, db,
			"/investment/fd/receipt/reconcile/auto-run",
			runID,
			map[string]interface{}{
				"entity_id":    e.EntityID,
				"entity_name":  e.EntityName,
				"record_id":    runID,
				"event":        "FD_RECEIPT_RECONCILIATION_AUTO_RUN",
				"actor_email":  "system@cimplr.auto",
				"period_start": e.PeriodStart.Format("2006-01-02"),
				"period_end":   e.PeriodEnd.Format("2006-01-02"),
				"trigger_mode": "AUTO",
			})
	}
	logger.LogInfo("[fd_receipt_scheduler] cycle complete — %d entity(-ies) processed", len(entities))
}
