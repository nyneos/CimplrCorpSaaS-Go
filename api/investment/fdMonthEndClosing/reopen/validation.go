package reopen

import (
	"context"
	"strconv"
	"strings"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

type postReopenValidation struct {
	AccrualValid        bool
	ReconciliationValid bool
	AccountingValid     bool
	Status              string
	Errors              string
}

func runPostReopenChecks(ctx context.Context, pool *pgxpool.Pool, cycleID string) postReopenValidation {
	var total, accrualOpen, reconOpen, reconExceptions, accountingOpen, accrualNonFinal int
	err := pool.QueryRow(ctx, `
		SELECT
			COUNT(*)::int,
			COUNT(*) FILTER (WHERE step_code IN ('ACCRUAL_RUN_COMPLETED','ACCRUAL_RUN_APPROVED') AND status <> 'COMPLETED')::int,
			COUNT(*) FILTER (WHERE step_code IN ('RECEIPTS_CAPTURED','RECEIPTS_RECONCILED','VARIANCES_CLOSED') AND status <> 'COMPLETED')::int,
			COALESCE(SUM(COALESCE(exception_count,0)) FILTER (WHERE step_code IN ('RECEIPTS_RECONCILED','VARIANCES_CLOSED')),0)::int,
			COUNT(*) FILTER (WHERE step_code IN ('TDS_VALIDATED','ACCOUNTING_CONSOLIDATED') AND status <> 'COMPLETED')::int
		FROM investment.fd_closing_checklist_item
		WHERE cycle_id = $1 AND COALESCE(is_deleted,false) = false`,
		cycleID,
	).Scan(&total, &accrualOpen, &reconOpen, &reconExceptions, &accountingOpen)
	if err != nil {
		api.LogError("[FDClosingReopen] post-reopen checklist check failed for cycle %s: %v", cycleID, err)
		return postReopenValidation{Status: "FAILED", Errors: "Unable to evaluate closing checklist: " + err.Error()}
	}

	err = pool.QueryRow(ctx, `
		SELECT COUNT(*)::int
		FROM investment.fd_closing_checklist_item i
		LEFT JOIN investment.fd_accrual_run r
		  ON r.run_id = i.evidence_ref AND COALESCE(r.is_deleted,false) = false
		WHERE i.cycle_id = $1
		  AND COALESCE(i.is_deleted,false) = false
		  AND i.step_code IN ('ACCRUAL_RUN_COMPLETED','ACCRUAL_RUN_APPROVED')
		  AND i.status = 'COMPLETED'
		  AND (r.run_id IS NULL OR UPPER(COALESCE(r.run_mode,'')) <> 'FINAL')`,
		cycleID,
	).Scan(&accrualNonFinal)
	if err != nil {
		api.LogError("[FDClosingReopen] post-reopen accrual run check failed for cycle %s: %v", cycleID, err)
		return postReopenValidation{Status: "FAILED", Errors: "Unable to evaluate accrual runs: " + err.Error()}
	}

	var errs []string
	if total == 0 {
		errs = append(errs, "No closing checklist items found for this cycle")
	}
	if accrualOpen > 0 {
		errs = append(errs, strconv.Itoa(accrualOpen)+" accrual step(s) not completed")
	}
	if accrualNonFinal > 0 {
		errs = append(errs, strconv.Itoa(accrualNonFinal)+" accrual step(s) completed without a FINAL accrual run reference")
	}
	if reconOpen > 0 {
		errs = append(errs, strconv.Itoa(reconOpen)+" reconciliation step(s) not completed")
	}
	if reconExceptions > 0 {
		errs = append(errs, strconv.Itoa(reconExceptions)+" open reconciliation exception(s)")
	}
	if accountingOpen > 0 {
		errs = append(errs, strconv.Itoa(accountingOpen)+" accounting step(s) not completed")
	}

	out := postReopenValidation{
		AccrualValid:        total > 0 && accrualOpen == 0 && accrualNonFinal == 0,
		ReconciliationValid: total > 0 && reconOpen == 0 && reconExceptions == 0,
		AccountingValid:     total > 0 && accountingOpen == 0,
		Status:              "COMPLETED",
		Errors:              strings.Join(errs, "; "),
	}
	if len(errs) > 0 {
		out.Status = "FAILED"
	}
	return out
}
