package fdReceipt

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const MatchTolerancePct = 1.0

// ReceiptRow holds minimal receipt data used during reconciliation.
type ReceiptRow struct {
	ReceiptID   string
	FDID        string
	FdRefNo     string
	EntityID    string
	ReceiptDate time.Time
	Gross       float64
	TDS         float64
	Net         float64
}

// runReconciliation executes the full reconciliation loop for a given run ID.
func runReconciliation(ctx context.Context, pool *pgxpool.Pool, runID string) error {
	// Step 1: Fetch run metadata
	var entityID, periodStart, periodEnd string
	err := pool.QueryRow(ctx, `
		SELECT entity_id, period_start::text, period_end::text
		FROM investment.fd_receipt_reconcile_run
		WHERE reconcile_run_id=$1`, runID).Scan(&entityID, &periodStart, &periodEnd)
	if err != nil {
		return fmt.Errorf("fetch run metadata: %w", err)
	}

	// Step 2: Load all APPROVED receipts for this entity + period
	rows, err := pool.Query(ctx, `
		SELECT receipt_id, fd_id, fd_ref_no, entity_id,
		       receipt_date, gross_interest_received, tds_amount_deducted, net_amount_received
		FROM investment.fd_interest_receipt
		WHERE entity_id=$1
		  AND receipt_date BETWEEN $2::date AND $3::date
		  AND receipt_status='APPROVED'
		  AND is_deleted=false`, entityID, periodStart, periodEnd)
	if err != nil {
		return fmt.Errorf("load receipts: %w", err)
	}
	defer rows.Close()

	var receipts []ReceiptRow
	for rows.Next() {
		var rec ReceiptRow
		if sErr := rows.Scan(&rec.ReceiptID, &rec.FDID, &rec.FdRefNo, &rec.EntityID,
			&rec.ReceiptDate, &rec.Gross, &rec.TDS, &rec.Net); sErr != nil {
			continue
		}
		receipts = append(receipts, rec)
	}
	if rows.Err() != nil {
		return fmt.Errorf("receipts scan error: %w", rows.Err())
	}

	// Step 3: Process each receipt
	matched, unmatched, exceptions := 0, 0, 0
	for _, rec := range receipts {
		// Try to find matching cashflow schedule entry
		cashflowAmt, cashflowID := findMatchingCashflow(ctx, pool, rec)

		// Try to find matching accrual ledger entry
		accrualAmt, accrualID := findMatchingAccrualLedger(ctx, pool, rec)

		// Step 4: Compare and classify
		matchStatus, variance, variancePct := classify(rec.Gross, cashflowAmt, accrualAmt)

		hasException := variancePct > MatchTolerancePct
		if matchStatus == "MISSING_CASHFLOW" || matchStatus == "MISSING_RECEIPT" || matchStatus == "OVER_RECEIVED" {
			hasException = true
		}

		// Step 5: Insert result
		resultID, rErr := insertReconcileResult(ctx, pool, rec, runID, matchStatus, variance, variancePct, hasException, cashflowID, accrualID, cashflowAmt, accrualAmt)
		if rErr != nil {
			continue
		}

		// Step 6: Mark cashflow cleared
		if cashflowID != "" {
			pool.Exec(ctx, `UPDATE investment.fd_cashflow_schedule SET receipt_cleared=true WHERE cashflow_id=$1`, cashflowID) //nolint:errcheck
		}

		// Step 7: Insert exception if needed
		if hasException {
			insertException(ctx, pool, rec, runID, resultID, matchStatus, variance, variancePct) //nolint:errcheck
			exceptions++
		}

		if matchStatus == "MATCHED" {
			matched++
		} else {
			unmatched++
		}
	}

	// Step 8: Handle missing receipts (cashflows with no receipt)
	missingRows, mErr := pool.Query(ctx, `
		SELECT cashflow_id, fd_id, COALESCE(fd_ref_no,''), entity_id, net_cash_flow
		FROM investment.fd_cashflow_schedule
		JOIN investment.fd_master fm ON fm.fd_id = fd_cashflow_schedule.fd_id
		WHERE entity_id=$1
		  AND event_type='INTEREST_RECEIPT'
		  AND COALESCE(receipt_cleared,false)=false
		  AND event_date BETWEEN $2::date AND $3::date
		  AND COALESCE(fd_cashflow_schedule.is_deleted,false)=false`, entityID, periodStart, periodEnd)
	if mErr == nil {
		defer missingRows.Close()
		for missingRows.Next() {
			var cfID, fdID, fdRef, entID string
			var schAmt float64
			if sErr := missingRows.Scan(&cfID, &fdID, &fdRef, &entID, &schAmt); sErr != nil {
				continue
			}
			rec := ReceiptRow{FDID: fdID, FdRefNo: fdRef, EntityID: entID, Gross: schAmt}
			resultID, rErr := insertReconcileResult(ctx, pool, rec, runID, "MISSING_RECEIPT", schAmt, 100.0, true, cfID, "", schAmt, 0)
			if rErr == nil {
				insertException(ctx, pool, rec, runID, resultID, "MISSING_RECEIPT", schAmt, 100.0) //nolint:errcheck
				exceptions++
			}
		}
	}

	// Step 9: Update run status to COMPLETED
	_, err = pool.Exec(ctx, `
		UPDATE investment.fd_receipt_reconcile_run
		SET run_status='COMPLETED',
		    completed_at=now(),
		    receipts_processed=$1,
		    receipts_matched=$2,
		    receipts_unmatched=$3,
		    receipts_exception=$4
		WHERE reconcile_run_id=$5`,
		len(receipts), matched, unmatched, exceptions, runID)

	return err
}

// findMatchingCashflow attempts to match a receipt to a cashflow schedule row.
func findMatchingCashflow(ctx context.Context, pool *pgxpool.Pool, rec ReceiptRow) (float64, string) {
	var cfID string
	var schAmt float64
	err := pool.QueryRow(ctx, `
		SELECT cashflow_id, net_cash_flow
		FROM investment.fd_cashflow_schedule
		WHERE fd_id=$1
		  AND event_type='INTEREST_RECEIPT'
		  AND COALESCE(receipt_cleared,false)=false
		  AND is_deleted=false
		ORDER BY ABS(EXTRACT(EPOCH FROM (event_date - $2::date)))
		LIMIT 1`, rec.FDID, rec.ReceiptDate).Scan(&cfID, &schAmt)
	if err != nil {
		return 0, ""
	}
	return schAmt, cfID
}

// findMatchingAccrualLedger finds the corresponding accrual ledger entry.
func findMatchingAccrualLedger(ctx context.Context, pool *pgxpool.Pool, rec ReceiptRow) (float64, string) {
	var ledgerID string
	var accrualAmt float64
	err := pool.QueryRow(ctx, `
		SELECT ledger_id, period_interest_accrued
		FROM investment.fd_accrual_ledger
		WHERE fd_id=$1
		  AND ledger_row_status='CALCULATED'
		  AND is_deleted=false
		ORDER BY ABS(EXTRACT(EPOCH FROM (accrual_period_end - $2::date)))
		LIMIT 1`, rec.FDID, rec.ReceiptDate).Scan(&ledgerID, &accrualAmt)
	if err != nil {
		return 0, ""
	}
	return accrualAmt, ledgerID
}

// classify determines match status and variance from gross vs expected amounts.
func classify(gross, cashflowAmt, accrualAmt float64) (string, float64, float64) {
	expected := cashflowAmt
	if expected == 0 {
		expected = accrualAmt
	}
	if expected == 0 {
		return "MISSING_CASHFLOW", gross, 100.0
	}
	variance := gross - expected
	var variancePct float64
	if expected != 0 {
		variancePct = (variance / expected) * 100
		if variancePct < 0 {
			variancePct = -variancePct
		}
	}
	if variancePct <= MatchTolerancePct {
		return "MATCHED", variance, variancePct
	}
	if gross > expected {
		return "OVER_RECEIVED", variance, variancePct
	}
	return "UNDER_RECEIVED", variance, variancePct
}

// insertReconcileResult writes one reconciliation result row and returns its ID.
func insertReconcileResult(ctx context.Context, pool *pgxpool.Pool,
	rec ReceiptRow, runID, matchStatus string,
	variance, variancePct float64, hasException bool,
	cashflowID, accrualID string,
	cashflowAmt, accrualAmt float64) (string, error) {

	var resultID string
	err := pool.QueryRow(ctx, `
		INSERT INTO investment.fd_receipt_reconcile_result (
			result_id, reconcile_run_id, receipt_id, fd_id, fd_ref_no, entity_id,
			cashflow_id, accrual_id,
			receipt_amount, cashflow_amount, accrual_amount,
			variance_amount, variance_pct,
			match_status, has_exception,
			reconciled_at
		) VALUES (
			'RRES-' || UPPER(SUBSTR(REPLACE(gen_random_uuid()::TEXT,'-',''),1,7)),
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,now()
		) RETURNING result_id`,
		runID,
		nullStr(rec.ReceiptID), rec.FDID, rec.FdRefNo, rec.EntityID,
		nullStr(cashflowID), nullStr(accrualID),
		rec.Gross, cashflowAmt, accrualAmt,
		variance, variancePct,
		matchStatus, hasException,
	).Scan(&resultID)
	return resultID, err
}

// insertException creates an exception record for a result with variance/missing.
func insertException(ctx context.Context, pool *pgxpool.Pool,
	rec ReceiptRow, runID, resultID, exceptionType string,
	variance, variancePct float64) error {

	severity := "WARNING"
	if exceptionType == "OVER_RECEIVED" || exceptionType == "MISSING_RECEIPT" {
		severity = "BLOCKER"
	}

	_, err := pool.Exec(ctx, `
		INSERT INTO investment.fd_receipt_exception (
			exception_id, reconcile_run_id, result_id,
			receipt_id, fd_id, fd_ref_no, entity_id,
			exception_type, severity,
			variance_amount, variance_pct,
			exception_status, raised_at,
			is_deleted
		) VALUES (
			'IREX-' || UPPER(SUBSTR(REPLACE(gen_random_uuid()::TEXT,'-',''),1,7)),
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'OPEN',now(),false
		)`,
		runID, nullStr(resultID),
		nullStr(rec.ReceiptID), rec.FDID, rec.FdRefNo, rec.EntityID,
		exceptionType, severity,
		variance, variancePct,
	)
	return err
}
