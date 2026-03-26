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
	var entityID, periodStart, periodEnd, matchingBasis, triggeredBy string
	err := pool.QueryRow(ctx, `
		SELECT entity_id, period_start::text, period_end::text,
		       COALESCE(matching_basis,'BOTH'), COALESCE(triggered_by,'system')
		FROM investment.fd_receipt_reconcile_run
		WHERE reconcile_run_id=$1`, runID).Scan(&entityID, &periodStart, &periodEnd, &matchingBasis, &triggeredBy)
	if err != nil {
		return fmt.Errorf("fetch run metadata: %w", err)
	}

	// Step 2: Load all APPROVED/POSTED receipts for this entity + period
	rows, err := pool.Query(ctx, `
		SELECT receipt_id, fd_id, fd_ref_no, entity_id,
		       receipt_date, gross_interest_received, tds_amount_deducted, net_amount_received
		FROM investment.fd_interest_receipt
		WHERE entity_id=$1
		  AND receipt_date BETWEEN $2::date AND $3::date
		  AND receipt_status IN ('APPROVED','POSTED')
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

	if len(receipts) == 0 {
		var totalCount, approvedCount int
		pool.QueryRow(ctx, `
			SELECT COUNT(*), COUNT(*) FILTER (WHERE receipt_status IN ('APPROVED','POSTED'))
			FROM investment.fd_interest_receipt
			WHERE entity_id=$1 AND receipt_date BETWEEN $2::date AND $3::date
			  AND is_deleted=false`,
			entityID, periodStart, periodEnd,
		).Scan(&totalCount, &approvedCount)
		return fmt.Errorf("no receipts to reconcile for entity=%s period=%s to %s: total=%d approved=%d",
			entityID, periodStart, periodEnd, totalCount, approvedCount)
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
		if matchStatus == "EXCEPTION" {
			hasException = true
		}

		// Step 5: Insert result
		resultID, rErr := insertReconcileResult(ctx, pool, rec, runID, matchStatus, matchingBasis, variance, variancePct, hasException, cashflowID, accrualID, cashflowAmt, rec.Gross)
		if rErr != nil {
			continue
		}

		// Step 6: Mark cashflow cleared and update receipt reconcile status
		if cashflowID != "" {
			if _, execErr := pool.Exec(ctx,
				`UPDATE investment.fd_cashflow_schedule SET receipt_cleared=true WHERE cashflow_id=$1`,
				cashflowID,
			); execErr != nil {
				fmt.Printf("[Reconcile] WARN: cashflow cleared update failed run=%s cf=%s: %v\n", runID, cashflowID, execErr)
			}
		}
		reconcileStatus := "MATCHED"
		if matchStatus != "MATCHED" {
			reconcileStatus = "UNMATCHED"
		}
		if _, execErr := pool.Exec(ctx,
			`UPDATE investment.fd_interest_receipt SET reconcile_status=$1, reconcile_run_id=$2 WHERE receipt_id=$3`,
			reconcileStatus, runID, rec.ReceiptID,
		); execErr != nil {
			fmt.Printf("[Reconcile] WARN: receipt reconcile_status update failed run=%s receipt=%s: %v\n", runID, rec.ReceiptID, execErr)
		}

		// Step 7: Insert exception if needed
		if hasException {
			exID, _ := insertException(ctx, pool, rec, runID, resultID, matchStatus, cashflowAmt, rec.Gross, triggeredBy)
			if exID != "" {
				if _, execErr := pool.Exec(ctx,
					`UPDATE investment.fd_receipt_reconcile_result SET exception_id=$1 WHERE result_id=$2`,
					exID, resultID,
				); execErr != nil {
					fmt.Printf("[Reconcile] WARN: exception_id update failed run=%s result=%s: %v\n", runID, resultID, execErr)
				}
			}
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
		SELECT cs.cashflow_id, cs.fd_id,
		       COALESCE(fm.bank_fd_ref_no, '') AS fd_ref_no,
		       fm.entity_id,
		       cs.net_cash_flow,
		       cs.event_date
		FROM investment.fd_cashflow_schedule cs
		JOIN investment.fd_master fm ON fm.fd_id = cs.fd_id
		WHERE fm.entity_id = $1
		  AND cs.event_type IN ('INTEREST_RECEIPT', 'MATURITY')
		  AND COALESCE(cs.receipt_cleared, false) = false
		  AND cs.event_date BETWEEN $2::date AND $3::date
		  AND COALESCE(cs.is_deleted, false) = false
		  AND COALESCE(fm.is_deleted, false) = false`,
		entityID, periodStart, periodEnd)
	if mErr == nil {
		defer missingRows.Close()
		for missingRows.Next() {
			var cfID, fdID, fdRef, entID string
			var schAmt float64
			var eventDate time.Time
			if sErr := missingRows.Scan(&cfID, &fdID, &fdRef, &entID, &schAmt, &eventDate); sErr != nil {
				continue
			}
			rec := ReceiptRow{FDID: fdID, FdRefNo: fdRef, EntityID: entID, Gross: schAmt, ReceiptDate: eventDate}
			resultID, rErr := insertReconcileResult(ctx, pool, rec, runID, "UNMATCHED", matchingBasis, schAmt, 100.0, true, cfID, "", schAmt, 0)
			if rErr == nil {
				exID, _ := insertException(ctx, pool, rec, runID, resultID, "MISSING_RECEIPT", schAmt, 0, triggeredBy)
				if exID != "" {
					if _, execErr := pool.Exec(ctx,
						`UPDATE investment.fd_receipt_reconcile_result SET exception_id=$1 WHERE result_id=$2`,
						exID, resultID,
					); execErr != nil {
						fmt.Printf("[Reconcile] WARN: missing-receipt exception_id update failed run=%s result=%s: %v\n", runID, resultID, execErr)
					}
				}
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
// It tries three strategies in order:
//  1. INTEREST_RECEIPT row closest to receipt date (periodic payout FDs)
//  2. Sum of ACCRUAL rows whose period_end is within 45 days before receipt date (cumulative FDs)
//  3. MATURITY row within 7 days of receipt date
func findMatchingCashflow(ctx context.Context, pool *pgxpool.Pool, rec ReceiptRow) (float64, string) {
	// Strategy 1: Look for an INTEREST_RECEIPT cashflow row close to receipt date
	var cfID string
	var schAmt float64
	err := pool.QueryRow(ctx, `
		SELECT cashflow_id, net_cash_flow
		FROM investment.fd_cashflow_schedule
		WHERE fd_id = $1
		  AND event_type = 'INTEREST_RECEIPT'
		  AND COALESCE(receipt_cleared, false) = false
		  AND COALESCE(is_deleted, false) = false
		ORDER BY ABS((event_date - $2::date))
		LIMIT 1`, rec.FDID, rec.ReceiptDate).Scan(&cfID, &schAmt)
	if err == nil && cfID != "" {
		return schAmt, cfID
	}

	// Strategy 2: For cumulative FDs — sum ACCRUAL rows whose period_end falls
	// within 45 days before the receipt date.
	var accrualSum float64
	var latestCFID string
	err = pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(interest_accrued), 0),
		       COALESCE(MAX(cashflow_id), '')
		FROM investment.fd_cashflow_schedule
		WHERE fd_id = $1
		  AND event_type = 'ACCRUAL'
		  AND period_end BETWEEN ($2::date - INTERVAL '45 days') AND $2::date
		  AND COALESCE(is_deleted, false) = false
		  AND COALESCE(receipt_cleared, false) = false`,
		rec.FDID, rec.ReceiptDate).Scan(&accrualSum, &latestCFID)
	if err == nil && accrualSum > 0 {
		return accrualSum, latestCFID
	}

	// Strategy 3: Check if receipt date matches a MATURITY row
	var maturityCFID string
	var maturityAmt float64
	err = pool.QueryRow(ctx, `
		SELECT cashflow_id, net_cash_flow
		FROM investment.fd_cashflow_schedule
		WHERE fd_id = $1
		  AND event_type = 'MATURITY'
		  AND ABS((event_date - $2::date)) < 7
		  AND COALESCE(is_deleted, false) = false
		LIMIT 1`, rec.FDID, rec.ReceiptDate).Scan(&maturityCFID, &maturityAmt)
	if err == nil && maturityCFID != "" {
		return maturityAmt, maturityCFID
	}

	// No match found
	return 0, ""
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
		ORDER BY ABS((accrual_period_end - $2::date))
		LIMIT 1`, rec.FDID, rec.ReceiptDate).Scan(&ledgerID, &accrualAmt)
	if err != nil {
		return 0, ""
	}
	return accrualAmt, ledgerID
}

// classify determines match status and variance from gross vs expected amounts.
// Returns only valid match_status values: MATCHED, PARTIAL, UNMATCHED, EXCEPTION
func classify(gross, cashflowAmt, accrualAmt float64) (string, float64, float64) {
	expected := cashflowAmt
	if expected == 0 {
		expected = accrualAmt
	}
	if expected == 0 {
		// No cashflow or accrual found — flag as exception
		return "EXCEPTION", gross, 100.0
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
	// Within 10% = PARTIAL; beyond = UNMATCHED
	if variancePct <= 10.0 {
		return "PARTIAL", variance, variancePct
	}
	return "UNMATCHED", variance, variancePct
}

// insertReconcileResult writes one reconciliation result row and returns its ID.
// Columns match investment.fd_receipt_reconcile_result schema exactly.
func insertReconcileResult(ctx context.Context, pool *pgxpool.Pool,
	rec ReceiptRow, runID, matchStatus, matchingBasis string,
	variance, variancePct float64, hasException bool,
	cashflowID, accrualLedgerID string,
	expectedInterest, receivedInterest float64) (string, error) {

	// Fetch bank_id, bank_name from fd_master for the result row
	var bankID, bankName string
	_ = pool.QueryRow(ctx,
		`SELECT COALESCE(bank_id,''), COALESCE(bank_name,'') FROM investment.fd_master WHERE fd_id=$1 AND is_deleted=false`,
		rec.FDID).Scan(&bankID, &bankName)

	// Derive period from receipt or default
	periodStart := rec.ReceiptDate.AddDate(0, -3, 0)
	periodEnd := rec.ReceiptDate

	// TDS variance
	tdsVariance := rec.TDS // received TDS; expected TDS not tracked separately here

	var resultID string
	err := pool.QueryRow(ctx, `
		INSERT INTO investment.fd_receipt_reconcile_result (
			reconcile_run_id, fd_id, fd_ref_no, receipt_id,
			cashflow_id, accrual_ledger_id,
			entity_id, bank_id, bank_name,
			period_start, period_end, matching_basis,
			expected_interest, expected_tds, expected_net,
			received_interest, received_tds, received_net,
			interest_variance, tds_variance, interest_variance_pct,
			match_status, match_type, has_exception
		) VALUES (
			$1,$2,$3,$4,
			$5,$6,
			$7,$8,$9,
			$10,$11,$12,
			$13,0,$14,
			$15,$16,$17,
			$18,$19,$20,
			$21,'AUTO',$22
		) RETURNING result_id`,
		runID, rec.FDID, rec.FdRefNo, nullStr(rec.ReceiptID),
		nullStr(cashflowID), nullStr(accrualLedgerID),
		rec.EntityID, bankID, bankName,
		periodStart, periodEnd, matchingBasis,
		expectedInterest, expectedInterest,     // expected_net = expected_interest (no TDS expected side)
		receivedInterest, rec.TDS, rec.Net,
		variance, tdsVariance, variancePct,
		matchStatus, hasException,
	).Scan(&resultID)
	return resultID, err
}

// insertException creates an exception record for a result with variance/missing.
// Returns the new exception_id and any error.
func insertException(ctx context.Context, pool *pgxpool.Pool,
	rec ReceiptRow, runID, resultID, exceptionType string,
	expectedAmt, receivedAmt float64,
	raisedBy string) (string, error) {

	severity := "WARNING"
	if exceptionType == "MISSING_RECEIPT" || exceptionType == "EXCEPTION" {
		severity = "BLOCKER"
	}
	variance := receivedAmt - expectedAmt
	if variance < 0 {
		variance = -variance
	}

	var exID string
	err := pool.QueryRow(ctx, `
		INSERT INTO investment.fd_receipt_exception (
			exception_id, reconcile_run_id, result_id,
			receipt_id, fd_id, fd_ref_no,
			exception_type, severity,
			expected_amount, received_amount, variance_amount,
			exception_status, raised_by, raised_at,
			is_active, is_deleted
		) VALUES (
			'IREX-' || UPPER(SUBSTR(REPLACE(gen_random_uuid()::TEXT,'-',''),1,7)),
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'OPEN',$11,now(),true,false
		) RETURNING exception_id`,
		runID, resultID,
		nullStr(rec.ReceiptID), rec.FDID, rec.FdRefNo,
		exceptionType, severity,
		expectedAmt, receivedAmt, variance,
		raisedBy,
	).Scan(&exID)
	return exID, err
}
