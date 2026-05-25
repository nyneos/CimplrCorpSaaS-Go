package fdReceipt

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/varianceengine"

	"github.com/jackc/pgx/v5/pgxpool"
)

const MatchTolerancePct = 1.0 // <= 1%  → MATCHED
const MatchPartialPct = 10.0  // <= 10% → PARTIAL  (> 10% → UNMATCHED)

// ReceiptRow holds minimal interest receipt data used during reconciliation.
type ReceiptRow struct {
	ReceiptID   string
	FDID        string
	FdRefNo     string
	EntityID    string
	EntityName  string
	BankID      string
	BankName    string
	PeriodStart time.Time
	PeriodEnd   time.Time
	ReceiptDate time.Time
	Gross       float64
	TDS         float64
	Net         float64
}

// TDSRow holds minimal TDS receipt data used during reconciliation.
type TDSRow struct {
	TDSID         string
	ReceiptID     string
	FDID          string
	FdRefNo       string
	EntityID      string
	EntityName    string
	BankID        string
	BankName      string
	PeriodStart   time.Time
	PeriodEnd     time.Time
	DeductionDate time.Time
	Actual        float64
}

// ─── Preview result types ─────────────────────────────────────────────────────

// CashflowLine is a single fd_cashflow_schedule row returned inside a preview line.
type CashflowLine struct {
	CashflowID       string  `json:"cashflow_id"`
	EventType        string  `json:"event_type"`
	EventDate        string  `json:"event_date"`
	PeriodStartDate  string  `json:"period_start_date"`
	PeriodEndDate    string  `json:"period_end_date"`
	PeriodDays       int     `json:"period_days"`
	OpeningPrincipal float64 `json:"opening_principal"`
	InterestAccrued  float64 `json:"interest_accrued"`
	TDSAmount        float64 `json:"tds_amount"`
	NetCashFlow      float64 `json:"net_cash_flow"`
	PostingStatus    string  `json:"posting_status"`
	Amount           float64 `json:"amount"` // the value used for matching (CASE-selected)
}

// AccrualLedgerLine is a single fd_accrual_ledger row returned inside a preview line.
type AccrualLedgerLine struct {
	LedgerID              string  `json:"ledger_id"`
	AccrualPeriodStart    string  `json:"accrual_period_start"`
	AccrualPeriodEnd      string  `json:"accrual_period_end"`
	AccrualDays           int     `json:"accrual_days"`
	PrincipalAmount       float64 `json:"principal_amount"`
	InterestRate          float64 `json:"interest_rate"`
	PeriodInterestAccrued float64 `json:"period_interest_accrued"`
	TDSDeductedInPeriod   float64 `json:"tds_deducted_in_period"`
	OpeningAccruedBalance float64 `json:"opening_accrued_balance"`
	ClosingAccruedBalance float64 `json:"closing_accrued_balance"`
	LedgerRowStatus       string  `json:"ledger_row_status"`
	Amount                float64 `json:"amount"` // the value used for matching (interest or TDS)
}

// ReconcilePreviewLine is one receipt's projected result (no DB writes).
type ReconcilePreviewLine struct {
	ReceiptID  string `json:"receipt_id,omitempty"`
	TDSID      string `json:"tds_id,omitempty"`
	ResultType string `json:"result_type"`
	FDID       string `json:"fd_id"`
	FdRefNo    string `json:"fd_ref_no"`
	// FD / entity / bank metadata
	EntityID       string  `json:"entity_id"`
	EntityName     string  `json:"entity_name"`
	BankID         string  `json:"bank_id"`
	BankName       string  `json:"bank_name"`
	PeriodStart    string  `json:"period_start"`
	PeriodEnd      string  `json:"period_end"`
	ReceivedAmount float64 `json:"received_amount"`
	ExpectedCF     float64 `json:"expected_from_cashflow"`
	ExpectedAcc    float64 `json:"expected_from_accrual"`
	ExpectedFinal  float64 `json:"expected_final"`
	Variance       float64 `json:"variance"`
	VariancePct    float64 `json:"variance_pct"`
	MatchStatus    string  `json:"match_status"`
	ExceptionType  string  `json:"exception_type,omitempty"`
	// Detailed rows used for matching — always populated in preview
	Cashflows     []CashflowLine      `json:"cashflows"`
	AccrualLedger []AccrualLedgerLine `json:"accrual_ledger"`
}

// ReconcilePreviewSummary is the dry-run response returned by /reconcile/run.
type ReconcilePreviewSummary struct {
	EntityID      string                 `json:"entity_id"`
	PeriodStart   string                 `json:"period_start"`
	PeriodEnd     string                 `json:"period_end"`
	MatchingBasis string                 `json:"matching_basis"`
	Interest      ReconcilePassSummary   `json:"interest"`
	TDS           ReconcilePassSummary   `json:"tds"`
	Lines         []ReconcilePreviewLine `json:"lines"`
	// Keyed lookups: each receipt_id / tds_id maps to its full detail
	InterestReceipts map[string]*ReconcilePreviewLine `json:"interest_receipts"`
	TDSReceipts      map[string]*ReconcilePreviewLine `json:"tds_receipts"`
}

// ReconcilePassSummary aggregates counts for one pass (interest or TDS).
type ReconcilePassSummary struct {
	Processed     int     `json:"processed"`
	Matched       int     `json:"matched"`
	Partial       int     `json:"partial"`
	Unmatched     int     `json:"unmatched"`
	Exception     int     `json:"exception"`
	TotalExpected float64 `json:"total_expected"`
	TotalReceived float64 `json:"total_received"`
	TotalVariance float64 `json:"total_variance"`
}

// ─── Main reconciliation engine ───────────────────────────────────────────────

// reconcileEngine is the shared computation core.
// dryRun=true  → computes everything, returns preview lines, writes NOTHING to DB.
// dryRun=false → writes junction table rows, results, exceptions, updates run counters.
// filterReceiptIDs / filterTDSIDs: when non-empty only those specific IDs are processed
// (overrides the entity+period entity-wide query).
func reconcileEngine(ctx context.Context, pool *pgxpool.Pool, runID string, dryRun bool,
	filterReceiptIDs, filterTDSIDs []string) (*ReconcilePreviewSummary, error) {
	// ── Fetch run metadata ────────────────────────────────────────────────────
	var entityID, periodStart, periodEnd, matchingBasis, triggeredBy string
	err := pool.QueryRow(ctx, `
		SELECT entity_id, period_start::text, period_end::text,
		       COALESCE(matching_basis,'BOTH'), COALESCE(triggered_by,'system')
		FROM investment.fd_receipt_reconcile_run
		WHERE reconcile_run_id=$1`, runID).Scan(
		&entityID, &periodStart, &periodEnd, &matchingBasis, &triggeredBy)
	if err != nil {
		return nil, fmt.Errorf("fetch run metadata: %w", err)
	}
	filterReceiptIDs, filterTDSIDs, err = expandLinkedReconcileIDs(ctx, pool, filterReceiptIDs, filterTDSIDs)
	if err != nil {
		if !dryRun {
			markRunFailed(ctx, pool, runID, err.Error())
		}
		return nil, err
	}

	preview := &ReconcilePreviewSummary{
		EntityID:         entityID,
		PeriodStart:      periodStart,
		PeriodEnd:        periodEnd,
		MatchingBasis:    matchingBasis,
		InterestReceipts: make(map[string]*ReconcilePreviewLine),
		TDSReceipts:      make(map[string]*ReconcilePreviewLine),
	}

	// ── Interest pass ─────────────────────────────────────────────────────────
	iProcessed, iMatched, iPartial, iUnmatched, iException := 0, 0, 0, 0, 0
	iTotalExpected, iTotalReceived := 0.0, 0.0

	receipts, err := loadInterestReceipts(ctx, pool, entityID, periodStart, periodEnd, filterReceiptIDs)
	if err != nil {
		if !dryRun {
			markRunFailed(ctx, pool, runID, err.Error())
		}
		return nil, err
	}

	for _, rec := range receipts {
		iProcessed++

		// ── Load full cashflow + accrual detail rows ───────────────────────────
		var cfLines []CashflowLine
		var acLines []AccrualLedgerLine
		var expectedCF, expectedAcc float64

		if matchingBasis == "CASHFLOW" || matchingBasis == "BOTH" {
			cfLines = loadCashflowsForReceipt(ctx, pool, rec.FDID, rec.PeriodStart, rec.PeriodEnd, false)
			for _, cf := range cfLines {
				expectedCF += cf.Amount
			}
		}
		if matchingBasis == "ACCRUAL" || matchingBasis == "BOTH" {
			acLines = loadAccrualLedgerForReceipt(ctx, pool, rec.FDID, rec.PeriodStart, rec.PeriodEnd, false)
			for _, al := range acLines {
				expectedAcc += al.Amount
			}
		}

		expected := resolveExpected(matchingBasis, expectedCF, expectedAcc)
		matchStatus, variance, variancePct := classifyVariance(rec.Gross, expected)

		// Build junction maps after matchStatus is known so the status is accurate.
		if !dryRun {
			if matchingBasis == "CASHFLOW" || matchingBasis == "BOTH" {
				buildCashflowMap(ctx, MapLookupParams{Pool: pool, RunID: runID, FDID: rec.FDID, ReceiptID: rec.ReceiptID, TDSID: "", PeriodStart: rec.PeriodStart, PeriodEnd: rec.PeriodEnd, MatchStatus: matchStatus}) //nolint:errcheck
			}
			if matchingBasis == "ACCRUAL" || matchingBasis == "BOTH" {
				buildAccrualMap(ctx, MapLookupParams{Pool: pool, RunID: runID, FDID: rec.FDID, ReceiptID: rec.ReceiptID, TDSID: "", PeriodStart: rec.PeriodStart, PeriodEnd: rec.PeriodEnd, MatchStatus: matchStatus}) //nolint:errcheck
			}
		}

		line := ReconcilePreviewLine{
			ReceiptID:      rec.ReceiptID,
			ResultType:     "INTEREST",
			FDID:           rec.FDID,
			FdRefNo:        rec.FdRefNo,
			EntityID:       rec.EntityID,
			EntityName:     rec.EntityName,
			BankID:         rec.BankID,
			BankName:       rec.BankName,
			PeriodStart:    rec.PeriodStart.Format(constants.DateFormat),
			PeriodEnd:      rec.PeriodEnd.Format(constants.DateFormat),
			ReceivedAmount: rec.Gross,
			ExpectedCF:     expectedCF,
			ExpectedAcc:    expectedAcc,
			ExpectedFinal:  expected,
			Variance:       variance,
			VariancePct:    variancePct,
			MatchStatus:    matchStatus,
			Cashflows:      cfLines,
			AccrualLedger:  acLines,
		}
		if matchStatus != "MATCHED" {
			line.ExceptionType = deriveExceptionType(variance, expected)
		}
		preview.Lines = append(preview.Lines, line)
		preview.InterestReceipts[rec.ReceiptID] = &preview.Lines[len(preview.Lines)-1]

		if !dryRun {
			// ── Insert result row ─────────────────────────────────────────────
			resultID, _ := insertInterestResult(ctx, InterestResultParams{Pool: pool, Rec: rec, RunID: runID, MatchingBasis: matchingBasis, Expected: expected, Variance: variance, VariancePct: variancePct, MatchStatus: matchStatus})

			// ── Stamp receipt ─────────────────────────────────────────────────
			pool.Exec(ctx, //nolint:errcheck
				`UPDATE investment.fd_interest_receipt
				 SET reconcile_status=$1, reconcile_run_id=$2
				 WHERE receipt_id=$3`,
				matchStatus, runID, rec.ReceiptID)

			// ── Variance engine ───────────────────────────────────────────────
			veRunID := varianceengine.NewRunID()
			veRules := []varianceengine.Rule{{
				FieldName:     "gross_interest_received",
				VarianceType:  varianceengine.TypeAmount,
				ExpectedValue: strconv.FormatFloat(expected, 'f', 6, 64),
				ActualValue:   strconv.FormatFloat(rec.Gross, 'f', 6, 64),
				Tolerance:     expected * MatchTolerancePct / 100,
				Priority:      priorityFromStatus(matchStatus),
				SystemComment: fmt.Sprintf("Reconcile run %s (basis=%s)", runID, matchingBasis),
			}}
			veItems := varianceengine.Compare("FD_INTEREST_RECEIPT", rec.ReceiptID, rec.EntityID, veRunID, veRules)
			varianceengine.PersistVariances(ctx, pool, veItems)                                                                          //nolint:errcheck
			varianceengine.UpdateRecordFlags(ctx, pool, "investment.fd_interest_receipt", "receipt_id", rec.ReceiptID, veRunID, veItems) //nolint:errcheck
			varianceengine.AutoResolveCleared(ctx, pool, rec.ReceiptID, veItems, triggeredBy, triggeredBy)                               //nolint:errcheck

			// ── Exception ─────────────────────────────────────────────────────
			// Always create an exception for non-MATCHED receipts. Do NOT gate
			// on resultID != ""; if insertInterestResult failed the exception
			// must still surface in /exception/all so the variance workflow works.
			if matchStatus != "MATCHED" {
				exID, _ := insertException(ctx, pool, ExceptionParams{
					Rec:           rec,
					RunID:         runID,
					ResultID:      resultID,
					ExceptionType: deriveExceptionType(variance, expected),
					Severity:      severityFromStatus(matchStatus),
					ResultType:    "INTEREST",
					ExpectedAmt:   expected,
					ReceivedAmt:   rec.Gross,
					RaisedBy:      triggeredBy,
				})
				if exID != "" {
					varianceAbs := variance
					if varianceAbs < 0 {
						varianceAbs = -varianceAbs
					}
					insertVarianceRaisedAudit(ctx, pool, exID, triggeredBy, deriveExceptionType(variance, expected), expected, rec.Gross, varianceAbs) //nolint:errcheck
					if resultID != "" {
						pool.Exec(ctx, //nolint:errcheck
							`UPDATE investment.fd_receipt_reconcile_result
							 SET has_exception=true, exception_id=$1 WHERE result_id=$2`,
							exID, resultID)
					}
				}
			}
		}

		switch matchStatus {
		case "MATCHED":
			iMatched++
		case "PARTIAL":
			iPartial++
		case "EXCEPTION":
			iException++
		default:
			iUnmatched++
		}
		iTotalExpected += expected
		iTotalReceived += rec.Gross
	}

	// ── TDS pass ──────────────────────────────────────────────────────────────
	tProcessed, tMatched, tPartial, tUnmatched, tException := 0, 0, 0, 0, 0
	tTotalExpected, tTotalReceived := 0.0, 0.0

	tdsRows, tdsErr := loadTDSReceipts(ctx, pool, entityID, periodStart, periodEnd, filterTDSIDs)
	if tdsErr == nil {
		for _, tds := range tdsRows {
			tProcessed++

			var tdsCFLines []CashflowLine
			var tdsACLines []AccrualLedgerLine
			var expectedCF, expectedAcc float64

			if matchingBasis == "CASHFLOW" || matchingBasis == "BOTH" {
				tdsCFLines = loadCashflowsForReceipt(ctx, pool, tds.FDID, tds.PeriodStart, tds.PeriodEnd, true)
				for _, cf := range tdsCFLines {
					expectedCF += cf.Amount
				}
			}
			if matchingBasis == "ACCRUAL" || matchingBasis == "BOTH" {
				tdsACLines = loadAccrualLedgerForReceipt(ctx, pool, tds.FDID, tds.PeriodStart, tds.PeriodEnd, true)
				for _, al := range tdsACLines {
					expectedAcc += al.Amount
				}
			}

			expected := resolveExpected(matchingBasis, expectedCF, expectedAcc)
			matchStatus, variance, variancePct := classifyVariance(tds.Actual, expected)

			// Build junction maps after matchStatus is known so the status is accurate.
			if !dryRun {
				if matchingBasis == "CASHFLOW" || matchingBasis == "BOTH" {
					buildCashflowMap(ctx, MapLookupParams{Pool: pool, RunID: runID, FDID: tds.FDID, ReceiptID: "", TDSID: tds.TDSID, PeriodStart: tds.PeriodStart, PeriodEnd: tds.PeriodEnd, MatchStatus: matchStatus}) //nolint:errcheck
				}
				if matchingBasis == "ACCRUAL" || matchingBasis == "BOTH" {
					buildAccrualMapTDS(ctx, MapLookupParams{Pool: pool, RunID: runID, FDID: tds.FDID, ReceiptID: "", TDSID: tds.TDSID, PeriodStart: tds.PeriodStart, PeriodEnd: tds.PeriodEnd, MatchStatus: matchStatus}) //nolint:errcheck
				}
			}

			line := ReconcilePreviewLine{
				TDSID:          tds.TDSID,
				ResultType:     "TDS",
				FDID:           tds.FDID,
				FdRefNo:        tds.FdRefNo,
				EntityID:       tds.EntityID,
				EntityName:     tds.EntityName,
				BankID:         tds.BankID,
				BankName:       tds.BankName,
				PeriodStart:    tds.PeriodStart.Format(constants.DateFormat),
				PeriodEnd:      tds.PeriodEnd.Format(constants.DateFormat),
				ReceivedAmount: tds.Actual,
				ExpectedCF:     expectedCF,
				ExpectedAcc:    expectedAcc,
				ExpectedFinal:  expected,
				Variance:       variance,
				VariancePct:    variancePct,
				MatchStatus:    matchStatus,
				Cashflows:      tdsCFLines,
				AccrualLedger:  tdsACLines,
			}
			if matchStatus != "MATCHED" {
				line.ExceptionType = deriveExceptionType(variance, expected)
			}
			preview.Lines = append(preview.Lines, line)
			preview.TDSReceipts[tds.TDSID] = &preview.Lines[len(preview.Lines)-1]

			if !dryRun {
				tdsResultID, _ := insertTDSResult(ctx, TDSResultParams{Pool: pool, TDS: tds, RunID: runID, MatchingBasis: matchingBasis, Expected: expected, Variance: variance, VariancePct: variancePct, MatchStatus: matchStatus})

				pool.Exec(ctx, //nolint:errcheck
					`UPDATE investment.fd_tds_receipt
					 SET reconcile_status=$1, reconcile_run_id=$2
					 WHERE tds_id=$3`,
					matchStatus, runID, tds.TDSID)

				tveRunID := varianceengine.NewRunID()
				tRules := []varianceengine.Rule{{
					FieldName:     "tds_deducted_actual",
					VarianceType:  varianceengine.TypeAmount,
					ExpectedValue: strconv.FormatFloat(expected, 'f', 6, 64),
					ActualValue:   strconv.FormatFloat(tds.Actual, 'f', 6, 64),
					Tolerance:     expected * MatchTolerancePct / 100,
					Priority:      priorityFromStatus(matchStatus),
					SystemComment: fmt.Sprintf("TDS reconcile run %s (basis=%s)", runID, matchingBasis),
				}}
				tItems := varianceengine.Compare("FD_TDS_RECEIPT", tds.TDSID, tds.EntityID, tveRunID, tRules)
				varianceengine.PersistVariances(ctx, pool, tItems)                                                              //nolint:errcheck
				varianceengine.UpdateRecordFlags(ctx, pool, "investment.fd_tds_receipt", "tds_id", tds.TDSID, tveRunID, tItems) //nolint:errcheck
				varianceengine.AutoResolveCleared(ctx, pool, tds.TDSID, tItems, triggeredBy, triggeredBy)                       //nolint:errcheck

				// Always create an exception for non-MATCHED TDS rows. Do NOT gate
				// on tdsResultID != ""; if insertTDSResult failed the exception
				// must still surface in /exception/all so the variance workflow works.
				if matchStatus != "MATCHED" {
					exID, _ := insertException(ctx, pool, ExceptionParams{
						Rec: ReceiptRow{
							ReceiptID: tds.ReceiptID,
							FDID:      tds.FDID,
							FdRefNo:   tds.FdRefNo,
							EntityID:  tds.EntityID,
						},
						RunID:         runID,
						ResultID:      tdsResultID,
						ExceptionType: deriveExceptionType(variance, expected),
						Severity:      severityFromStatus(matchStatus),
						ResultType:    "TDS",
						TDSID:         tds.TDSID,
						ExpectedAmt:   expected,
						ReceivedAmt:   tds.Actual,
						RaisedBy:      triggeredBy,
					})
					if exID != "" {
						varianceAmt := tds.Actual - expected
						if varianceAmt < 0 {
							varianceAmt = -varianceAmt
						}
						insertVarianceRaisedAudit(ctx, pool, exID, triggeredBy, deriveExceptionType(variance, expected), expected, tds.Actual, varianceAmt) //nolint:errcheck
						if tdsResultID != "" {
							pool.Exec(ctx, //nolint:errcheck
								`UPDATE investment.fd_receipt_reconcile_result
								 SET has_exception=true, exception_id=$1 WHERE result_id=$2`,
								exID, tdsResultID)
						}
					}
				}
			}

			switch matchStatus {
			case "MATCHED":
				tMatched++
			case "PARTIAL":
				tPartial++
			case "EXCEPTION":
				tException++
			default:
				tUnmatched++
			}
			tTotalExpected += expected
			tTotalReceived += tds.Actual
		}
	}

	// ── Build summary ─────────────────────────────────────────────────────────
	preview.Interest = ReconcilePassSummary{
		Processed:     iProcessed,
		Matched:       iMatched,
		Partial:       iPartial,
		Unmatched:     iUnmatched,
		Exception:     iException,
		TotalExpected: iTotalExpected,
		TotalReceived: iTotalReceived,
		TotalVariance: iTotalReceived - iTotalExpected,
	}
	preview.TDS = ReconcilePassSummary{
		Processed:     tProcessed,
		Matched:       tMatched,
		Partial:       tPartial,
		Unmatched:     tUnmatched,
		Exception:     tException,
		TotalExpected: tTotalExpected,
		TotalReceived: tTotalReceived,
		TotalVariance: tTotalReceived - tTotalExpected,
	}

	// ── Finalise run row (only on real ingest) ────────────────────────────────
	if !dryRun {
		pool.Exec(ctx, `
			UPDATE investment.fd_receipt_reconcile_run
			SET run_status               = 'COMPLETED',
			    completed_at             = now(),
			    interest_processed       = $1,
			    interest_matched         = $2,
			    interest_partial         = $3,
			    interest_unmatched       = $4,
			    interest_exception       = $5,
			    tds_processed            = $6,
			    tds_matched              = $7,
			    tds_partial              = $8,
			    tds_unmatched            = $9,
			    tds_exception            = $10,
			    total_expected_interest  = $11,
			    total_received_interest  = $12,
			    total_interest_variance  = $13,
			    total_expected_tds       = $14,
			    total_received_tds       = $15,
			    total_tds_variance       = $16
			WHERE reconcile_run_id = $17`, //nolint:errcheck
			iProcessed, iMatched, iPartial, iUnmatched, iException,
			tProcessed, tMatched, tPartial, tUnmatched, tException,
			iTotalExpected, iTotalReceived, iTotalReceived-iTotalExpected,
			tTotalExpected, tTotalReceived, tTotalReceived-tTotalExpected,
			runID)
	}

	return preview, nil
}

// runReconciliation is the background goroutine entry point (full ingest, dryRun=false).
func runReconciliation(ctx context.Context, pool *pgxpool.Pool, runID string, filterReceiptIDs, filterTDSIDs []string) error {
	_, err := reconcileEngine(ctx, pool, runID, false, filterReceiptIDs, filterTDSIDs)
	return err
}

func addUniqueString(values []string, seen map[string]bool, value string) []string {
	if value == "" || seen[value] {
		return values
	}
	seen[value] = true
	return append(values, value)
}

func expandLinkedReconcileIDs(ctx context.Context, pool *pgxpool.Pool, receiptIDs, tdsIDs []string) ([]string, []string, error) {
	receiptSeen := map[string]bool{}
	tdsSeen := map[string]bool{}
	expandedReceiptIDs := make([]string, 0, len(receiptIDs))
	expandedTDSIDs := make([]string, 0, len(tdsIDs))
	for _, id := range receiptIDs {
		expandedReceiptIDs = addUniqueString(expandedReceiptIDs, receiptSeen, id)
	}
	for _, id := range tdsIDs {
		expandedTDSIDs = addUniqueString(expandedTDSIDs, tdsSeen, id)
	}

	if len(receiptIDs) > 0 {
		rows, err := pool.Query(ctx, `
			SELECT tds_id
			FROM investment.fd_tds_receipt
			WHERE receipt_id = ANY($1::text[])
			  AND is_deleted = false`, receiptIDs)
		if err != nil {
			return nil, nil, fmt.Errorf("load linked TDS receipts: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var tdsID string
			if err := rows.Scan(&tdsID); err == nil {
				expandedTDSIDs = addUniqueString(expandedTDSIDs, tdsSeen, tdsID)
			}
		}
		if err := rows.Err(); err != nil {
			return nil, nil, fmt.Errorf("scan linked TDS receipts: %w", err)
		}
	}

	if len(tdsIDs) > 0 {
		// Step 1: direct receipt_id link.
		rows, err := pool.Query(ctx, `
			SELECT COALESCE(receipt_id, '')
			FROM investment.fd_tds_receipt
			WHERE tds_id = ANY($1::text[])
			  AND is_deleted = false`, tdsIDs)
		if err != nil {
			return nil, nil, fmt.Errorf("load linked interest receipts: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var receiptID string
			if err := rows.Scan(&receiptID); err == nil {
				expandedReceiptIDs = addUniqueString(expandedReceiptIDs, receiptSeen, receiptID)
			}
		}
		if err := rows.Err(); err != nil {
			return nil, nil, fmt.Errorf("scan linked interest receipts: %w", err)
		}

		// Step 2: fallback — for TDS entries that had no receipt_id link, restrict the
		// interest pass to receipts from the same FD(s) and overlapping periods.
		// Without this guard, loadInterestReceipts falls back to entity+period-wide and pulls in unrelated FDs' receipts.
		irRows, irErr := pool.Query(ctx, `
			SELECT r.receipt_id
			FROM investment.fd_interest_receipt r
			JOIN investment.fd_tds_receipt t ON t.fd_id = r.fd_id
			WHERE t.tds_id = ANY($1::text[])
			  AND COALESCE(t.receipt_id, '') = ''
			  AND r.is_deleted = false
			  AND t.is_deleted = false
			  AND r.receipt_status IN ('APPROVED','POSTED','PARTIAL')
			  AND r.period_start <= t.period_end
			  AND r.period_end >= t.period_start`, tdsIDs)
		if irErr == nil {
			for irRows.Next() {
				var rid string
				if irRows.Scan(&rid) == nil {
					expandedReceiptIDs = addUniqueString(expandedReceiptIDs, receiptSeen, rid)
				}
			}
			irRows.Close()
		}
	}

	return expandedReceiptIDs, expandedTDSIDs, nil
}

// reconcilePreviewDirect runs a pure in-memory preview without any run row.
// It derives entity+period from the receipt/TDS records themselves.
// No DB writes happen — safe to call repeatedly.
func reconcilePreviewDirect(ctx context.Context, pool *pgxpool.Pool,
	matchingBasis string,
	filterReceiptIDs, filterTDSIDs []string) (*ReconcilePreviewSummary, error) {

	var expandErr error
	filterReceiptIDs, filterTDSIDs, expandErr = expandLinkedReconcileIDs(ctx, pool, filterReceiptIDs, filterTDSIDs)
	if expandErr != nil {
		return nil, expandErr
	}

	// Derive entity+period from the supplied IDs by querying the receipt tables.
	// We use the widest possible period that covers all supplied records.
	entityID := ""
	periodStart := constants.DateMax
	periodEnd := constants.DateMin

	if len(filterReceiptIDs) > 0 {
		rows, err := pool.Query(ctx, `
			SELECT entity_id, period_start::text, period_end::text
			FROM investment.fd_interest_receipt
			WHERE receipt_id = ANY($1::text[]) AND is_deleted = false`, filterReceiptIDs)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var eid, ps, pe string
				if rows.Scan(&eid, &ps, &pe) == nil {
					if entityID == "" {
						entityID = eid
					}
					if ps < periodStart {
						periodStart = ps
					}
					if pe > periodEnd {
						periodEnd = pe
					}
				}
			}
		}
	}
	if len(filterTDSIDs) > 0 {
		rows2, err := pool.Query(ctx, `
			SELECT t.entity_id,
			       COALESCE(r.period_start, t.period_start)::text,
			       COALESCE(r.period_end, t.period_end)::text
			FROM investment.fd_tds_receipt t
			LEFT JOIN investment.fd_interest_receipt r ON r.receipt_id = t.receipt_id AND r.is_deleted = false
			WHERE t.tds_id = ANY($1::text[]) AND t.is_deleted = false`, filterTDSIDs)
		if err == nil {
			defer rows2.Close()
			for rows2.Next() {
				var eid, ps, pe string
				if rows2.Scan(&eid, &ps, &pe) == nil {
					if entityID == "" {
						entityID = eid
					}
					if ps < periodStart {
						periodStart = ps
					}
					if pe > periodEnd {
						periodEnd = pe
					}
				}
			}
		}
	}
	// Fallback sentinels if nothing was found
	if periodStart == constants.DateMax {
		periodStart = constants.DateMin
	}
	if periodEnd == constants.DateMin {
		periodEnd = constants.DateMax
	}
	if matchingBasis == "" {
		matchingBasis = "BOTH"
	}

	preview := &ReconcilePreviewSummary{
		EntityID:         entityID,
		PeriodStart:      periodStart,
		PeriodEnd:        periodEnd,
		MatchingBasis:    matchingBasis,
		InterestReceipts: make(map[string]*ReconcilePreviewLine),
		TDSReceipts:      make(map[string]*ReconcilePreviewLine),
	}

	iProcessed, iMatched, iPartial, iUnmatched, iException := 0, 0, 0, 0, 0
	iTotalExpected, iTotalReceived := 0.0, 0.0

	receipts, err := loadInterestReceipts(ctx, pool, entityID, periodStart, periodEnd, filterReceiptIDs)
	if err != nil {
		return nil, err
	}
	for _, rec := range receipts {
		iProcessed++
		var cfLines []CashflowLine
		var acLines []AccrualLedgerLine
		var expectedCF, expectedAcc float64
		if matchingBasis == "CASHFLOW" || matchingBasis == "BOTH" {
			cfLines = loadCashflowsForReceipt(ctx, pool, rec.FDID, rec.PeriodStart, rec.PeriodEnd, false)
			for _, cf := range cfLines {
				expectedCF += cf.Amount
			}
		}
		if matchingBasis == "ACCRUAL" || matchingBasis == "BOTH" {
			acLines = loadAccrualLedgerForReceipt(ctx, pool, rec.FDID, rec.PeriodStart, rec.PeriodEnd, false)
			for _, al := range acLines {
				expectedAcc += al.Amount
			}
		}
		expected := resolveExpected(matchingBasis, expectedCF, expectedAcc)
		matchStatus, variance, variancePct := classifyVariance(rec.Gross, expected)

		line := ReconcilePreviewLine{
			ReceiptID: rec.ReceiptID, ResultType: "INTEREST",
			FDID: rec.FDID, FdRefNo: rec.FdRefNo,
			EntityID: rec.EntityID, EntityName: rec.EntityName,
			BankID: rec.BankID, BankName: rec.BankName,
			PeriodStart:    rec.PeriodStart.Format(constants.DateFormat),
			PeriodEnd:      rec.PeriodEnd.Format(constants.DateFormat),
			ReceivedAmount: rec.Gross,
			ExpectedCF:     expectedCF, ExpectedAcc: expectedAcc, ExpectedFinal: expected,
			Variance: variance, VariancePct: variancePct, MatchStatus: matchStatus,
			Cashflows: cfLines, AccrualLedger: acLines,
		}
		if matchStatus != "MATCHED" {
			line.ExceptionType = deriveExceptionType(variance, expected)
		}
		preview.Lines = append(preview.Lines, line)
		preview.InterestReceipts[rec.ReceiptID] = &preview.Lines[len(preview.Lines)-1]
		switch matchStatus {
		case "MATCHED":
			iMatched++
		case "PARTIAL":
			iPartial++
		case "EXCEPTION":
			iException++
		default:
			iUnmatched++
		}
		iTotalExpected += expected
		iTotalReceived += rec.Gross
	}

	tProcessed, tMatched, tPartial, tUnmatched, tException := 0, 0, 0, 0, 0
	tTotalExpected, tTotalReceived := 0.0, 0.0
	tdsRows, _ := loadTDSReceipts(ctx, pool, entityID, periodStart, periodEnd, filterTDSIDs)
	for _, tds := range tdsRows {
		tProcessed++
		var tdsCFLines []CashflowLine
		var tdsACLines []AccrualLedgerLine
		var expectedCF, expectedAcc float64
		if matchingBasis == "CASHFLOW" || matchingBasis == "BOTH" {
			tdsCFLines = loadCashflowsForReceipt(ctx, pool, tds.FDID, tds.PeriodStart, tds.PeriodEnd, true)
			for _, cf := range tdsCFLines {
				expectedCF += cf.Amount
			}
		}
		if matchingBasis == "ACCRUAL" || matchingBasis == "BOTH" {
			tdsACLines = loadAccrualLedgerForReceipt(ctx, pool, tds.FDID, tds.PeriodStart, tds.PeriodEnd, true)
			for _, al := range tdsACLines {
				expectedAcc += al.Amount
			}
		}
		expected := resolveExpected(matchingBasis, expectedCF, expectedAcc)
		matchStatus, variance, variancePct := classifyVariance(tds.Actual, expected)

		line := ReconcilePreviewLine{
			TDSID: tds.TDSID, ResultType: "TDS",
			FDID: tds.FDID, FdRefNo: tds.FdRefNo,
			EntityID: tds.EntityID, EntityName: tds.EntityName,
			BankID: tds.BankID, BankName: tds.BankName,
			PeriodStart:    tds.PeriodStart.Format(constants.DateFormat),
			PeriodEnd:      tds.PeriodEnd.Format(constants.DateFormat),
			ReceivedAmount: tds.Actual,
			ExpectedCF:     expectedCF, ExpectedAcc: expectedAcc, ExpectedFinal: expected,
			Variance: variance, VariancePct: variancePct, MatchStatus: matchStatus,
			Cashflows: tdsCFLines, AccrualLedger: tdsACLines,
		}
		if matchStatus != "MATCHED" {
			line.ExceptionType = deriveExceptionType(variance, expected)
		}
		preview.Lines = append(preview.Lines, line)
		preview.TDSReceipts[tds.TDSID] = &preview.Lines[len(preview.Lines)-1]
		switch matchStatus {
		case "MATCHED":
			tMatched++
		case "PARTIAL":
			tPartial++
		case "EXCEPTION":
			tException++
		default:
			tUnmatched++
		}
		tTotalExpected += expected
		tTotalReceived += tds.Actual
	}

	preview.Interest = ReconcilePassSummary{
		Processed: iProcessed, Matched: iMatched, Partial: iPartial,
		Unmatched: iUnmatched, Exception: iException,
		TotalExpected: iTotalExpected, TotalReceived: iTotalReceived,
		TotalVariance: iTotalReceived - iTotalExpected,
	}
	preview.TDS = ReconcilePassSummary{
		Processed: tProcessed, Matched: tMatched, Partial: tPartial,
		Unmatched: tUnmatched, Exception: tException,
		TotalExpected: tTotalExpected, TotalReceived: tTotalReceived,
		TotalVariance: tTotalReceived - tTotalExpected,
	}
	return preview, nil
}

// ─── Data loaders ─────────────────────────────────────────────────────────────

func loadInterestReceipts(ctx context.Context, pool *pgxpool.Pool, entityID, periodStart, periodEnd string, filterIDs []string) ([]ReceiptRow, error) {
	var rows interface {
		Next() bool
		Scan(...any) error
		Close()
		Err() error
	}
	var err error
	if len(filterIDs) > 0 {
		// Specific receipt IDs requested — ignore entity+period filter
		rows, err = pool.Query(ctx, `
			SELECT r.receipt_id, r.fd_id, r.fd_ref_no, r.entity_id,
			       COALESCE(m.entity_name, r.entity_id, '')   AS entity_name,
			       COALESCE(r.bank_id,'')                      AS bank_id,
			       COALESCE(m.bank_name, '')                   AS bank_name,
			       r.period_start, r.period_end, r.receipt_date,
			       r.gross_interest_received, r.tds_amount_deducted, r.net_amount_received
			FROM investment.fd_interest_receipt r
			LEFT JOIN investment.fd_master m ON m.fd_id = r.fd_id AND m.is_deleted = false
			WHERE r.receipt_id = ANY($1::text[])
			  AND r.receipt_status IN ('APPROVED','POSTED','PARTIAL')
			  AND r.is_deleted = false`, filterIDs)
	} else {
		rows, err = pool.Query(ctx, `
			SELECT r.receipt_id, r.fd_id, r.fd_ref_no, r.entity_id,
			       COALESCE(m.entity_name, r.entity_id, '')   AS entity_name,
			       COALESCE(r.bank_id,'')                      AS bank_id,
			       COALESCE(m.bank_name, '')                   AS bank_name,
			       r.period_start, r.period_end, r.receipt_date,
			       r.gross_interest_received, r.tds_amount_deducted, r.net_amount_received
			FROM investment.fd_interest_receipt r
			LEFT JOIN investment.fd_master m ON m.fd_id = r.fd_id AND m.is_deleted = false
			WHERE r.entity_id = $1
			  AND r.receipt_date BETWEEN $2::date AND $3::date
			  AND r.receipt_status IN ('APPROVED','POSTED','PARTIAL')
			  AND r.is_deleted = false`, entityID, periodStart, periodEnd)
	}
	if err != nil {
		return nil, fmt.Errorf("load interest receipts: %w", err)
	}
	defer rows.Close()
	var out []ReceiptRow
	for rows.Next() {
		var r ReceiptRow
		if e := rows.Scan(&r.ReceiptID, &r.FDID, &r.FdRefNo, &r.EntityID, &r.EntityName, &r.BankID, &r.BankName,
			&r.PeriodStart, &r.PeriodEnd, &r.ReceiptDate,
			&r.Gross, &r.TDS, &r.Net); e == nil {
			out = append(out, r)
		}
	}
	return out, rows.Err()
}

func loadTDSReceipts(ctx context.Context, pool *pgxpool.Pool, entityID, periodStart, periodEnd string, filterIDs []string) ([]TDSRow, error) {
	var rows interface {
		Next() bool
		Scan(...any) error
		Close()
		Err() error
	}
	var err error
	if len(filterIDs) > 0 {
		rows, err = pool.Query(ctx, `
			SELECT t.tds_id, COALESCE(t.receipt_id,''), t.fd_id, t.fd_ref_no, t.entity_id,
			       COALESCE(m.entity_name, t.entity_id, '') AS entity_name,
			       COALESCE(t.bank_id,'')                    AS bank_id,
			       COALESCE(m.bank_name, '')                 AS bank_name,
			       COALESCE(r.period_start, t.period_start),
			       COALESCE(r.period_end, t.period_end),
			       t.deduction_date,
			       t.tds_deducted_actual
			FROM investment.fd_tds_receipt t
			LEFT JOIN investment.fd_master m ON m.fd_id = t.fd_id AND m.is_deleted = false
			LEFT JOIN investment.fd_interest_receipt r ON r.receipt_id = t.receipt_id AND r.is_deleted = false
			WHERE t.tds_id = ANY($1::text[])
			  AND t.tds_status IN ('APPROVED','POSTED','PARTIAL')
			  AND t.is_deleted = false`, filterIDs)
	} else {
		rows, err = pool.Query(ctx, `
			SELECT t.tds_id, COALESCE(t.receipt_id,''), t.fd_id, t.fd_ref_no, t.entity_id,
			       COALESCE(m.entity_name, t.entity_id, '') AS entity_name,
			       COALESCE(t.bank_id,'')                    AS bank_id,
			       COALESCE(m.bank_name, '')                 AS bank_name,
			       COALESCE(r.period_start, t.period_start),
			       COALESCE(r.period_end, t.period_end),
			       t.deduction_date,
			       t.tds_deducted_actual
			FROM investment.fd_tds_receipt t
			LEFT JOIN investment.fd_master m ON m.fd_id = t.fd_id AND m.is_deleted = false
			LEFT JOIN investment.fd_interest_receipt r ON r.receipt_id = t.receipt_id AND r.is_deleted = false
			WHERE t.entity_id = $1
			  AND COALESCE(r.period_end, t.period_end) > $2::date
			  AND COALESCE(r.period_end, t.period_end) <= $3::date
			  AND t.tds_status IN ('APPROVED','POSTED','PARTIAL')
			  AND t.is_deleted = false`, entityID, periodStart, periodEnd)
	}
	if err != nil {
		return nil, fmt.Errorf("load TDS receipts: %w", err)
	}
	defer rows.Close()
	var out []TDSRow
	for rows.Next() {
		var t TDSRow
		if e := rows.Scan(&t.TDSID, &t.ReceiptID, &t.FDID, &t.FdRefNo, &t.EntityID, &t.EntityName, &t.BankID, &t.BankName,
			&t.PeriodStart, &t.PeriodEnd, &t.DeductionDate, &t.Actual); e == nil {
			out = append(out, t)
		}
	}
	return out, rows.Err()
}

// ─── Detail row loaders (populate preview arrays + compute sums) ──────────────

// loadCashflowsForReceipt returns cashflow rows whose accrual period belongs to
// the receipt/TDS window. The lower bound is exclusive and the upper bound is
// inclusive, so a 2025-10-24→2025-11-24 receipt matches only that period, not
// the prior 2025-09-24→2025-10-24 cashflow.
// forTDS=true  → Amount = tds_amount;  forTDS=false → Amount = interest_accrued.
func loadCashflowsForReceipt(ctx context.Context, pool *pgxpool.Pool,
	fdID string, periodStart, periodEnd time.Time, forTDS bool) []CashflowLine {

	// For compound FDs the bank pays interest at each CAPITALIZATION event,
	// NOT at MATURITY (the MATURITY row's interest_accrued is the net-after-TDS
	// figure, not the gross). So:
	//   INTEREST: include CAPITALIZATION; exclude MATURITY when CAPITALIZATION exists.
	//   TDS:      include CAPITALIZATION + MATURITY (bank deducts TDS at both).
	amountExpr := `CASE event_type
		         WHEN 'INTEREST_RECEIPT' THEN COALESCE(interest_accrued, 0)
		         WHEN 'CAPITALIZATION'   THEN COALESCE(interest_accrued, 0)
		         WHEN 'MATURITY' THEN
		             CASE WHEN EXISTS (
		                 SELECT 1 FROM investment.fd_cashflow_schedule cs2
		                 WHERE cs2.fd_id = fd_cashflow_schedule.fd_id
		                   AND cs2.event_type = 'CAPITALIZATION'
		                   AND COALESCE(cs2.period_end_date, cs2.event_date) > $2
		                   AND COALESCE(cs2.period_end_date, cs2.event_date) <= $3
		                   AND COALESCE(cs2.is_deleted, false) = false
		             ) THEN 0
		             ELSE COALESCE(interest_accrued, 0)
		             END
		         ELSE 0
		       END`
	if forTDS {
		amountExpr = `CASE event_type
			         WHEN 'INTEREST_RECEIPT' THEN COALESCE(tds_amount, 0)
			         WHEN 'CAPITALIZATION'   THEN COALESCE(tds_amount, 0)
			         WHEN 'MATURITY'         THEN COALESCE(tds_amount, 0)
			         ELSE 0
			       END`
	}

	selectCols := fmt.Sprintf(`
		SELECT cashflow_id,
		       event_type,
		       event_date,
		       COALESCE(period_start_date, event_date),
		       COALESCE(period_end_date,   event_date),
		       COALESCE(period_days, 0),
		       COALESCE(opening_principal, 0),
		       COALESCE(interest_accrued, 0),
		       COALESCE(tds_amount, 0),
		       COALESCE(net_cash_flow, 0),
		       COALESCE(posting_status, 'NOT_POSTED'),
		       %s AS amount
		FROM investment.fd_cashflow_schedule`, amountExpr)

	scanRows := func(rows interface {
		Next() bool
		Scan(...any) error
		Close()
		Err() error
	}) []CashflowLine {
		defer rows.Close()
		var out []CashflowLine
		for rows.Next() {
			var c CashflowLine
			var evDate, psDate, peDate time.Time
			if e := rows.Scan(
				&c.CashflowID, &c.EventType,
				&evDate, &psDate, &peDate,
				&c.PeriodDays, &c.OpeningPrincipal,
				&c.InterestAccrued, &c.TDSAmount, &c.NetCashFlow,
				&c.PostingStatus, &c.Amount,
			); e == nil {
				c.EventDate = evDate.Format(constants.DateFormat)
				c.PeriodStartDate = psDate.Format(constants.DateFormat)
				c.PeriodEndDate = peDate.Format(constants.DateFormat)
				out = append(out, c)
			}
		}
		return out
	}

	rows, err := pool.Query(ctx, selectCols+`
		WHERE fd_id = $1
		  AND COALESCE(period_end_date, event_date) > $2
		  AND COALESCE(period_end_date, event_date) <= $3
		  AND COALESCE(is_deleted, false) = false
		ORDER BY COALESCE(period_end_date, event_date), event_date`, fdID, periodStart, periodEnd)
	if err == nil {
		return scanRows(rows)
	}
	return nil
}

// loadAccrualLedgerForReceipt returns all matching accrual ledger rows for a receipt period.
// forTDS=true → Amount = tds_deducted_in_period; false → Amount = period_interest_accrued.
func loadAccrualLedgerForReceipt(ctx context.Context, pool *pgxpool.Pool,
	fdID string, periodStart, periodEnd time.Time, forTDS bool) []AccrualLedgerLine {

	amountCol := "period_interest_accrued"
	if forTDS {
		amountCol = "tds_deducted_in_period"
	}
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT ledger_id,
		       accrual_period_start,
		       accrual_period_end,
		       COALESCE(accrual_days, 0),
		       COALESCE(principal_amount, 0),
		       COALESCE(interest_rate, 0),
		       COALESCE(period_interest_accrued, 0),
		       COALESCE(tds_deducted_in_period, 0),
		       COALESCE(opening_accrued_balance, 0),
		       COALESCE(closing_accrued_balance, 0),
		       ledger_row_status,
		       CASE 
		         WHEN COALESCE(accrual_days, 0) > 0 THEN
		           ROUND(
		             (COALESCE(%[1]s, 0) * GREATEST(0, LEAST($3::date, accrual_period_end) - GREATEST($2::date, accrual_period_start))::numeric) / accrual_days
		           )
		         ELSE COALESCE(%[1]s, 0)
		       END AS amount
		FROM investment.fd_accrual_ledger
		WHERE fd_id                = $1
		  AND accrual_period_start  < $3
		  AND accrual_period_end    > $2
		  AND ledger_row_status IN ('CALCULATED','POSTED','OVERRIDDEN')
		  AND is_deleted        = false
		ORDER BY accrual_period_start`, amountCol),
		fdID, periodStart, periodEnd)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []AccrualLedgerLine
	for rows.Next() {
		var al AccrualLedgerLine
		var psDate, peDate time.Time
		if e := rows.Scan(
			&al.LedgerID,
			&psDate, &peDate,
			&al.AccrualDays, &al.PrincipalAmount, &al.InterestRate,
			&al.PeriodInterestAccrued, &al.TDSDeductedInPeriod,
			&al.OpeningAccruedBalance, &al.ClosingAccruedBalance,
			&al.LedgerRowStatus, &al.Amount,
		); e == nil {
			al.AccrualPeriodStart = psDate.Format(constants.DateFormat)
			al.AccrualPeriodEnd = peDate.Format(constants.DateFormat)
			out = append(out, al)
		}
	}
	return out
}

// ─── Junction table builders ──────────────────────────────────────────────────

// MapLookupParams holds common inputs for buildCashflowMap, buildAccrualMap,
// and buildAccrualMapTDS.
type MapLookupParams struct {
	Pool        *pgxpool.Pool
	RunID       string
	FDID        string
	ReceiptID   string
	TDSID       string
	PeriodStart time.Time
	PeriodEnd   time.Time
	MatchStatus string // actual outcome — written into match_status on junction rows
}

// buildCashflowMap loads ALL interest/accrual cashflow rows for the receipt's
// fd_id within its period, inserts one fd_receipt_cashflow_map row per cashflow,
// and returns the SUM of amounts. Pass receiptID or tdsID; leave the other empty.
func buildCashflowMap(ctx context.Context, p MapLookupParams) float64 {
	pool := p.Pool
	runID := p.RunID
	fdID := p.FDID
	receiptID := p.ReceiptID
	tdsID := p.TDSID
	periodStart := p.PeriodStart
	periodEnd := p.PeriodEnd

	// Include CAPITALIZATION so compound FDs are matched correctly.
	// INTEREST_RECEIPT covers simple/quarterly-payout FDs; MATURITY covers
	// remaining interest at the end; CAPITALIZATION covers compound periods.
	eventTypes := []string{"INTEREST_RECEIPT", "MATURITY", "CAPITALIZATION"}

	// Interest: use CAPITALIZATION gross interest; suppress MATURITY when
	// CAPITALIZATION rows exist (MATURITY's interest_accrued is net-after-TDS).
	// TDS: always include CAPITALIZATION + MATURITY (bank deducts TDS at both).
	amountCol := `CASE event_type
		         WHEN 'INTEREST_RECEIPT' THEN COALESCE(interest_accrued, 0)
		         WHEN 'CAPITALIZATION'   THEN COALESCE(interest_accrued, 0)
		         WHEN 'MATURITY' THEN
		             CASE WHEN EXISTS (
		                 SELECT 1 FROM investment.fd_cashflow_schedule cs2
		                 WHERE cs2.fd_id = fd_cashflow_schedule.fd_id
		                   AND cs2.event_type = 'CAPITALIZATION'
		                   AND COALESCE(cs2.period_end_date, cs2.event_date) > $3
		                   AND COALESCE(cs2.period_end_date, cs2.event_date) <= $4
		                   AND COALESCE(cs2.is_deleted, false) = false
		             ) THEN 0
		             ELSE COALESCE(interest_accrued, 0)
		             END
		         ELSE 0
		       END`
	if tdsID != "" {
		amountCol = `CASE event_type
		         WHEN 'INTEREST_RECEIPT' THEN COALESCE(tds_amount, 0)
		         WHEN 'CAPITALIZATION'   THEN COALESCE(tds_amount, 0)
		         WHEN 'MATURITY'         THEN COALESCE(tds_amount, 0)
		         ELSE 0
		       END`
	}

	cfRows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT cashflow_id,
		       event_type,
		       COALESCE(period_start_date, event_date) AS cf_period_start,
		       COALESCE(period_end_date,   event_date) AS cf_period_end,
		       %s AS amount
		FROM investment.fd_cashflow_schedule
		WHERE fd_id      = $1
		  AND event_type = ANY($2::text[])
		  AND COALESCE(period_end_date, event_date) > $3
		  AND COALESCE(period_end_date, event_date) <= $4
		  AND COALESCE(is_deleted, false) = false`, amountCol),
		fdID, eventTypes, periodStart, periodEnd)
	if err != nil {
		return 0
	}
	defer cfRows.Close()

	var total float64
	for cfRows.Next() {
		var cfID, evType string
		var cfPStart, cfPEnd time.Time
		var amount float64
		if e := cfRows.Scan(&cfID, &evType, &cfPStart, &cfPEnd, &amount); e != nil {
			continue
		}
		total += amount

		pool.Exec(ctx, `
			INSERT INTO investment.fd_receipt_cashflow_map (
				reconcile_run_id, fd_id,
				receipt_id, tds_id,
				cashflow_id,
				cashflow_event_type,
				cashflow_period_start, cashflow_period_end,
				cashflow_amount, matched_amount,
				match_status, is_deleted
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9,$10,false)
			ON CONFLICT DO NOTHING`, //nolint:errcheck
			runID, fdID,
			nullStr(receiptID), nullStr(tdsID),
			cfID, evType,
			cfPStart, cfPEnd,
			amount, p.MatchStatus)
	}
	return total
}

// buildAccrualMap loads ALL accrual ledger rows for the receipt's fd_id within its
// period, inserts one fd_receipt_accrual_map row per ledger row, and returns the SUM
// of period_interest_accrued. For interest receipts; use buildAccrualMapTDS for TDS.
func buildAccrualMap(ctx context.Context, p MapLookupParams) float64 {
	pool := p.Pool
	runID := p.RunID
	fdID := p.FDID
	receiptID := p.ReceiptID
	tdsID := p.TDSID
	periodStart := p.PeriodStart
	periodEnd := p.PeriodEnd

	acRows, err := pool.Query(ctx, `
		SELECT ledger_id,
		       accrual_period_start AS ac_period_start,
		       accrual_period_end   AS ac_period_end,
		       COALESCE(period_interest_accrued, 0) AS amount
		FROM investment.fd_accrual_ledger
		WHERE fd_id                = $1
		  AND accrual_period_start  < $3
		  AND accrual_period_end    > $2
		  AND ledger_row_status IN ('CALCULATED','POSTED','OVERRIDDEN')
		  AND is_deleted        = false`,
		fdID, periodStart, periodEnd)
	if err != nil {
		return 0
	}
	defer acRows.Close()

	var total float64
	for acRows.Next() {
		var ledgerID string
		var acPStart, acPEnd time.Time
		var amount float64
		if e := acRows.Scan(&ledgerID, &acPStart, &acPEnd, &amount); e != nil {
			continue
		}
		total += amount

		pool.Exec(ctx, `
			INSERT INTO investment.fd_receipt_accrual_map (
				reconcile_run_id, fd_id,
				receipt_id, tds_id,
				accrual_ledger_id,
				accrual_period_start, accrual_period_end,
				accrual_amount, matched_amount,
				match_status, is_deleted
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$8,$9,false)
			ON CONFLICT DO NOTHING`, //nolint:errcheck
			runID, fdID,
			nullStr(receiptID), nullStr(tdsID),
			ledgerID,
			acPStart, acPEnd,
			amount, p.MatchStatus)
	}
	return total
}

// buildAccrualMapTDS is like buildAccrualMap but uses tds_deducted_in_period
// from the accrual ledger — used when reconciling TDS receipts.
func buildAccrualMapTDS(ctx context.Context, p MapLookupParams) float64 {
	pool := p.Pool
	runID := p.RunID
	fdID := p.FDID
	receiptID := p.ReceiptID
	tdsID := p.TDSID
	periodStart := p.PeriodStart
	periodEnd := p.PeriodEnd

	acRows, err := pool.Query(ctx, `
		SELECT ledger_id,
		       accrual_period_start AS ac_period_start,
		       accrual_period_end   AS ac_period_end,
		       COALESCE(tds_deducted_in_period, 0) AS amount
		FROM investment.fd_accrual_ledger
		WHERE fd_id                = $1
		  AND accrual_period_start  < $3
		  AND accrual_period_end    > $2
		  AND ledger_row_status IN ('CALCULATED','POSTED','OVERRIDDEN')
		  AND is_deleted        = false`,
		fdID, periodStart, periodEnd)
	if err != nil {
		return 0
	}
	defer acRows.Close()

	var total float64
	for acRows.Next() {
		var ledgerID string
		var acPStart, acPEnd time.Time
		var amount float64
		if e := acRows.Scan(&ledgerID, &acPStart, &acPEnd, &amount); e != nil {
			continue
		}
		total += amount

		pool.Exec(ctx, `
			INSERT INTO investment.fd_receipt_accrual_map (
				reconcile_run_id, fd_id,
				receipt_id, tds_id,
				accrual_ledger_id,
				accrual_period_start, accrual_period_end,
				accrual_amount, matched_amount,
				match_status, is_deleted
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$8,$9,false)
			ON CONFLICT DO NOTHING`, //nolint:errcheck
			runID, fdID,
			nullStr(receiptID), nullStr(tdsID),
			ledgerID,
			acPStart, acPEnd,
			amount, p.MatchStatus)
	}
	return total
}

// ─── Classification helpers ───────────────────────────────────────────────────

// resolveExpected picks the expected amount based on the run's matching_basis.
// BOTH: prefer cashflow; fall back to accrual when cashflow is zero.
func resolveExpected(basis string, cf, acc float64) float64 {
	switch basis {
	case "CASHFLOW":
		return cf
	case "ACCRUAL":
		return acc
	default: // BOTH
		if cf > 0 {
			return cf
		}
		return acc
	}
}

// classifyVariance returns (matchStatus, variance, variancePct).
// ≤1% → MATCHED, ≤10% → PARTIAL, >10% → UNMATCHED, expected=0 → EXCEPTION.
func classifyVariance(actual, expected float64) (string, float64, float64) {
	if expected == 0 {
		return "EXCEPTION", actual, 100.0
	}
	variance := actual - expected
	pct := math.Abs(variance/expected) * 100
	switch {
	case pct <= MatchTolerancePct:
		return "MATCHED", variance, pct
	case pct <= MatchPartialPct:
		return "PARTIAL", variance, pct
	default:
		return "UNMATCHED", variance, pct
	}
}

func deriveExceptionType(variance, expected float64) string {
	if expected == 0 {
		return "UNMATCHED"
	}
	if variance < 0 {
		return "UNDER_RECEIVED"
	}
	return "OVER_RECEIVED"
}

func severityFromStatus(s string) string {
	switch s {
	case "EXCEPTION", "UNMATCHED":
		return "BLOCKER"
	case "PARTIAL":
		return "WARNING"
	default:
		return "INFORMATIONAL"
	}
}

func priorityFromStatus(s string) string {
	switch s {
	case "EXCEPTION", "UNMATCHED":
		return varianceengine.PriorityHigh
	case "PARTIAL":
		return varianceengine.PriorityMedium
	default:
		return varianceengine.PriorityLow
	}
}

// ─── Result row inserts ───────────────────────────────────────────────────────

// InterestResultParams holds all inputs for insertInterestResult.
type InterestResultParams struct {
	Pool          *pgxpool.Pool
	Rec           ReceiptRow
	RunID         string
	MatchingBasis string
	Expected      float64
	Variance      float64
	VariancePct   float64
	MatchStatus   string
}

func insertInterestResult(ctx context.Context, p InterestResultParams) (string, error) {
	pool := p.Pool
	rec := p.Rec
	runID := p.RunID
	matchingBasis := p.MatchingBasis
	expected := p.Expected
	variance := p.Variance
	variancePct := p.VariancePct
	matchStatus := p.MatchStatus

	var bankID, fdRefNo string
	_ = pool.QueryRow(ctx,
		`SELECT COALESCE(bank_id,''), COALESCE(bank_fd_ref_no,'')
		 FROM investment.fd_master WHERE fd_id=$1 AND is_deleted=false`,
		rec.FDID).Scan(&bankID, &fdRefNo)
	if fdRefNo == "" {
		fdRefNo = rec.FdRefNo
	}
	if bankID == "" {
		bankID = rec.BankID
	}

	var varPctArg interface{}
	if variancePct >= 0 {
		varPctArg = variancePct
	}

	var resultID string
	err := pool.QueryRow(ctx, `
		INSERT INTO investment.fd_receipt_reconcile_result (
			reconcile_run_id, fd_id, fd_ref_no, entity_id, bank_id,
			result_type, receipt_id, tds_id,
			period_start, period_end, matching_basis,
			expected_amount, received_amount,
			amount_variance, amount_variance_pct,
			match_status, match_type, has_exception
		) VALUES (
			$1,$2,$3,$4,$5,
			'INTEREST',$6,NULL,
			$7,$8,$9,
			$10,$11,
			$12,$13,
			$14,'AUTO',false
		) RETURNING result_id`,
		runID, rec.FDID, fdRefNo, rec.EntityID, bankID,
		nullStr(rec.ReceiptID),
		rec.PeriodStart, rec.PeriodEnd, matchingBasis,
		expected, rec.Gross,
		variance, varPctArg,
		matchStatus,
	).Scan(&resultID)
	return resultID, err
}

// TDSResultParams holds all inputs for insertTDSResult.
type TDSResultParams struct {
	Pool          *pgxpool.Pool
	TDS           TDSRow
	RunID         string
	MatchingBasis string
	Expected      float64
	Variance      float64
	VariancePct   float64
	MatchStatus   string
}

func insertTDSResult(ctx context.Context, p TDSResultParams) (string, error) {
	pool := p.Pool
	tds := p.TDS
	runID := p.RunID
	matchingBasis := p.MatchingBasis
	expected := p.Expected
	variance := p.Variance
	variancePct := p.VariancePct
	matchStatus := p.MatchStatus

	var bankID, fdRefNo string
	_ = pool.QueryRow(ctx,
		`SELECT COALESCE(bank_id,''), COALESCE(bank_fd_ref_no,'')
		 FROM investment.fd_master WHERE fd_id=$1 AND is_deleted=false`,
		tds.FDID).Scan(&bankID, &fdRefNo)
	if fdRefNo == "" {
		fdRefNo = tds.FdRefNo
	}
	if bankID == "" {
		bankID = tds.BankID
	}

	var varPctArg interface{}
	if variancePct >= 0 {
		varPctArg = variancePct
	}

	var resultID string
	err := pool.QueryRow(ctx, `
		INSERT INTO investment.fd_receipt_reconcile_result (
			reconcile_run_id, fd_id, fd_ref_no, entity_id, bank_id,
			result_type, receipt_id, tds_id,
			period_start, period_end, matching_basis,
			expected_amount, received_amount,
			amount_variance, amount_variance_pct,
			match_status, match_type, has_exception
		) VALUES (
			$1,$2,$3,$4,$5,
			'TDS',NULL,$6,
			$7,$8,$9,
			$10,$11,
			$12,$13,
			$14,'AUTO',false
		) RETURNING result_id`,
		runID, tds.FDID, fdRefNo, tds.EntityID, bankID,
		nullStr(tds.TDSID),
		tds.PeriodStart, tds.PeriodEnd, matchingBasis,
		expected, tds.Actual,
		variance, varPctArg,
		matchStatus,
	).Scan(&resultID)
	return resultID, err
}

// ─── Exception insert ─────────────────────────────────────────────────────────

// ExceptionParams groups arguments for insertException.
type ExceptionParams struct {
	Rec           ReceiptRow
	RunID         string
	ResultID      string
	ExceptionType string
	Severity      string // optional — derived from ExceptionType if empty
	ResultType    string // "INTEREST" or "TDS"
	TDSID         string // set when ResultType=TDS
	ExpectedAmt   float64
	ReceivedAmt   float64
	RaisedBy      string
}

func insertException(ctx context.Context, pool *pgxpool.Pool, p ExceptionParams) (string, error) {
	variance := p.ReceivedAmt - p.ExpectedAmt
	if variance < 0 {
		variance = -variance
	}
	resultType := p.ResultType
	if resultType == "" {
		resultType = "INTEREST"
	}
	severity := p.Severity
	if severity == "" {
		severity = severityFromStatus(p.ExceptionType)
	}

	var exID string
	err := pool.QueryRow(ctx, `
		INSERT INTO investment.fd_receipt_exception (
			exception_id, reconcile_run_id, result_id,
			fd_id, fd_ref_no, result_type,
			receipt_id, tds_id,
			exception_type, severity,
			expected_amount, received_amount, variance_amount,
			case_type, exception_status, raised_by, raised_at,
			is_active, is_deleted
		) VALUES (
			'IREX-' || UPPER(SUBSTR(REPLACE(gen_random_uuid()::TEXT,'-',''),1,7)),
			$1,$2,
			$3,$4,$5,
			$6,$7,
			$8,$9,
			$10,$11,$12,
			'VARIANCE','OPEN',$13,now(),
			true,false
		) RETURNING exception_id`,
		p.RunID, nullStr(p.ResultID),
		p.Rec.FDID, p.Rec.FdRefNo, resultType,
		nullStr(p.Rec.ReceiptID), nullStr(p.TDSID),
		p.ExceptionType, severity,
		p.ExpectedAmt, p.ReceivedAmt, variance,
		p.RaisedBy,
	).Scan(&exID)
	return exID, err
}

// ─── Utility ──────────────────────────────────────────────────────────────────

// parseDateRange parses two "YYYY-MM-DD" strings into time.Time values.
// On parse error it falls back to sensible sentinel dates so callers never panic.
func parseDateRange(start, end string) (time.Time, time.Time) {
	const layout = constants.DateFormat
	ps, err1 := time.Parse(layout, start)
	pe, err2 := time.Parse(layout, end)
	if err1 != nil {
		ps = time.Time{}
	}
	if err2 != nil {
		pe = time.Now()
	}
	return ps, pe
}

func markRunFailed(ctx context.Context, pool *pgxpool.Pool, runID, msg string) {
	pool.Exec(ctx, //nolint:errcheck
		`UPDATE investment.fd_receipt_reconcile_run
		 SET run_status='FAILED', error_message=$1, completed_at=now()
		 WHERE reconcile_run_id=$2`, msg, runID)
}

// ReconcileResultParams is kept for backwards compatibility with any callers
// that may still reference this type from other files.
type ReconcileResultParams struct {
	Rec              ReceiptRow
	RunID            string
	MatchStatus      string
	MatchingBasis    string
	Variance         float64
	VariancePct      float64
	HasException     bool
	CashflowID       string
	AccrualLedgerID  string
	ExpectedInterest float64
	ReceivedInterest float64
}
