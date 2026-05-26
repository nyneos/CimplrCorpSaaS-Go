package fdReceipt

import (
	"context"
	"encoding/json"
	"net/http"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// reconcileResultDetail mirrors one row from /reconcile/results for the linked result_id.
type reconcileResultDetail struct {
	ResultID          string              `json:"result_id"`
	ReconcileRunID    string              `json:"reconcile_run_id"`
	ResultType        string              `json:"result_type"`
	FDID              string              `json:"fd_id"`
	FdRefNo           string              `json:"fd_ref_no"`
	EntityID          string              `json:"entity_id"`
	EntityName        string              `json:"entity_name"`
	BankID            string              `json:"bank_id"`
	BankName          string              `json:"bank_name"`
	PeriodStart       string              `json:"period_start"`
	PeriodEnd         string              `json:"period_end"`
	MatchingBasis     string              `json:"matching_basis"`
	ReceiptID         string              `json:"receipt_id,omitempty"`
	TDSID             string              `json:"tds_id,omitempty"`
	ExpectedAmount    float64             `json:"expected_amount"`
	ReceivedAmount    float64             `json:"received_amount"`
	Variance          float64             `json:"variance"`
	VariancePct       float64             `json:"variance_pct"`
	MatchStatus       string              `json:"match_status"`
	MatchType         string              `json:"match_type"`
	HasException      bool                `json:"has_exception"`
	ReceiptStatus     string              `json:"receipt_status,omitempty"`
	ReconcileStatus   string              `json:"reconcile_status,omitempty"`
	JournalEntryID    string              `json:"journal_entry_id,omitempty"`
	TDSStatus         string              `json:"tds_status,omitempty"`
	TDSJournalEntryID string              `json:"tds_journal_entry_id,omitempty"`
	Cashflows         []CashflowLine      `json:"cashflows"`
	AccrualLedger     []AccrualLedgerLine `json:"accrual_ledger"`
}

// GetExceptionDetail returns variance case + audit trail + linked reconcile context.
// POST /investment/fd/exception/detail
func GetExceptionDetail(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string `json:"user_id"`
			ExceptionID string `json:"exception_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.ExceptionID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "exception_id is required")
			return
		}
		if resolveUserEmail(r.Context()) == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		repairClosedAcceptException(ctx, pool, req.ExceptionID)

		hdr, err := loadVarianceCase(ctx, pool, req.ExceptionID)
		if err != nil {
			if err == pgx.ErrNoRows {
				api.RespondWithError(w, http.StatusNotFound, constants.ErrExceptionNotFound)
				return
			}
			api.RespondWithError(w, http.StatusInternalServerError, "Load variance failed: "+err.Error())
			return
		}

		audit, _ := loadVarianceAuditTrail(ctx, pool, req.ExceptionID)

		var result *reconcileResultDetail
		if hdr.ResultID != "" {
			if rd, rErr := loadReconcileResultDetail(ctx, pool, hdr.ResultID); rErr == nil {
				result = rd
			}
		}

		receiptID, tdsID := resolveExceptionReceiptLinks(ctx, pool, hdr.ReceiptID, hdr.TDSID)
		var receipt map[string]interface{}
		if hdr.ResultType == "TDS" && tdsID != "" {
			receipt, _ = loadTDSReceiptMap(ctx, pool, tdsID)
		} else if receiptID != "" {
			receipt, _ = loadInterestReceiptMap(ctx, pool, receiptID)
		}

		var run map[string]interface{}
		if hdr.ReconcileRunID != "" {
			run, _ = loadReconcileRunMap(ctx, pool, hdr.ReconcileRunID)
		}

		canPost := false
		if receiptID != "" {
			canPost = checkReceiptPostingEligibility(ctx, pool, receiptID) == ""
		}
		checkerApproved := hasVarianceCheckerApproval(ctx, pool, req.ExceptionID)

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":           true,
			"variance":          hdr,
			"exception":         hdr,
			"audit_trail":       audit,
			"audit":             audit,
			"reconcile_result":  result,
			"receipt":           receipt,
			"reconcile_run":     run,
			"checker_approved":  checkerApproved,
			"allowed_actions":   varianceAllowedActions(hdr.WorkflowStatus, checkerApproved),
			"resolve_form":      exceptionResolveFormOptions(),
			"can_post_journals": canPost,
			"awaiting_close": checkerApproved && hdr.WorkflowStatus == "IN_REVIEW",
			"is_locked":      hdr.WorkflowStatus == "CLOSE",
			"workflow": map[string]string{
				"statuses": "OPEN | IN_REVIEW | CLOSE",
				"raised":   "CREATE + PENDING_APPROVAL",
				"resolve":  "OPEN → IN_REVIEW (EDIT + PENDING_APPROVAL)",
				"edit":     "IN_REVIEW before approve only (PENDING_EDIT_APPROVAL)",
				"approve":  "checker OK — leaves pending queue; only close|reject; bulk exception_ids[]",
				"close":    "IN_REVIEW+approved → CLOSE + reconcile status update; bulk exception_ids[]",
				"reject":   "→ OPEN (audit REJECTED); bulk exception_ids[]",
				"post":     "POST /receipt/post-journals after CLOSE+ACCEPT",
			},
		})
	}
}

func loadReconcileResultDetail(ctx context.Context, pool *pgxpool.Pool, resultID string) (*reconcileResultDetail, error) {
	var rd reconcileResultDetail
	err := pool.QueryRow(ctx, `
		SELECT
			rr.result_id,
			rr.reconcile_run_id,
			rr.result_type,
			rr.fd_id,
			COALESCE(m.bank_fd_ref_no, rr.fd_ref_no, ''),
			rr.entity_id,
			COALESCE(m.entity_name, rr.entity_id, ''),
			rr.bank_id,
			COALESCE(m.bank_name, ''),
			rr.period_start::text,
			rr.period_end::text,
			COALESCE(rr.matching_basis,'BOTH'),
			COALESCE(rr.receipt_id,''),
			COALESCE(rr.tds_id,''),
			COALESCE(rr.expected_amount,0),
			COALESCE(rr.received_amount,0),
			COALESCE(rr.amount_variance,0),
			COALESCE(rr.amount_variance_pct,0),
			rr.match_status,
			COALESCE(rr.match_type,''),
			COALESCE(rr.has_exception,false)
		FROM investment.fd_receipt_reconcile_result rr
		LEFT JOIN investment.fd_master m ON m.fd_id = rr.fd_id AND m.is_deleted = false
		WHERE rr.result_id = $1`,
		resultID,
	).Scan(
		&rd.ResultID, &rd.ReconcileRunID, &rd.ResultType,
		&rd.FDID, &rd.FdRefNo, &rd.EntityID, &rd.EntityName,
		&rd.BankID, &rd.BankName,
		&rd.PeriodStart, &rd.PeriodEnd, &rd.MatchingBasis,
		&rd.ReceiptID, &rd.TDSID,
		&rd.ExpectedAmount, &rd.ReceivedAmount, &rd.Variance, &rd.VariancePct,
		&rd.MatchStatus, &rd.MatchType, &rd.HasException,
	)
	if err != nil {
		return nil, err
	}

	ps, pe := parseDateRange(rd.PeriodStart, rd.PeriodEnd)
	forTDS := rd.ResultType == "TDS"
	basis := rd.MatchingBasis
	if basis == "CASHFLOW" || basis == "BOTH" {
		rd.Cashflows = loadCashflowsForReceipt(ctx, pool, rd.FDID, ps, pe, forTDS)
	}
	if basis == "ACCRUAL" || basis == "BOTH" {
		rd.AccrualLedger = loadAccrualLedgerForReceipt(ctx, pool, rd.FDID, ps, pe, forTDS)
	}
	if rd.Cashflows == nil {
		rd.Cashflows = []CashflowLine{}
	}
	if rd.AccrualLedger == nil {
		rd.AccrualLedger = []AccrualLedgerLine{}
	}

	applyReconcilePostingEnrichment(ctx, pool, rd.ReceiptID, rd.TDSID, rd.ResultType,
		reconcileEnrichOutputs{ReceiptStatus: &rd.ReceiptStatus, ReconcileStatus: &rd.ReconcileStatus, JournalEntryID: &rd.JournalEntryID, TDSStatus: &rd.TDSStatus, TDSJournalEntryID: &rd.TDSJournalEntryID, Cashflows: &rd.Cashflows})

	return &rd, nil
}

func loadInterestReceiptMap(ctx context.Context, pool *pgxpool.Pool, receiptID string) (map[string]interface{}, error) {
	rows, err := pool.Query(ctx, `
		SELECT receipt_id, fd_id, fd_ref_no, entity_id, entity_name,
		       bank_id, bank_name,
		       receipt_date::text, period_start::text, period_end::text,
		       gross_interest_received, tds_amount_deducted, net_amount_received,
		       receipt_status, reconcile_status, reconcile_run_id,
		       COALESCE(journal_entry_id,'')
		FROM investment.fd_interest_receipt
		WHERE receipt_id=$1 AND COALESCE(is_deleted,false)=false`, receiptID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return firstRowMap(rows)
}

func loadTDSReceiptMap(ctx context.Context, pool *pgxpool.Pool, tdsID string) (map[string]interface{}, error) {
	rows, err := pool.Query(ctx, `
		SELECT tds_id, receipt_id, fd_id, fd_ref_no, entity_id,
		       period_start::text, period_end::text, deduction_date::text,
		       gross_interest, tds_expected, tds_deducted_actual, tds_variance,
		       tds_status, reconcile_status, reconcile_run_id,
		       COALESCE(journal_entry_id,'')
		FROM investment.fd_tds_receipt
		WHERE tds_id=$1 AND COALESCE(is_deleted,false)=false`, tdsID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return firstRowMap(rows)
}

func loadReconcileRunMap(ctx context.Context, pool *pgxpool.Pool, runID string) (map[string]interface{}, error) {
	rows, err := pool.Query(ctx, `
		SELECT reconcile_run_id, entity_id, entity_name,
		       period_start::text, period_end::text,
		       matching_basis, run_status, trigger_mode,
		       triggered_by, triggered_at::text, completed_at::text,
		       COALESCE(receipts_matched,0), COALESCE(receipts_unmatched,0),
		       COALESCE(receipts_exception,0), COALESCE(receipts_processed,0)
		FROM investment.fd_receipt_reconcile_run
		WHERE reconcile_run_id=$1`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return firstRowMap(rows)
}

func firstRowMap(rows pgx.Rows) (map[string]interface{}, error) {
	fields := rows.FieldDescriptions()
	if !rows.Next() {
		return nil, rows.Err()
	}
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	out := make(map[string]interface{}, len(fields))
	for i, f := range fields {
		if vals[i] == nil {
			out[string(f.Name)] = ""
		} else {
			out[string(f.Name)] = vals[i]
		}
	}
	return out, rows.Err()
}

// applyExceptionReject restores receipt reconcile_status after reject → OPEN.
func applyExceptionReject(ctx context.Context, pool *pgxpool.Pool, exceptionID string) {
	var receiptID, tdsID, resultID string
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(receipt_id,''), COALESCE(tds_id,''), COALESCE(result_id,'')
		FROM investment.fd_receipt_exception
		WHERE exception_id=$1 AND COALESCE(is_deleted,false)=false`,
		exceptionID,
	).Scan(&receiptID, &tdsID, &resultID)
	if err != nil {
		return
	}
	receiptID, tdsID = resolveExceptionReceiptLinks(ctx, pool, receiptID, tdsID)

	if resultID != "" {
		_, _ = pool.Exec(ctx, `
			UPDATE investment.fd_receipt_reconcile_result
			SET has_exception=false
			WHERE result_id=$1`, resultID)
	}

	UpdateAggregateReceiptStatus(ctx, pool, receiptID, tdsID)
}
