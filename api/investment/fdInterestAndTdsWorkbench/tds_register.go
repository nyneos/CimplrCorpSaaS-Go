package fdInterestAndTdsWorkbench

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

func nullIfEmpty(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

func toFloat64(v interface{}) float64 {
	if v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int64:
		return float64(n)
	case int:
		return float64(n)
	default:
		// pgtype.Numeric and other types — convert via fmt
		f := 0.0
		fmt.Sscanf(fmt.Sprintf("%v", v), "%f", &f)
		return f
	}
}

// ─── TDS Register Create ────────────────────────────────────────────────────
// POST /investment/fd/tds-register/create
// Creates a standalone TDS receipt entry (backlog fill).
// receipt_id is optional — if not provided the entry is created without a link
// to an interest receipt.  fd_ref_no, bank_id, entity_name, bank_name are resolved
// from fd_master using fd_id — no hallucination, read from the actual table.

func CreateTDSRegister(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID            string  `json:"user_id"`
			EntityID          string  `json:"entity_id"`
			FDID              string  `json:"fd_id"`
			ReceiptID         string  `json:"receipt_id"`         // optional
			PeriodStart       string  `json:"period_start"`
			PeriodEnd         string  `json:"period_end"`
			TDSExpected       float64 `json:"tds_expected"`
			TDSDeductedActual float64 `json:"tds_deducted_actual"`
			GrossInterest     float64 `json:"gross_interest"`     // renamed from interest_amount
			TDSDeductionDate  string  `json:"tds_deduction_date"`
			TDSRateApplied    float64 `json:"tds_rate_applied"`
			TDSSection        string  `json:"tds_section"`
			HasPAN            bool    `json:"has_pan"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		userEmail := resolveUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		if req.FDID == "" || req.EntityID == "" || req.PeriodStart == "" || req.PeriodEnd == "" {
			api.RespondWithError(w, http.StatusBadRequest, "fd_id, entity_id, period_start, period_end are required")
			return
		}

		ctx := context.Background()

		// ── Resolve fd_ref_no, bank_id, entity_name, bank_name from fd_master ──
		var fdRefNo, bankID, entityName, bankName string
		_ = pool.QueryRow(ctx, `
			SELECT
				COALESCE(bank_fd_ref_no, ''),
				COALESCE(bank_id, ''),
				COALESCE(entity_name, ''),
				COALESCE(bank_name, '')
			FROM investment.fd_master
			WHERE fd_id = $1 AND is_deleted = false
			LIMIT 1`, req.FDID).Scan(&fdRefNo, &bankID, &entityName, &bankName)

		tdsVariance := req.TDSDeductedActual - req.TDSExpected
		exceptionRaised := tdsVariance != 0

		// receipt_id is nullable — pass NULL when not provided
		var receiptIDArg interface{}
		if strings.TrimSpace(req.ReceiptID) != "" {
			receiptIDArg = req.ReceiptID
		}
		// deduction_date falls back to period_end if not provided
		deductionDate := req.TDSDeductionDate
		if strings.TrimSpace(deductionDate) == "" {
			deductionDate = req.PeriodEnd
		}

		var tdsID string
		err := pool.QueryRow(ctx, `
			INSERT INTO investment.fd_tds_receipt (
				tds_id, receipt_id, fd_id, fd_ref_no, entity_id, bank_id,
				ingestion_source,
				period_start, period_end, deduction_date,
				gross_interest, tds_rate_applied, tds_rate_expected,
				tds_expected, tds_deducted_actual, tds_variance,
				has_pan, tds_section,
				tds_status, exception_raised,
				is_active, is_deleted
			) VALUES (
				'TDSR-' || UPPER(SUBSTR(REPLACE(gen_random_uuid()::TEXT,'-',''),1,8)),
				$1, $2, $3, $4, $5,
				'TDS_WORKBENCH',
				$6::date, $7::date, $8::date,
				$9, $10, $10,
				$11, $12, $13,
				$14, $15,
				'CAPTURED', $16,
				true, false
			) RETURNING tds_id`,
			receiptIDArg, req.FDID, fdRefNo, req.EntityID, bankID,
			req.PeriodStart, req.PeriodEnd, deductionDate,
			req.GrossInterest, req.TDSRateApplied,
			req.TDSExpected, req.TDSDeductedActual, tdsVariance,
			req.HasPAN, nullIfEmpty(req.TDSSection),
			exceptionRaised,
		).Scan(&tdsID)

		if err != nil {
			api.LogError("[TDSRegister] Create failed: %v", err)
			api.RespondWithError(w, http.StatusInternalServerError, "TDS register creation failed: "+err.Error())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "TDS register entry created successfully",
			"data": map[string]interface{}{
				"tds_id":              tdsID,
				"fd_id":               req.FDID,
				"fd_ref_no":           fdRefNo,
				"entity_id":           req.EntityID,
				"entity_name":         entityName,
				"bank_id":             bankID,
				"bank_name":           bankName,
				"ingestion_source":    "TDS_WORKBENCH",
				"tds_expected":        req.TDSExpected,
				"tds_deducted_actual": req.TDSDeductedActual,
				"tds_variance":        tdsVariance,
				"exception_raised":    exceptionRaised,
			},
		})
	}
}

// ─── TDS Register View ──────────────────────────────────────────────────────
// POST /investment/fd/tds-register/view
// Returns TDS register entries as-is with all receipt details

func GetTDSRegisterView(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			EntityID string `json:"entity_id"`
			FDID     string `json:"fd_id"`
			DateFrom string `json:"date_from"`
			DateTo   string `json:"date_to"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		userEmail := resolveUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := context.Background()

		// Simple query with basic joins
		sql := `
			SELECT 
				tds.tds_id,
				tds.receipt_id,
				tds.entity_id,
				tds.fd_id,
				tds.fd_ref_no,
				TO_CHAR(tds.period_start, 'YYYY-MM-DD') AS period_start,
				TO_CHAR(tds.period_end, 'YYYY-MM-DD') AS period_end,
				TO_CHAR(tds.deduction_date, 'YYYY-MM-DD') AS deduction_date,
				COALESCE(tds.tds_expected, 0) AS tds_expected,
				COALESCE(tds.tds_deducted_actual, 0) AS tds_deducted_actual,
				COALESCE(tds.tds_variance, 0) AS tds_variance,
				COALESCE(tds.gross_interest, 0) AS gross_interest,
				COALESCE(tds.tds_status, 'CAPTURED') AS tds_status,
				COALESCE(tds.exception_raised, false) AS exception_raised,
				TO_CHAR(tds.deduction_date, 'YYYY-MM-DD') AS deduction_date_fmt,
				COALESCE(fd.entity_name, '') AS entity_name,
				COALESCE(fd.bank_name, '') AS bank_name
			FROM investment.fd_tds_receipt tds
			LEFT JOIN investment.fd_master fd ON fd.fd_id = tds.fd_id
			WHERE tds.is_deleted = false
		  AND tds.ingestion_source = 'TDS_WORKBENCH'`

		args := []interface{}{}
		argIdx := 1

		if req.EntityID != "" {
			sql += fmt.Sprintf(" AND tds.entity_id = $%d", argIdx)
			args = append(args, req.EntityID)
			argIdx++
		}

		if req.FDID != "" {
			sql += fmt.Sprintf(" AND tds.fd_id = $%d", argIdx)
			args = append(args, req.FDID)
			argIdx++
		}

		if req.DateFrom != "" {
			sql += fmt.Sprintf(" AND tds.period_start >= $%d", argIdx)
			args = append(args, req.DateFrom)
			argIdx++
		}

		if req.DateTo != "" {
			sql += fmt.Sprintf(" AND tds.period_end <= $%d", argIdx)
			args = append(args, req.DateTo)
			argIdx++
		}

		sql += " ORDER BY tds.deduction_date DESC LIMIT 100"

		rows, err := pool.Query(ctx, sql, args...)
		if err != nil {
			api.LogError("[TDSRegisterView] Query failed: %v", err)
			api.RespondWithError(w, http.StatusInternalServerError, "TDS register query failed: "+err.Error())
			return
		}
		defer rows.Close()

		entries, err := rowsToMapSlice(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to parse TDS register data")
			return
		}

		// Calculate summary
		summary := map[string]interface{}{
			"total_count":        len(entries),
			"total_tds_expected": 0.0,
			"total_tds_actual":   0.0,
			"total_variance":     0.0,
		}

		for _, entry := range entries {
			summary["total_tds_expected"] = summary["total_tds_expected"].(float64) + toFloat64(entry["tds_expected"])
			summary["total_tds_actual"] = summary["total_tds_actual"].(float64) + toFloat64(entry["tds_deducted_actual"])
			summary["total_variance"] = summary["total_variance"].(float64) + toFloat64(entry["tds_variance"])
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    entries,
			"summary": summary,
		})
	}
}

// ─── TDS Reconciliation ─────────────────────────────────────────────────────
// POST /investment/fd/tds-register/reconcile
// Reconciles TDS entries with actual amounts

func ReconcileTDSRegister(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID              string  `json:"user_id"`
			EntityID            string  `json:"entity_id"`
			ReconcileAction     string  `json:"reconcile_action"` // AUTO or MANUAL
			ToleranceAmount     float64 `json:"tolerance_amount"`
			ReconciliationItems []struct {
				TDSID                string  `json:"tds_id"`
				ExpectedAmount       float64 `json:"expected_amount"`
				ActualAmount         float64 `json:"actual_amount"`
				ReconciliationAction string  `json:"reconciliation_action"` // ACCEPT, REJECT
				Notes                string  `json:"notes"`
			} `json:"reconciliation_items"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		userEmail := resolveUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := context.Background()

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Transaction start failed")
			return
		}
		defer tx.Rollback(ctx)

		updatedCount := 0

		if req.ReconcileAction == "AUTO" {
			// Auto-reconcile within tolerance
			var cmdTag pgconn.CommandTag
			cmdTag, err = tx.Exec(ctx, `
				UPDATE investment.fd_tds_receipt 
				SET tds_status = CASE
					WHEN ABS(tds_variance) <= $1 THEN 'APPROVED'
					ELSE 'CAPTURED'
				END
				WHERE entity_id = $2
				  AND ingestion_source = 'TDS_WORKBENCH'
				  AND tds_status = 'CAPTURED'
				  AND is_deleted = false`,
				req.ToleranceAmount, req.EntityID,
			)

			if err != nil {
				api.LogError("[TDSReconcile] Auto reconcile failed: %v", err)
				api.RespondWithError(w, http.StatusInternalServerError, "Auto reconciliation failed")
				return
			}
			updatedCount = int(cmdTag.RowsAffected())
		} else {
			// Manual reconciliation
			for _, item := range req.ReconciliationItems {
				variance := item.ActualAmount - item.ExpectedAmount
				newStatus := "CAPTURED"
				if item.ReconciliationAction == "ACCEPT" {
					newStatus = "APPROVED"
				} else if item.ReconciliationAction == "REJECT" {
					newStatus = "REJECTED"
				}

				_, err = tx.Exec(ctx, `
					UPDATE investment.fd_tds_receipt 
					SET tds_deducted_actual = $1,
						tds_variance = $2,
						tds_status = $3
					WHERE tds_id = $4`,
					item.ActualAmount, variance, newStatus, item.TDSID,
				)

				if err != nil {
					api.LogError("[TDSReconcile] Manual reconcile failed for %s: %v", item.TDSID, err)
					continue
				}

				updatedCount++
			}
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Transaction commit failed")
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "TDS reconciliation completed successfully",
			"data": map[string]interface{}{
				"updated_count": updatedCount,
				"action":        req.ReconcileAction,
			},
		})
	}
}

// ─── TDS Journal Entries ────────────────────────────────────────────────────
// POST /investment/fd/tds-register/journal
// Returns TDS journal entries (simplified for now without accounting integration)

// GetTDSJournalEntries returns journal entries from accounting_journal_entry for a given
// receipt_id, tds_id, or fd_id — with all line items included.
//
// Lookup priority:
//  1. receipt_id  → je.receipt_id = $receipt_id
//  2. tds_id      → resolve receipt_id from fd_tds_receipt, then same as above
//  3. fd_id       → je.fd_id = $fd_id  (returns all journals for that FD)
//  4. entity_id   → je.entity_id = $entity_id  (with optional date range)
func GetTDSJournalEntries(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string `json:"user_id"`
			ReceiptID string `json:"receipt_id"`
			TDSID     string `json:"tds_id"`
			FDID      string `json:"fd_id"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		userEmail := resolveUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := context.Background()

		// If tds_id provided, resolve receipt_id (may be NULL) and fd_id fallback
		if req.TDSID != "" && req.ReceiptID == "" {
			var rid, fdid pgtype.Text
			if err := pool.QueryRow(ctx,
				`SELECT receipt_id, fd_id FROM investment.fd_tds_receipt WHERE tds_id = $1 AND is_deleted = false`,
				req.TDSID).Scan(&rid, &fdid); err == nil {
				if rid.Valid && rid.String != "" {
					req.ReceiptID = rid.String
				} else if fdid.Valid && fdid.String != "" && req.FDID == "" {
					// TDS_WORKBENCH entry — no receipt_id, fall back to fd_id
					req.FDID = fdid.String
				}
			}
		}

		if req.ReceiptID == "" && req.FDID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "one of receipt_id, tds_id or fd_id is required")
			return
		}

		var filterCol, filterVal string
		if req.ReceiptID != "" {
			filterCol = "je.receipt_id"
			filterVal = req.ReceiptID
		} else {
			filterCol = "je.fd_id"
			filterVal = req.FDID
		}

		rows, err := pool.Query(ctx, `
			SELECT
				je.entry_id, je.activity_id, je.entity_id, je.entity_name,
				je.fd_id, je.receipt_id, je.accrual_run_id, je.accrual_ledger_id,
				TO_CHAR(je.entry_date, 'YYYY-MM-DD') AS entry_date,
				je.accounting_period, je.entry_type, je.description,
				je.total_debit, je.total_credit, je.status,
				TO_CHAR(je.created_at, 'YYYY-MM-DD HH24:MI:SS') AS created_at,
				je.created_by,
				jl.line_id, jl.line_number, jl.account_number, jl.account_name,
				jl.account_type, jl.debit_amount, jl.credit_amount, jl.narration,
				jl.folio_id, jl.demat_id
			FROM investment.accounting_journal_entry je
			LEFT JOIN investment.accounting_journal_entry_line jl ON jl.entry_id = je.entry_id
			WHERE `+filterCol+` = $1 AND je.is_deleted = false
			ORDER BY je.entry_date DESC, jl.line_number ASC`, filterVal)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Journal query failed: "+err.Error())
			return
		}
		defer rows.Close()

		payload, err := rowsToMapSlice(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to read journal rows")
			return
		}
		if payload == nil {
			payload = []map[string]interface{}{}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    payload,
		})
	}
}
