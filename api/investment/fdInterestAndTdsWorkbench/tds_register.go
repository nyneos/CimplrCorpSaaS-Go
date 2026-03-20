package fdInterestAndTdsWorkbench

import (
	"encoding/json"
	"fmt"
	"net/http"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ReconciliationItem represents a single TDS reconciliation item
type ReconciliationItem struct {
	TDSReceiptID         string  `json:"tds_receipt_id"`
	FDID                 string  `json:"fd_id"`
	ExpectedAmount       float64 `json:"expected_amount"`
	ActualAmount         float64 `json:"actual_amount"`
	ReconciliationAction string  `json:"reconciliation_action"` // ACCEPT, REJECT, INVESTIGATE
	Resolution           string  `json:"resolution"`            // FREE_TEXT
	NewStatus            string  `json:"new_status"`           // MATCHED, VARIANCE, PENDING
}

// ─── TDS Register Create ──────────────────────────────────────────────────────
// POST /investment/fd/tds-register/create
// Creates a new TDS register entry

func CreateTDSRegister(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID               string  `json:"user_id"`
			EntityID             string  `json:"entity_id"`
			FDID                 string  `json:"fd_id"`
			BookingID            string  `json:"booking_id"`
			ConfirmationID       string  `json:"confirmation_id"`
			TDSPlanID            string  `json:"tds_plan_id"`
			PeriodStart          string  `json:"period_start"`
			PeriodEnd            string  `json:"period_end"`
			TDSExpected          float64 `json:"tds_expected"`
			TDSDeductedActual    float64 `json:"tds_deducted_actual"`
			TDSVariance          float64 `json:"tds_variance"`
			VariancePercentage   float64 `json:"variance_percentage"`
			InterestAmount       float64 `json:"interest_amount"`
			NetAmountReceived    float64 `json:"net_amount_received"`
			BankCertificateNo    string  `json:"bank_certificate_no"`
			TDSDeductionDate     string  `json:"tds_deduction_date"`
			TDSFilingDate        string  `json:"tds_filing_date"`
			TDSChallanNo         string  `json:"tds_challan_no"`
			ExceptionRaised      bool    `json:"exception_raised"`
			ExceptionType        string  `json:"exception_type"`
			ExceptionSeverity    string  `json:"exception_severity"`
			ReconciliationStatus string  `json:"reconciliation_status"`
			Remarks              string  `json:"remarks"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		// Validation
		if req.EntityID == "" || req.FDID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "entity_id and fd_id are required")
			return
		}

		if req.TDSExpected < 0 || req.TDSDeductedActual < 0 {
			api.RespondWithError(w, http.StatusBadRequest, "TDS amounts cannot be negative")
			return
		}

		// Resolve user email
		userEmail := resolveUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// Calculate variance if not provided
		if req.TDSVariance == 0 && req.TDSExpected > 0 {
			req.TDSVariance = req.TDSDeductedActual - req.TDSExpected
			req.VariancePercentage = (req.TDSVariance / req.TDSExpected) * 100
		}

		// Auto-determine reconciliation status
		if req.ReconciliationStatus == "" {
			if req.TDSVariance == 0 {
				req.ReconciliationStatus = "MATCHED"
			} else if abs(req.TDSVariance) <= 1.0 { // ₹1 tolerance
				req.ReconciliationStatus = "TOLERANCE_MATCHED"
			} else {
				req.ReconciliationStatus = "VARIANCE"
			}
		}

		// Auto-raise exception for significant variances
		if !req.ExceptionRaised && abs(req.TDSVariance) > 100 { // ₹100 threshold
			req.ExceptionRaised = true
			req.ExceptionType = "TDS_VARIANCE"
			if abs(req.TDSVariance) > 1000 {
				req.ExceptionSeverity = "HIGH"
			} else {
				req.ExceptionSeverity = "MEDIUM"
			}
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Transaction start failed")
			return
		}
		defer tx.Rollback(ctx)

		var tdsRegisterID string
		err = tx.QueryRow(ctx, `
			INSERT INTO investment.fd_tds_receipt (
				entity_id, fd_id, booking_id, confirmation_id, tds_plan_id,
				period_start, period_end,
				tds_expected, tds_deducted_actual, tds_variance, variance_percentage,
				interest_amount, net_amount_received,
				bank_certificate_no, tds_deduction_date, tds_filing_date, tds_challan_no,
				exception_raised, exception_type, exception_severity,
				reconciliation_status, remarks,
				created_by, created_at, is_deleted
			) VALUES (
				$1, $2, $3, $4, $5,
				$6::date, $7::date,
				$8, $9, $10, $11,
				$12, $13,
				$14, $15::date, $16::date, $17,
				$18, $19, $20,
				$21, $22,
				$23, NOW(), false
			) RETURNING tds_receipt_id`,
			req.EntityID, req.FDID, req.BookingID, req.ConfirmationID, req.TDSPlanID,
			nullIfEmpty(req.PeriodStart), nullIfEmpty(req.PeriodEnd),
			req.TDSExpected, req.TDSDeductedActual, req.TDSVariance, req.VariancePercentage,
			req.InterestAmount, req.NetAmountReceived,
			req.BankCertificateNo, nullIfEmpty(req.TDSDeductionDate), nullIfEmpty(req.TDSFilingDate), req.TDSChallanNo,
			req.ExceptionRaised, req.ExceptionType, req.ExceptionSeverity,
			req.ReconciliationStatus, req.Remarks,
			userEmail,
		).Scan(&tdsRegisterID)

		if err != nil {
			api.LogError("[TDSRegister] Create failed: %v", err)
			api.RespondWithError(w, http.StatusInternalServerError, "TDS register creation failed: "+err.Error())
			return
		}

		// Create exception record if needed
		if req.ExceptionRaised {
			_, err = tx.Exec(ctx, `
				INSERT INTO investment.fd_receipt_exception (
					fd_id, tds_receipt_id, exception_type, severity,
					variance_amount, exception_status, raised_by, raised_at, is_deleted
				) VALUES ($1, $2, $3, $4, $5, 'OPEN', $6, NOW(), false)`,
				req.FDID, tdsRegisterID, req.ExceptionType, req.ExceptionSeverity,
				req.TDSVariance, userEmail,
			)
			if err != nil {
				api.LogError("[TDSRegister] Exception creation failed: %v", err)
				// Don't fail the main transaction for exception creation failure
			}
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Transaction commit failed")
			return
		}

		api.LogInfo("[TDSRegister] Created: tds_receipt_id=%s entity=%s fd=%s variance=%f", 
			tdsRegisterID, req.EntityID, req.FDID, req.TDSVariance)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":         true,
			"tds_receipt_id":  tdsRegisterID,
			"reconciliation_status": req.ReconciliationStatus,
			"exception_raised": req.ExceptionRaised,
			"message":         "TDS register entry created successfully",
		})
	}
}

// ─── TDS Register View ──────────────────────────────────────────────────────
// GET /investment/fd/tds-register/view?user_id=X&entity_id=Y[&fd_id=Z]
// Returns TDS register entries as-is with all receipt details

func GetTDSRegisterView(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID := r.URL.Query().Get("user_id")
		entityID := r.URL.Query().Get("entity_id")
		fdID := r.URL.Query().Get("fd_id")
		status := r.URL.Query().Get("reconciliation_status")
		fromDate := r.URL.Query().Get("from_date")
		toDate := r.URL.Query().Get("to_date")

		userEmail := resolveUserEmail(userID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// Build comprehensive query with all TDS details
		sql := `
			SELECT
				tds.tds_receipt_id,
				tds.entity_id,
				COALESCE(ent.entity_name, '') AS entity_name,
				tds.fd_id,
				COALESCE(fd.fd_ref_no, '') AS fd_ref_no,
				tds.booking_id,
				tds.confirmation_id,
				tds.tds_plan_id,
				TO_CHAR(tds.period_start, 'YYYY-MM-DD') AS period_start,
				TO_CHAR(tds.period_end, 'YYYY-MM-DD') AS period_end,
				COALESCE(tds.tds_expected, 0) AS tds_expected,
				COALESCE(tds.tds_deducted_actual, 0) AS tds_deducted_actual,
				COALESCE(tds.tds_variance, 0) AS tds_variance,
				COALESCE(tds.variance_percentage, 0) AS variance_percentage,
				COALESCE(tds.interest_amount, 0) AS interest_amount,
				COALESCE(tds.net_amount_received, 0) AS net_amount_received,
				COALESCE(tds.bank_certificate_no, '') AS bank_certificate_no,
				TO_CHAR(tds.tds_deduction_date, 'YYYY-MM-DD') AS tds_deduction_date,
				TO_CHAR(tds.tds_filing_date, 'YYYY-MM-DD') AS tds_filing_date,
				COALESCE(tds.tds_challan_no, '') AS tds_challan_no,
				COALESCE(tds.exception_raised, false) AS exception_raised,
				COALESCE(tds.exception_type, '') AS exception_type,
				COALESCE(tds.exception_severity, '') AS exception_severity,
				COALESCE(tds.reconciliation_status, '') AS reconciliation_status,
				COALESCE(tds.remarks, '') AS remarks,
				COALESCE(tds.created_by, '') AS created_by,
				TO_CHAR(tds.created_at, 'YYYY-MM-DD HH24:MI:SS') AS created_at,
				TO_CHAR(tds.updated_at, 'YYYY-MM-DD HH24:MI:SS') AS updated_at,
				-- Bank details
				COALESCE(fd.bank_name, '') AS bank_name,
				COALESCE(fd.bank_fd_ref_no, '') AS bank_fd_ref_no,
				COALESCE(fd.principal_amount, 0) AS principal_amount,
				COALESCE(fd.interest_rate, 0) AS interest_rate,
				-- Exception details
				COALESCE(ex.exception_id, '') AS exception_id,
				COALESCE(ex.exception_status, '') AS exception_status,
				TO_CHAR(ex.raised_at, 'YYYY-MM-DD HH24:MI:SS') AS exception_raised_at
			FROM investment.fd_tds_receipt tds
			LEFT JOIN master.entity_master ent ON ent.entity_id = tds.entity_id
			LEFT JOIN investment.fd_booking_request fd ON fd.fd_id = tds.fd_id
			LEFT JOIN investment.fd_receipt_exception ex ON ex.tds_receipt_id = tds.tds_receipt_id AND ex.is_deleted = false
			WHERE tds.is_deleted = false`

		args := []interface{}{}
		argIdx := 1

		if entityID != "" {
			sql += fmt.Sprintf(" AND tds.entity_id = $%d", argIdx)
			args = append(args, entityID)
			argIdx++
		}

		if fdID != "" {
			sql += fmt.Sprintf(" AND tds.fd_id = $%d", argIdx)
			args = append(args, fdID)
			argIdx++
		}

		if status != "" {
			sql += fmt.Sprintf(" AND tds.reconciliation_status = $%d", argIdx)
			args = append(args, status)
			argIdx++
		}

		if fromDate != "" {
			sql += fmt.Sprintf(" AND tds.period_start >= $%d", argIdx)
			args = append(args, fromDate)
			argIdx++
		}

		if toDate != "" {
			sql += fmt.Sprintf(" AND tds.period_end <= $%d", argIdx)
			args = append(args, toDate)
			argIdx++
		}

		sql += " ORDER BY tds.created_at DESC, tds.period_start DESC"

		rows, err := pool.Query(ctx, sql, args...)
		if err != nil {
			api.LogError("[TDSRegister] Query failed: %v", err)
			api.RespondWithError(w, http.StatusInternalServerError, "TDS register query failed: "+err.Error())
			return
		}
		defer rows.Close()

		registers, err := rowsToMapSlice(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Result processing failed: "+err.Error())
			return
		}

		// Get summary statistics
		var totalCount, matchedCount, varianceCount, exceptionCount int
		var totalExpected, totalActual, totalVariance float64

		for _, reg := range registers {
			totalCount++
			if expected, ok := reg["tds_expected"].(float64); ok {
				totalExpected += expected
			}
			if actual, ok := reg["tds_deducted_actual"].(float64); ok {
				totalActual += actual
			}
			if variance, ok := reg["tds_variance"].(float64); ok {
				totalVariance += variance
			}
			if status, ok := reg["reconciliation_status"].(string); ok {
				if status == "MATCHED" || status == "TOLERANCE_MATCHED" {
					matchedCount++
				} else {
					varianceCount++
				}
			}
			if raised, ok := reg["exception_raised"].(bool); ok && raised {
				exceptionCount++
			}
		}

		api.LogInfo("[TDSRegister] View: %d entries found for entity=%s", len(registers), entityID)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"summary": map[string]interface{}{
				"total_count":      totalCount,
				"matched_count":    matchedCount,
				"variance_count":   varianceCount,
				"exception_count":  exceptionCount,
				"total_expected":   totalExpected,
				"total_actual":     totalActual,
				"total_variance":   totalVariance,
			},
			"registers": registers,
		})
	}
}

// ─── TDS Reconciliation ──────────────────────────────────────────────────────
// POST /investment/fd/tds-register/reconcile
// Reconciles TDS entries, marks as matched/variance, resolves exceptions

func ReconcileTDSRegister(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID               string                 `json:"user_id"`
			EntityID             string                 `json:"entity_id"`
			ReconciliationType   string                 `json:"reconciliation_type"` // AUTO, MANUAL, BULK
			ReconciliationItems  []ReconciliationItem   `json:"reconciliation_items"`
			ToleranceAmount      float64                `json:"tolerance_amount"`     // Default 1.0
			AutoResolveMatched   bool                   `json:"auto_resolve_matched"` // Auto-resolve matched items
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		// Set defaults
		if req.ToleranceAmount == 0 {
			req.ToleranceAmount = 1.0 // ₹1 default tolerance
		}

		userEmail := resolveUserEmail(req.UserID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Transaction start failed")
			return
		}
		defer tx.Rollback(ctx)

		reconciled := 0
		matched := 0
		variances := 0
		resolved := 0

		// Process reconciliation items
		for _, item := range req.ReconciliationItems {
			variance := item.ActualAmount - item.ExpectedAmount
			
			// Determine final status
			finalStatus := item.NewStatus
			if finalStatus == "" {
				if variance == 0 {
					finalStatus = "MATCHED"
				} else if abs(variance) <= req.ToleranceAmount {
					finalStatus = "TOLERANCE_MATCHED"
				} else {
					finalStatus = "VARIANCE"
				}
			}

			// Update TDS receipt
			_, err = tx.Exec(ctx, `
				UPDATE investment.fd_tds_receipt 
				SET reconciliation_status = $1,
					tds_deducted_actual = $2,
					tds_variance = $3,
					variance_percentage = CASE 
						WHEN tds_expected > 0 THEN ($3 / tds_expected) * 100 
						ELSE 0 
					END,
					remarks = CASE 
						WHEN $4 != '' THEN COALESCE(remarks, '') || ' | Reconciliation: ' || $4
						ELSE remarks 
					END,
					updated_by = $5,
					updated_at = NOW()
				WHERE tds_receipt_id = $6`,
				finalStatus, item.ActualAmount, variance, item.Resolution, userEmail, item.TDSReceiptID,
			)

			if err != nil {
				api.LogError("[TDSReconcile] Update failed for %s: %v", item.TDSReceiptID, err)
				continue
			}

			// Handle exceptions
			if item.ReconciliationAction == "ACCEPT" && req.AutoResolveMatched && finalStatus == "MATCHED" {
				_, err = tx.Exec(ctx, `
					UPDATE investment.fd_receipt_exception 
					SET exception_status = 'RESOLVED',
						resolved_by = $1,
						resolved_at = NOW(),
						resolution_notes = 'Auto-resolved via reconciliation: ' || $2
					WHERE tds_receipt_id = $3 AND exception_status = 'OPEN'`,
					userEmail, item.Resolution, item.TDSReceiptID,
				)
				if err == nil {
					resolved++
				}
			}

			reconciled++
			if finalStatus == "MATCHED" || finalStatus == "TOLERANCE_MATCHED" {
				matched++
			} else {
				variances++
			}
		}

		// Bulk auto-reconciliation if requested
		if req.ReconciliationType == "AUTO" {
			bulkSQL := `
				UPDATE investment.fd_tds_receipt 
				SET reconciliation_status = CASE
						WHEN tds_variance = 0 THEN 'MATCHED'
						WHEN ABS(tds_variance) <= $1 THEN 'TOLERANCE_MATCHED'
						ELSE 'VARIANCE'
					END,
					updated_by = $2,
					updated_at = NOW()
				WHERE entity_id = $3 
				  AND reconciliation_status IN ('PENDING', '')
				  AND is_deleted = false`

			result, err := tx.Exec(ctx, bulkSQL, req.ToleranceAmount, userEmail, req.EntityID)
			if err != nil {
				api.LogError("[TDSReconcile] Bulk auto-reconciliation failed: %v", err)
			} else {
				bulkReconciled := result.RowsAffected()
				reconciled += int(bulkReconciled)
			}
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Transaction commit failed")
			return
		}

		api.LogInfo("[TDSReconcile] Completed: reconciled=%d matched=%d variances=%d resolved=%d entity=%s", 
			reconciled, matched, variances, resolved, req.EntityID)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":           true,
			"reconciled_count":  reconciled,
			"matched_count":     matched,
			"variance_count":    variances,
			"resolved_count":    resolved,
			"tolerance_amount":  req.ToleranceAmount,
			"message":           "TDS reconciliation completed successfully",
		})
	}
}

// Helper functions
func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// ─── TDS Journal Entries ────────────────────────────────────────────────────
// GET /investment/fd/tds-register/journal?user_id=X&entity_id=Y[&receipt_id=Z][&tds_id=A]
// Returns TDS journal entries with accounting details for receipts
// Supports filtering by receipt_id or tds_receipt_id for targeted lookups

func GetTDSJournalEntries(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID := r.URL.Query().Get("user_id")
		entityID := r.URL.Query().Get("entity_id")
		fdID := r.URL.Query().Get("fd_id")
		receiptID := r.URL.Query().Get("receipt_id")           // Interest receipt ID
		tdsReceiptID := r.URL.Query().Get("tds_id")            // TDS receipt ID  
		fromDate := r.URL.Query().Get("from_date")
		toDate := r.URL.Query().Get("to_date")
		postingStatus := r.URL.Query().Get("posting_status") // POSTED, UNPOSTED

		userEmail := resolveUserEmail(userID)
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// Comprehensive TDS journal query with accounting entries
		sql := `
			SELECT
				-- TDS Receipt Details
				tds.tds_receipt_id,
				tds.entity_id,
				COALESCE(ent.entity_name, '') AS entity_name,
				tds.fd_id,
				COALESCE(fd.fd_ref_no, '') AS fd_ref_no,
				COALESCE(fd.bank_name, '') AS bank_name,
				TO_CHAR(tds.period_start, 'YYYY-MM-DD') AS period_start,
				TO_CHAR(tds.period_end, 'YYYY-MM-DD') AS period_end,
				
				-- TDS Amounts
				COALESCE(tds.tds_expected, 0) AS tds_expected,
				COALESCE(tds.tds_deducted_actual, 0) AS tds_deducted_actual,
				COALESCE(tds.tds_variance, 0) AS tds_variance,
				COALESCE(tds.interest_amount, 0) AS gross_interest,
				COALESCE(tds.net_amount_received, 0) AS net_amount_received,
				
				-- Certificate and Filing Details
				COALESCE(tds.bank_certificate_no, '') AS bank_certificate_no,
				TO_CHAR(tds.tds_deduction_date, 'YYYY-MM-DD') AS tds_deduction_date,
				TO_CHAR(tds.tds_filing_date, 'YYYY-MM-DD') AS tds_filing_date,
				COALESCE(tds.tds_challan_no, '') AS tds_challan_no,
				
				-- Status and Classification
				COALESCE(tds.reconciliation_status, '') AS reconciliation_status,
				COALESCE(tds.exception_raised, false) AS exception_raised,
				COALESCE(tds.exception_type, '') AS exception_type,
				
				-- Journal Entry Details (if exists)
				COALESCE(je.journal_entry_id, '') AS journal_entry_id,
				COALESCE(je.voucher_number, '') AS voucher_number,
				TO_CHAR(je.posting_date, 'YYYY-MM-DD') AS posting_date,
				COALESCE(je.posting_status, 'UNPOSTED') AS posting_status,
				COALESCE(je.total_debit, 0) AS total_debit,
				COALESCE(je.total_credit, 0) AS total_credit,
				
				-- Interest Receipt Linkage (if exists)
				COALESCE(ir.receipt_id, '') AS interest_receipt_id,
				COALESCE(ir.receipt_status, '') AS interest_receipt_status,
				COALESCE(ir.gross_interest_received, 0) AS interest_gross_received,
				TO_CHAR(ir.receipt_date, 'YYYY-MM-DD') AS interest_receipt_date,
				
				-- Account Mapping
				COALESCE(am_tds.account_code, '') AS tds_account_code,
				COALESCE(am_tds.account_name, '') AS tds_account_name,
				COALESCE(am_int.account_code, '') AS interest_account_code,
				COALESCE(am_int.account_name, '') AS interest_account_name,
				
				-- Timestamps
				TO_CHAR(tds.created_at, 'YYYY-MM-DD HH24:MI:SS') AS tds_created_at,
				TO_CHAR(je.created_at, 'YYYY-MM-DD HH24:MI:SS') AS journal_created_at
				
			FROM investment.fd_tds_receipt tds
			LEFT JOIN master.entity_master ent ON ent.entity_id = tds.entity_id
			LEFT JOIN investment.fd_booking_request fd ON fd.fd_id = tds.fd_id
			
			-- Interest Receipt linkage (for receipt_id filtering)
			LEFT JOIN investment.fd_interest_receipt ir ON ir.fd_id = tds.fd_id
				AND ir.is_deleted = false
			
			-- Journal Entry linkage (if TDS has been posted)
			LEFT JOIN accounting.journal_entries je ON je.source_id = tds.tds_receipt_id 
				AND je.source_type = 'TDS_RECEIPT' AND je.is_deleted = false
			
			-- Account mappings for TDS and Interest
			LEFT JOIN accounting.account_master am_tds ON am_tds.account_type = 'TDS_RECEIVABLE'
				AND am_tds.entity_id = tds.entity_id AND am_tds.is_active = true
			LEFT JOIN accounting.account_master am_int ON am_int.account_type = 'INTEREST_INCOME'
				AND am_int.entity_id = tds.entity_id AND am_int.is_active = true
				
			WHERE tds.is_deleted = false`

		args := []interface{}{}
		argIdx := 1

		if entityID != "" {
			sql += fmt.Sprintf(" AND tds.entity_id = $%d", argIdx)
			args = append(args, entityID)
			argIdx++
		}

		if fdID != "" {
			sql += fmt.Sprintf(" AND tds.fd_id = $%d", argIdx)
			args = append(args, fdID)
			argIdx++
		}

		// Filter by specific TDS receipt ID
		if tdsReceiptID != "" {
			sql += fmt.Sprintf(" AND tds.tds_receipt_id = $%d", argIdx)
			args = append(args, tdsReceiptID)
			argIdx++
		}

		// Filter by interest receipt ID (find related TDS entries)
		if receiptID != "" {
			sql += fmt.Sprintf(" AND ir.receipt_id = $%d", argIdx)
			args = append(args, receiptID)
			argIdx++
		}

		if fromDate != "" {
			sql += fmt.Sprintf(" AND tds.period_start >= $%d", argIdx)
			args = append(args, fromDate)
			argIdx++
		}

		if toDate != "" {
			sql += fmt.Sprintf(" AND tds.period_end <= $%d", argIdx)
			args = append(args, toDate)
			argIdx++
		}

		if postingStatus != "" {
			if postingStatus == "UNPOSTED" {
				sql += " AND je.journal_entry_id IS NULL"
			} else {
				sql += fmt.Sprintf(" AND je.posting_status = $%d", argIdx)
				args = append(args, postingStatus)
				argIdx++
			}
		}

		sql += " ORDER BY tds.tds_deduction_date DESC, tds.created_at DESC"

		rows, err := pool.Query(ctx, sql, args...)
		if err != nil {
			api.LogError("[TDSJournal] Query failed: %v", err)
			api.RespondWithError(w, http.StatusInternalServerError, "TDS journal query failed: "+err.Error())
			return
		}
		defer rows.Close()

		journals, err := rowsToMapSlice(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Result processing failed: "+err.Error())
			return
		}

		// Calculate journal summary
		var totalEntries, postedEntries, unpostedEntries int
		var totalTDSExpected, totalTDSActual, totalGrossInterest, totalNetReceived float64

		for _, journal := range journals {
			totalEntries++
			
			if tdsExp, ok := journal["tds_expected"].(float64); ok {
				totalTDSExpected += tdsExp
			}
			if tdsAct, ok := journal["tds_deducted_actual"].(float64); ok {
				totalTDSActual += tdsAct
			}
			if grossInt, ok := journal["gross_interest"].(float64); ok {
				totalGrossInterest += grossInt
			}
			if netRec, ok := journal["net_amount_received"].(float64); ok {
				totalNetReceived += netRec
			}
			
			if postStatus, ok := journal["posting_status"].(string); ok && postStatus == "POSTED" {
				postedEntries++
			} else {
				unpostedEntries++
			}
		}

		api.LogInfo("[TDSJournal] Retrieved: %d entries, %d posted, %d unposted for entity=%s tds_id=%s receipt_id=%s", 
			totalEntries, postedEntries, unpostedEntries, entityID, tdsReceiptID, receiptID)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"summary": map[string]interface{}{
				"total_entries":       totalEntries,
				"posted_entries":      postedEntries,
				"unposted_entries":    unpostedEntries,
				"total_tds_expected":  totalTDSExpected,
				"total_tds_actual":    totalTDSActual,
				"total_gross_interest": totalGrossInterest,
				"total_net_received":  totalNetReceived,
				"tds_variance_total":  totalTDSActual - totalTDSExpected,
			},
			"journal_entries": journals,
		})
	}
}