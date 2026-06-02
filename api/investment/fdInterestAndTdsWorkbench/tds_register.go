package fdInterestAndTdsWorkbench

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"

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

// checkFDDates validates that date strings fall inside the FD start-to-maturity window.
// Returns a human-friendly error message, or "" when all dates are valid.
func checkFDDates(fdStart, fdMaturity time.Time, labels, values []string) string {
	for i, v := range values {
		if v == "" {
			continue
		}
		t, err := time.Parse(constants.DateFormat, v)
		if err != nil {
			return fmt.Sprintf("%s must be in YYYY-MM-DD format", labels[i])
		}
		if t.Before(fdStart) || t.After(fdMaturity) {
			return fmt.Sprintf(
				"%s (%s) must be within this FD's window (%s to %s)",
				labels[i], v, fdStart.Format(constants.DateFormat), fdMaturity.Format(constants.DateFormat))
		}
	}
	return ""
}

func checkFDPeriodDates(fdStart, fdMaturity time.Time, periodStart, periodEnd string) string {
	if errMsg := checkFDDates(fdStart, fdMaturity,
		[]string{"period_start", "period_end"},
		[]string{periodStart, periodEnd}); errMsg != "" {
		return errMsg
	}
	if periodStart == "" || periodEnd == "" {
		return ""
	}
	ps, psErr := time.Parse(constants.DateFormat, periodStart)
	pe, peErr := time.Parse(constants.DateFormat, periodEnd)
	if psErr != nil || peErr != nil {
		return ""
	}
	if ps.After(pe) {
		return fmt.Sprintf("period_start (%s) cannot be after period_end (%s)", periodStart, periodEnd)
	}
	return ""
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
			ReceiptID         string  `json:"receipt_id"` // optional
			PeriodStart       string  `json:"period_start"`
			PeriodEnd         string  `json:"period_end"`
			TDSExpected       float64 `json:"tds_expected"`
			TDSDeductedActual float64 `json:"tds_deducted_actual"`
			GrossInterest     float64 `json:"gross_interest"` // renamed from interest_amount
			TDSDeductionDate  string  `json:"tds_deduction_date"`
			TDSRateApplied    float64 `json:"tds_rate_applied"`
			TDSSection        string  `json:"tds_section"`
			HasPAN            bool    `json:"has_pan"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		if req.FDID == "" || req.EntityID == "" || req.PeriodStart == "" || req.PeriodEnd == "" {
			api.RespondWithError(w, http.StatusBadRequest, "fd_id, entity_id, period_start, period_end are required")
			return
		}

		ctx := r.Context()

		// ── Resolve fd_ref_no, bank_id, entity_name, bank_name, date bounds from fd_master ──
		var fdRefNo, bankID, entityName, bankName string
		var fdStart, fdMaturity time.Time
		if err := pool.QueryRow(ctx, `
			SELECT
				COALESCE(bank_fd_ref_no, ''),
				COALESCE(bank_id, ''),
				COALESCE(entity_name, ''),
				COALESCE(bank_name, ''),
				start_date,
				maturity_date
			FROM investment.fd_master
			WHERE fd_id = $1 AND is_deleted = false
			LIMIT 1`, req.FDID).Scan(&fdRefNo, &bankID, &entityName, &bankName, &fdStart, &fdMaturity); err != nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFDNotFound)
			return
		}

		// deduction_date falls back to period_end if not provided
		deductionDate := req.TDSDeductionDate
		if strings.TrimSpace(deductionDate) == "" {
			deductionDate = req.PeriodEnd
		}
		if errMsg := checkFDPeriodDates(fdStart, fdMaturity, req.PeriodStart, req.PeriodEnd); errMsg != "" {
			api.RespondWithError(w, http.StatusBadRequest, errMsg)
			return
		}
		if errMsg := checkFDDates(fdStart, fdMaturity,
			[]string{"tds_deduction_date"},
			[]string{deductionDate}); errMsg != "" {
			api.RespondWithError(w, http.StatusBadRequest, errMsg)
			return
		}

		tdsVariance := req.TDSDeductedActual - req.TDSExpected
		exceptionRaised := tdsVariance != 0

		// receipt_id is nullable — pass NULL when not provided
		var receiptIDArg interface{}
		if strings.TrimSpace(req.ReceiptID) != "" {
			receiptIDArg = req.ReceiptID
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

		// Create audit entry
		_, auditErr := pool.Exec(ctx, `
			INSERT INTO investment.fd_tds_receipt_audit (
				tds_id, action_type, processing_status, requested_by, requested_at
			) VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now())`, tdsID, userEmail)
		if auditErr != nil {
			api.LogError("[TDSRegister] Audit insert failed for %s: %v", tdsID, auditErr)
			// Don't fail the request — audit is non-critical for workbench entries
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
		go func(id, uEmail, fID, eID string) {
			defer func() { recover() }() //nolint:errcheck
			bgCtx := context.Background()
			instID, instErr := approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
				ModuleCode:       "FIXED_DEPOSIT",
				EntityCode:       eID,
				TransactionType:  "FD_TDS_REGISTER_CREATE",
				RecordID:         id,
				RecordTable:      "investment.fd_tds_receipt",
				AuditTable:       "investment.fd_tds_receipt_audit",
				AuditIDColumn:    "tds_id",
				ActionType:       "CREATE",
				SubmittedByEmail: uEmail,
			})
			if instErr != nil {
				api.LogError("[FDTDS] CreateInstance CREATE failed: %v", instErr)
			} else if instID != "" {
				api.LogInfo("[FDTDS] CreateInstance CREATE created instance=%s", instID)
			}
			notifcatalog.TriggerNotification(bgCtx, pool, "/investment/fd/tds-register/create", id, map[string]interface{}{
				"record_id": id, "event": "FD_TDS_REGISTER_CREATED", "actor_email": uEmail,
				"fd_id": fID, "entity_id": eID,
			})
		}(tdsID, userEmail, req.FDID, req.EntityID)
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

		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// Simple query with basic joins + audit history
		sql := `
			WITH latest_audit AS (
			  SELECT DISTINCT ON (a.tds_id)
			    a.tds_id, a.action_type, a.processing_status,
			    a.requested_by, a.requested_at,
			    a.checker_by, a.checker_at, a.checker_comment
			  FROM investment.fd_tds_receipt_audit a
			  ORDER BY a.tds_id,
			    GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamptz),COALESCE(a.checker_at,'1970-01-01'::timestamptz)) DESC,
			    a.audit_id DESC
			),
			history AS (
			  SELECT
			    tds_id,
			    MAX(CASE WHEN action_type='CREATE' THEN requested_by END)                                   AS created_by_audit,
			    MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at_audit,
			    MAX(CASE WHEN action_type='EDIT'   THEN requested_by END)                                   AS edited_by,
			    MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
			    MAX(CASE WHEN action_type='DELETE' THEN requested_by END)                                   AS deleted_by,
			    MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
			  FROM investment.fd_tds_receipt_audit
			  GROUP BY tds_id
			)
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
				COALESCE(fd.bank_name, '') AS bank_name,
				COALESCE(la.processing_status,'')      AS processing_status,
				COALESCE(la.action_type,'')            AS action_type,
				COALESCE(la.requested_by,'')           AS requested_by,
				COALESCE(TO_CHAR(la.requested_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
				COALESCE(la.checker_by,'')             AS checker_by,
				COALESCE(TO_CHAR(la.checker_at,'YYYY-MM-DD HH24:MI:SS'),'')   AS checker_at,
				COALESCE(la.checker_comment,'')        AS checker_comment,
				COALESCE(h.created_by_audit,'')        AS created_by_audit,
				COALESCE(h.created_at_audit,'')        AS created_at_audit,
				COALESCE(h.edited_by,'')               AS edited_by,
				COALESCE(h.edited_at,'')               AS edited_at,
				COALESCE(h.deleted_by,'')              AS deleted_by,
				COALESCE(h.deleted_at,'')              AS deleted_at,
				COALESCE(tds.reconcile_status,'PENDING')  AS reconcile_status,
				COALESCE(tds.reconcile_run_id,'')         AS reconcile_run_id
			FROM investment.fd_tds_receipt tds
			LEFT JOIN investment.fd_master fd ON fd.fd_id = tds.fd_id
			LEFT JOIN latest_audit la ON la.tds_id = tds.tds_id
			LEFT JOIN history h ON h.tds_id = tds.tds_id
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

		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxStartFailed)
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
					newStatus = constants.StatusApproved
				} else if item.ReconciliationAction == "REJECT" {
					newStatus = constants.StatusRejected
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
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxCommitFailed)
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
		go func(uEmail, eID, action string) {
			defer func() { recover() }() //nolint:errcheck
			bgCtx := context.Background()
			instID, instErr := approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
				ModuleCode:       "FIXED_DEPOSIT",
				EntityCode:       eID,
				TransactionType:  "FD_TDS_REGISTER_RECONCILE",
				RecordID:         eID,
				RecordTable:      "investment.fd_tds_receipt",
				AuditTable:       "investment.fd_tds_receipt_audit",
				AuditIDColumn:    "tds_id",
				ActionType:       "RECONCILE",
				SubmittedByEmail: uEmail,
			})
			if instErr != nil {
				api.LogError("[FDTDS] CreateInstance RECONCILE failed: %v", instErr)
			} else if instID != "" {
				api.LogInfo("[FDTDS] CreateInstance RECONCILE created instance=%s", instID)
			}
			notifcatalog.TriggerNotification(bgCtx, pool, "/investment/fd/tds-register/reconcile", eID, map[string]interface{}{
				"record_id": eID, "event": "FD_TDS_REGISTER_RECONCILED", "actor_email": uEmail,
				"entity_id": eID, "reconcile_action": action,
			})
		}(userEmail, req.EntityID, req.ReconcileAction)
	}
}

// ─── TDS Register Approve ──────────────────────────────────────────────────
// POST /investment/fd/tds-register/approve
// Moves a single TDS entry from CAPTURED → APPROVED and writes checker audit.

func ApproveTDSRegister(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string `json:"user_id"`
			TDSID   string `json:"tds_id"`
			Comment string `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if strings.TrimSpace(req.TDSID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrTDSIDRequired)
			return
		}

		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		// Only CAPTURED entries can be approved
		var currentStatus string
		err := pool.QueryRow(ctx,
			`SELECT tds_status FROM investment.fd_tds_receipt WHERE tds_id = $1 AND is_deleted = false`,
			req.TDSID).Scan(&currentStatus)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, "TDS entry not found")
			return
		}
		if currentStatus != "CAPTURED" {
			api.RespondWithError(w, http.StatusBadRequest,
				"only CAPTURED entries can be approved (current status: "+currentStatus+")")
			return
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxStartFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		// Promote status
		if _, err = tx.Exec(ctx,
			`UPDATE investment.fd_tds_receipt SET tds_status = 'APPROVED' WHERE tds_id = $1`,
			req.TDSID); err != nil {
			api.LogError("[TDSApprove] status update failed for %s: %v", req.TDSID, err)
			api.RespondWithError(w, http.StatusInternalServerError, "failed to approve TDS entry")
			return
		}

		// Write checker audit row
		if _, err = tx.Exec(ctx, `
			UPDATE investment.fd_tds_receipt_audit
			SET processing_status = 'APPROVED',
			    checker_by        = $1,
			    checker_at        = now(),
			    checker_comment   = $2
			WHERE tds_id = $3
			  AND processing_status = 'PENDING_APPROVAL'`,
			userEmail, nullIfEmpty(req.Comment), req.TDSID); err != nil {
			api.LogError("[TDSApprove] audit update failed for %s: %v", req.TDSID, err)
		}

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxCommitFailed)
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
			"success": true,
			"message": "TDS entry approved successfully",
			"tds_id":  req.TDSID,
		})
	}
}

// ─── TDS Register Update ───────────────────────────────────────────────────
// POST /investment/fd/tds-register/update
// Edits a CAPTURED TDS entry and writes an EDIT audit row.

func UpdateTDSRegister(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID            string  `json:"user_id"`
			TDSID             string  `json:"tds_id"`
			PeriodStart       string  `json:"period_start"`
			PeriodEnd         string  `json:"period_end"`
			GrossInterest     float64 `json:"gross_interest"`
			TDSExpected       float64 `json:"tds_expected"`
			TDSDeductedActual float64 `json:"tds_deducted_actual"`
			TDSDeductionDate  string  `json:"tds_deduction_date"`
			TDSRateApplied    float64 `json:"tds_rate_applied"`
			TDSSection        string  `json:"tds_section"`
			HasPAN            bool    `json:"has_pan"`
			Reason            string  `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if strings.TrimSpace(req.TDSID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrTDSIDRequired)
			return
		}

		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		var currentStatus, fdIDForTDS string
		if err := pool.QueryRow(ctx,
			`SELECT tds_status, fd_id FROM investment.fd_tds_receipt WHERE tds_id = $1 AND is_deleted = false`,
			req.TDSID).Scan(&currentStatus, &fdIDForTDS); err != nil {
			api.RespondWithError(w, http.StatusNotFound, "TDS entry not found")
			return
		}
		if currentStatus == constants.StatusApproved || currentStatus == "POSTED" {
			api.RespondWithError(w, http.StatusBadRequest, "approved/posted entries cannot be edited")
			return
		}

		deductionDate := req.TDSDeductionDate
		if strings.TrimSpace(deductionDate) == "" {
			deductionDate = req.PeriodEnd
		}
		if fdIDForTDS != "" {
			var fdStartU, fdMaturityU time.Time
			if scanErr := pool.QueryRow(ctx,
				`SELECT start_date, maturity_date FROM investment.fd_master WHERE fd_id=$1 AND is_deleted=false`,
				fdIDForTDS).Scan(&fdStartU, &fdMaturityU); scanErr != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "FD date lookup failed: "+scanErr.Error())
				return
			}
			if errMsg := checkFDPeriodDates(fdStartU, fdMaturityU, req.PeriodStart, req.PeriodEnd); errMsg != "" {
				api.RespondWithError(w, http.StatusBadRequest, errMsg)
				return
			}
			if errMsg := checkFDDates(fdStartU, fdMaturityU,
				[]string{"tds_deduction_date"},
				[]string{deductionDate}); errMsg != "" {
				api.RespondWithError(w, http.StatusBadRequest, errMsg)
				return
			}
		}

		tdsVariance := req.TDSDeductedActual - req.TDSExpected

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "tx start failed")
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		if _, err = tx.Exec(ctx, `
			UPDATE investment.fd_tds_receipt SET
				period_start       = $1::date,
				period_end         = $2::date,
				deduction_date     = $3::date,
				gross_interest     = $4,
				tds_expected       = $5,
				tds_deducted_actual = $6,
				tds_variance       = $7,
				tds_rate_applied   = $8,
				tds_rate_expected  = $8,
				tds_section        = $9,
				has_pan            = $10,
				exception_raised   = ($7 != 0)
			WHERE tds_id = $11`,
			req.PeriodStart, req.PeriodEnd, deductionDate,
			req.GrossInterest, req.TDSExpected, req.TDSDeductedActual, tdsVariance,
			req.TDSRateApplied, nullIfEmpty(req.TDSSection), req.HasPAN, req.TDSID,
		); err != nil {
			api.LogError("[TDSUpdate] update failed for %s: %v", req.TDSID, err)
			api.RespondWithError(w, http.StatusInternalServerError, "update failed")
			return
		}

		tx.Exec(ctx, `
			INSERT INTO investment.fd_tds_receipt_audit
				(tds_id, action_type, processing_status, requested_by, requested_at, checker_comment)
			VALUES ($1, 'EDIT', 'PENDING_APPROVAL', $2, now(), $3)`,
			req.TDSID, userEmail, nullIfEmpty(req.Reason)) //nolint:errcheck

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxCommitFailed)
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
			"success": true,
			"message": "TDS entry updated",
			"tds_id":  req.TDSID,
		})
	}
}

// ─── TDS Register Bulk Approve ─────────────────────────────────────────────
// POST /investment/fd/tds-register/approve-bulk
// Approves multiple TDS entries at once.

func BulkApproveTDSRegister(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			TDSIDs  []string `json:"tds_ids"`
			Comment string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.TDSIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrTDSIDRequired)
			return
		}

		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		type result struct {
			TDSID   string `json:"tds_id"`
			OK      bool   `json:"ok"`
			Message string `json:"message,omitempty"`
		}
		results := make([]result, 0, len(req.TDSIDs))
		approved := 0

		for _, tdsID := range req.TDSIDs {
			tx, err := pool.Begin(ctx)
			if err != nil {
				results = append(results, result{TDSID: tdsID, OK: false, Message: "tx start failed"})
				continue
			}
			tag, err := tx.Exec(ctx,
				`UPDATE investment.fd_tds_receipt SET tds_status = 'APPROVED'
				 WHERE tds_id = $1 AND tds_status = 'CAPTURED' AND is_deleted = false`,
				tdsID)
			if err != nil || tag.RowsAffected() == 0 {
				tx.Rollback(ctx) //nolint:errcheck
				results = append(results, result{TDSID: tdsID, OK: false, Message: "not CAPTURED or not found"})
				continue
			}
			tx.Exec(ctx, `
				UPDATE investment.fd_tds_receipt_audit
				SET processing_status = 'APPROVED', checker_by = $1,
				    checker_at = now(), checker_comment = $2
				WHERE tds_id = $3 AND processing_status = 'PENDING_APPROVAL'`,
				userEmail, nullIfEmpty(req.Comment), tdsID) //nolint:errcheck
			if err = tx.Commit(ctx); err != nil {
				results = append(results, result{TDSID: tdsID, OK: false, Message: "commit failed"})
				continue
			}
			approved++
			results = append(results, result{TDSID: tdsID, OK: true})
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
			"success":  approved > 0,
			"approved": approved,
			"failed":   len(req.TDSIDs) - approved,
			"results":  results,
		})
	}
}

// ─── TDS Register Bulk Reject ──────────────────────────────────────────────
// POST /investment/fd/tds-register/reject-bulk

func BulkRejectTDSRegister(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			TDSIDs  []string `json:"tds_ids"`
			Comment string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.TDSIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrTDSIDRequired)
			return
		}

		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		rejected := 0
		for _, tdsID := range req.TDSIDs {
			tx, err := pool.Begin(ctx)
			if err != nil {
				continue
			}
			tag, err := tx.Exec(ctx,
				`UPDATE investment.fd_tds_receipt SET tds_status = 'REJECTED'
				 WHERE tds_id = $1 AND is_deleted = false`, tdsID)
			if err != nil || tag.RowsAffected() == 0 {
				tx.Rollback(ctx) //nolint:errcheck
				continue
			}
			tx.Exec(ctx, `
				UPDATE investment.fd_tds_receipt_audit
				SET processing_status = 'REJECTED', checker_by = $1,
				    checker_at = now(), checker_comment = $2
				WHERE tds_id = $3 AND processing_status = 'PENDING_APPROVAL'`,
				userEmail, nullIfEmpty(req.Comment), tdsID) //nolint:errcheck
			if err = tx.Commit(ctx); err != nil {
				continue
			}
			rejected++
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
			"success":  rejected > 0,
			"rejected": rejected,
		})
	}
}

// ─── TDS Register Reject ──────────────────────────────────────────────────
// POST /investment/fd/tds-register/reject

func RejectTDSRegister(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string `json:"user_id"`
			TDSID   string `json:"tds_id"`
			Comment string `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if strings.TrimSpace(req.TDSID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrTDSIDRequired)
			return
		}

		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxStartFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		if _, err = tx.Exec(ctx,
			`UPDATE investment.fd_tds_receipt SET tds_status = 'REJECTED' WHERE tds_id = $1 AND is_deleted = false`,
			req.TDSID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to reject TDS entry")
			return
		}

		tx.Exec(ctx, `
			UPDATE investment.fd_tds_receipt_audit
			SET processing_status = 'REJECTED',
			    checker_by        = $1,
			    checker_at        = now(),
			    checker_comment   = $2
			WHERE tds_id = $3
			  AND processing_status = 'PENDING_APPROVAL'`,
			userEmail, nullIfEmpty(req.Comment), req.TDSID) //nolint:errcheck

		if err = tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxCommitFailed)
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
			"success": true,
			"message": "TDS entry rejected",
			"tds_id":  req.TDSID,
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

		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

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

// ─── TDS Register Bulk Soft-Delete ────────────────────────────────────────────
// POST /investment/fd/tds-register/delete-bulk
// Soft-deletes TDS entries (sets is_deleted=true).
// Only CAPTURED or REJECTED entries from ingestion_source=TDS_WORKBENCH can be deleted.

func BulkDeleteTDSRegister(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TDSIDs []string `json:"tds_ids"`
			Reason string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.TDSIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrTDSIDRequired)
			return
		}

		userEmail := resolveUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		deleted := 0
		failed := 0

		for _, tdsID := range req.TDSIDs {
			tx, err := pool.Begin(ctx)
			if err != nil {
				failed++
				continue
			}
			tag, err := tx.Exec(ctx,
				`UPDATE investment.fd_tds_receipt
				 SET is_deleted = true, deleted_by = $1, deleted_at = now()
				 WHERE tds_id = $2
				   AND tds_status IN ('CAPTURED', 'REJECTED')
				   AND ingestion_source = 'TDS_WORKBENCH'
				   AND is_deleted = false`,
				userEmail, tdsID)
			if err != nil || tag.RowsAffected() == 0 {
				tx.Rollback(ctx) //nolint:errcheck
				failed++
				continue
			}
			// Record deletion in audit
			tx.Exec(ctx, `
				UPDATE investment.fd_tds_receipt_audit
				SET deleted_by = $1, deleted_at = now()
				WHERE tds_id = $2`, userEmail, tdsID) //nolint:errcheck
			if err = tx.Commit(ctx); err != nil {
				failed++
				continue
			}
			deleted++
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
			"success": deleted > 0,
			"deleted": deleted,
			"failed":  failed,
		})
	}
}
