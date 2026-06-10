package fdInterestAndTdsWorkbench

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/validation"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func resolveUserEmail(ctx context.Context) string {
	if s := api.GetSessionFromCtx(ctx); s != nil {
		return s.Email
	}
	return ""
}

func rowsToMapSlice(rows pgx.Rows) ([]map[string]interface{}, error) {
	fields := rows.FieldDescriptions()
	var out []map[string]interface{}
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(fields))
		for i, f := range fields {
			if vals[i] == nil {
				row[string(f.Name)] = ""
			} else {
				row[string(f.Name)] = vals[i]
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func fdAllowedEntityIDs(ctx context.Context) []string {
	return api.GetEntityIDsFromCtx(ctx)
}

func fdEntityAllowed(ctx context.Context, entityID string) bool {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return false
	}
	for _, allowedID := range fdAllowedEntityIDs(ctx) {
		if strings.EqualFold(strings.TrimSpace(allowedID), entityID) {
			return true
		}
	}
	return false
}

func appendFDEntityScope(ctx context.Context, query *string, args *[]interface{}, idx *int, column, requestedEntityID string) string {
	requestedEntityID = strings.TrimSpace(requestedEntityID)
	if requestedEntityID != "" {
		if !fdEntityAllowed(ctx, requestedEntityID) {
			return fmt.Sprintf(constants.ErrEntityIDNotAuthorized, requestedEntityID)
		}
		*query += fmt.Sprintf(" AND %s=$%d", column, *idx)
		*args = append(*args, requestedEntityID)
		*idx += 1
		return ""
	}
	allowedIDs := fdAllowedEntityIDs(ctx)
	if len(allowedIDs) == 0 {
		return constants.ErrNoAccessibleBusinessUnit
	}
	*query += fmt.Sprintf(" AND %s=ANY($%d::text[])", column, *idx)
	*args = append(*args, allowedIDs)
	*idx += 1
	return ""
}

func validateFDRecordAccess(ctx context.Context, pool *pgxpool.Pool, fdID string) string {
	fdID = strings.TrimSpace(fdID)
	if fdID == "" {
		return ""
	}
	var entityID, bankID, bankName string
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(entity_id, ''), COALESCE(bank_id, ''), COALESCE(bank_name, '')
		FROM investment.fd_master
		WHERE fd_id = $1 AND COALESCE(is_deleted, false) = false
		LIMIT 1
	`, fdID).Scan(&entityID, &bankID, &bankName); err != nil {
		return constants.ErrFDNotFound
	}
	if !fdEntityAllowed(ctx, entityID) {
		return fmt.Sprintf(constants.ErrEntityIDNotAuthorized, entityID)
	}
	return validation.ValidateFDMasterReferences(ctx, map[string]interface{}{
		"entity_id": entityID,
		"bank_id":   bankID,
		"bank_name": bankName,
	})
}

func validateTDSRecordAccess(ctx context.Context, pool *pgxpool.Pool, tdsID string) string {
	tdsID = strings.TrimSpace(tdsID)
	if tdsID == "" {
		return ""
	}
	var entityID, fdID, bankID, bankName string
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(t.entity_id, ''), COALESCE(t.fd_id, ''), COALESCE(fd.bank_id, ''), COALESCE(fd.bank_name, '')
		FROM investment.fd_tds_receipt t
		LEFT JOIN investment.fd_master fd ON fd.fd_id = t.fd_id
		WHERE t.tds_id = $1 AND COALESCE(t.is_deleted, false) = false
		LIMIT 1
	`, tdsID).Scan(&entityID, &fdID, &bankID, &bankName); err != nil {
		return "TDS receipt not found"
	}
	if !fdEntityAllowed(ctx, entityID) {
		return fmt.Sprintf(constants.ErrEntityIDNotAuthorized, entityID)
	}
	return validation.ValidateFDMasterReferences(ctx, map[string]interface{}{
		"entity_id": entityID,
		"bank_id":   bankID,
		"bank_name": bankName,
		"fd_id":     fdID,
	})
}

func validateFDInterestReceiptAccess(ctx context.Context, pool *pgxpool.Pool, receiptID string) string {
	receiptID = strings.TrimSpace(receiptID)
	if receiptID == "" {
		return ""
	}
	var entityID, fdID, bankID, bankName string
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(r.entity_id, ''), COALESCE(r.fd_id, ''), COALESCE(r.bank_id, ''), COALESCE(r.bank_name, '')
		FROM investment.fd_interest_receipt r
		WHERE r.receipt_id = $1 AND COALESCE(r.is_deleted, false) = false
		LIMIT 1
	`, receiptID).Scan(&entityID, &fdID, &bankID, &bankName); err != nil {
		return "FD interest receipt not found"
	}
	if !fdEntityAllowed(ctx, entityID) {
		return fmt.Sprintf(constants.ErrEntityIDNotAuthorized, entityID)
	}
	return validation.ValidateFDMasterReferences(ctx, map[string]interface{}{
		"entity_id": entityID,
		"bank_id":   bankID,
		"bank_name": bankName,
		"fd_id":     fdID,
	})
}

// GetInterestWorkbenchSummary returns consolidated interest receipt status counts and totals.
func GetInterestWorkbenchSummary(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			EntityID string `json:"entity_id"`
			FromDate string `json:"from_date"`
			ToDate   string `json:"to_date"`
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

		statusSQL := `
			SELECT receipt_status,
			       COUNT(*) AS count,
			       COALESCE(SUM(gross_interest_received),0) AS total_gross,
			       COALESCE(SUM(tds_amount_deducted),0)     AS total_tds,
			       COALESCE(SUM(net_amount_received),0)     AS total_net
			FROM investment.fd_interest_receipt WHERE is_deleted=false`
		args := []interface{}{}
		idx := 1
		if msg := appendFDEntityScope(ctx, &statusSQL, &args, &idx, "entity_id", req.EntityID); msg != "" {
			api.RespondWithError(w, http.StatusForbidden, msg)
			return
		}
		if req.FromDate != "" {
			statusSQL += fmt.Sprintf(constants.ErrReceiptDateFilter, idx)
			args = append(args, req.FromDate)
			idx++
		}
		if req.ToDate != "" {
			statusSQL += fmt.Sprintf(constants.ErrReceiptDateFilterEnd, idx)
			args = append(args, req.ToDate)
			idx++
		}
		statusSQL += " GROUP BY receipt_status ORDER BY receipt_status"

		statusRows, err := pool.Query(ctx, statusSQL, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Status query failed: "+err.Error())
			return
		}
		defer statusRows.Close()
		statusData, _ := rowsToMapSlice(statusRows)

		recentSQL := `
			SELECT receipt_id, fd_id, fd_ref_no, entity_name, bank_name,
			       TO_CHAR(receipt_date,'YYYY-MM-DD') AS receipt_date,
			       gross_interest_received, tds_amount_deducted, net_amount_received,
			       receipt_status, reconcile_status
			FROM investment.fd_interest_receipt WHERE is_deleted=false`
		recentArgs := []interface{}{}
		ridx := 1
		if msg := appendFDEntityScope(ctx, &recentSQL, &recentArgs, &ridx, "entity_id", req.EntityID); msg != "" {
			api.RespondWithError(w, http.StatusForbidden, msg)
			return
		}
		if req.FromDate != "" {
			recentSQL += fmt.Sprintf(constants.ErrReceiptDateFilter, ridx)
			recentArgs = append(recentArgs, req.FromDate)
			ridx++
		}
		if req.ToDate != "" {
			recentSQL += fmt.Sprintf(constants.ErrReceiptDateFilterEnd, ridx)
			recentArgs = append(recentArgs, req.ToDate)
			ridx++
		}
		recentSQL += " ORDER BY created_at DESC LIMIT 20"

		recentRows, err := pool.Query(ctx, recentSQL, recentArgs...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Recent receipts query failed: "+err.Error())
			return
		}
		defer recentRows.Close()
		recentData, _ := rowsToMapSlice(recentRows)

		var totalGross, totalTDS, totalNet float64
		var totalCount int
		totalSQL := `SELECT COUNT(*), COALESCE(SUM(gross_interest_received),0),
			        COALESCE(SUM(tds_amount_deducted),0), COALESCE(SUM(net_amount_received),0)
			 FROM investment.fd_interest_receipt WHERE is_deleted=false`
		totalArgs := []interface{}{}
		tidx := 1
		if msg := appendFDEntityScope(ctx, &totalSQL, &totalArgs, &tidx, "entity_id", req.EntityID); msg != "" {
			api.RespondWithError(w, http.StatusForbidden, msg)
			return
		}
		if req.FromDate != "" {
			totalSQL += fmt.Sprintf(constants.ErrReceiptDateFilter, tidx)
			totalArgs = append(totalArgs, req.FromDate)
			tidx++
		}
		if req.ToDate != "" {
			totalSQL += fmt.Sprintf(constants.ErrReceiptDateFilterEnd, tidx)
			totalArgs = append(totalArgs, req.ToDate)
			tidx++
		}
		pool.QueryRow(ctx, totalSQL, totalArgs...).Scan(&totalCount, &totalGross, &totalTDS, &totalNet) //nolint:errcheck

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":          true,
			"summary":          map[string]interface{}{"total_count": totalCount, "total_gross": totalGross, "total_tds": totalTDS, "total_net": totalNet},
			"status_breakdown": statusData,
			"recent_receipts":  recentData,
		})
	}
}

// GetTDSWorkbenchSummary returns TDS register summary, variance analysis, and exception counts.
func GetTDSWorkbenchSummary(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			EntityID string `json:"entity_id"`
			FromDate string `json:"from_date"`
			ToDate   string `json:"to_date"`
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

		baseFilter := " WHERE is_deleted=false"
		args := []interface{}{}
		idx := 1
		baseSQL := baseFilter
		if msg := appendFDEntityScope(ctx, &baseSQL, &args, &idx, "entity_id", req.EntityID); msg != "" {
			api.RespondWithError(w, http.StatusForbidden, msg)
			return
		}
		baseFilter = baseSQL
		if req.FromDate != "" {
			baseFilter += fmt.Sprintf(" AND period_start>=$%d", idx)
			args = append(args, req.FromDate)
			idx++
		}
		if req.ToDate != "" {
			baseFilter += fmt.Sprintf(" AND period_end<=$%d", idx)
			args = append(args, req.ToDate)
			idx++
		}

		var totalExpected, totalActual, totalVariance float64
		var totalCount, exceptionCount int
		pool.QueryRow(ctx,
			`SELECT COUNT(*), COALESCE(SUM(tds_expected),0), COALESCE(SUM(tds_deducted_actual),0),
			        COALESCE(SUM(tds_variance),0), COUNT(*) FILTER (WHERE exception_raised=true)
			 FROM investment.fd_tds_receipt`+baseFilter, args...,
		).Scan(&totalCount, &totalExpected, &totalActual, &totalVariance, &exceptionCount) //nolint:errcheck

		entitySQL := `
			SELECT entity_id,
			       COUNT(*) AS tds_rows,
			       COALESCE(SUM(tds_expected),0)        AS expected,
			       COALESCE(SUM(tds_deducted_actual),0) AS actual,
			       COALESCE(SUM(tds_variance),0)        AS variance
			FROM investment.fd_tds_receipt` + baseFilter + " GROUP BY entity_id ORDER BY entity_id"

		entityRows, err := pool.Query(ctx, entitySQL, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Entity breakdown query failed: "+err.Error())
			return
		}
		defer entityRows.Close()
		entityData, _ := rowsToMapSlice(entityRows)

		exSQL := `
			SELECT e.exception_id, e.fd_id,
			       COALESCE(exception_type,'') AS exception_type,
			       COALESCE(severity,'') AS severity,
			       COALESCE(variance_amount,0) AS variance_amount,
			       COALESCE(exception_status,'') AS exception_status,
			       TO_CHAR(raised_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS raised_at
			FROM investment.fd_receipt_exception e
			JOIN investment.fd_master fd ON fd.fd_id = e.fd_id
			WHERE e.is_deleted=false`
		exArgs := []interface{}{}
		exIdx := 1
		if msg := appendFDEntityScope(ctx, &exSQL, &exArgs, &exIdx, "fd.entity_id", req.EntityID); msg != "" {
			api.RespondWithError(w, http.StatusForbidden, msg)
			return
		}
		exSQL += " ORDER BY raised_at DESC LIMIT 10"
		exRows, err := pool.Query(ctx, exSQL, exArgs...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Exception query failed: "+err.Error())
			return
		}
		defer exRows.Close()
		exData, _ := rowsToMapSlice(exRows)

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":           true,
			"summary":           map[string]interface{}{"total_count": totalCount, "total_expected": totalExpected, "total_actual": totalActual, "total_variance": totalVariance, "exception_count": exceptionCount},
			"entity_breakdown":  entityData,
			"recent_exceptions": exData,
		})
	}
}

// GetReconciliationDashboard returns reconciliation run history and exception pipeline.
func GetReconciliationDashboard(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			EntityID string `json:"entity_id"`
			Limit    int    `json:"limit"`
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
		if req.Limit <= 0 || req.Limit > 100 {
			req.Limit = 20
		}
		ctx := r.Context()

		runSQL := `
			SELECT reconcile_run_id, entity_id, entity_name,
			       TO_CHAR(period_start,'YYYY-MM-DD') AS period_start,
			       TO_CHAR(period_end,'YYYY-MM-DD')   AS period_end,
			       run_status, trigger_mode,
			       COALESCE(receipts_matched,0) AS matched_count,
			       COALESCE(receipts_unmatched,0) AS unmatched_count,
			       COALESCE(receipts_exception,0) AS exception_count,
			       COALESCE(receipts_processed,0) AS total_processed,
			       TO_CHAR(triggered_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS triggered_at,
			       TO_CHAR(completed_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS completed_at
			FROM investment.fd_receipt_reconcile_run`
		runArgs := []interface{}{}
		runSQL += " WHERE 1=1"
		runIdx := 1
		if msg := appendFDEntityScope(ctx, &runSQL, &runArgs, &runIdx, "entity_id", req.EntityID); msg != "" {
			api.RespondWithError(w, http.StatusForbidden, msg)
			return
		}
		runSQL += fmt.Sprintf(" ORDER BY triggered_at DESC LIMIT %d", req.Limit)

		runRows, err := pool.Query(ctx, runSQL, runArgs...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Run history query failed: "+err.Error())
			return
		}
		defer runRows.Close()
		runData, _ := rowsToMapSlice(runRows)

		pipelineSQL := `
			SELECT severity, exception_status, COUNT(*) AS count,
			       COALESCE(SUM(variance_amount),0) AS total_variance
			FROM investment.fd_receipt_exception e
			JOIN investment.fd_master fd ON fd.fd_id = e.fd_id
			WHERE e.is_deleted=false AND exception_status NOT IN ('CLOSED')`
		pipelineArgs := []interface{}{}
		pipelineIdx := 1
		if msg := appendFDEntityScope(ctx, &pipelineSQL, &pipelineArgs, &pipelineIdx, "fd.entity_id", req.EntityID); msg != "" {
			api.RespondWithError(w, http.StatusForbidden, msg)
			return
		}
		pipelineSQL += " GROUP BY severity, exception_status ORDER BY severity, exception_status"
		pipelineRows, err := pool.Query(ctx, pipelineSQL, pipelineArgs...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Pipeline query failed: "+err.Error())
			return
		}
		defer pipelineRows.Close()
		pipelineData, _ := rowsToMapSlice(pipelineRows)

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":            true,
			"recent_runs":        runData,
			"exception_pipeline": pipelineData,
		})
	}
}

// GetInterestVsAccrualAnalysis compares received interest against accrued interest for variance analysis.
func GetInterestVsAccrualAnalysis(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			EntityID string `json:"entity_id"`
			FdID     string `json:"fd_id"`
			FromDate string `json:"from_date"`
			ToDate   string `json:"to_date"`
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
		if msg := validateFDRecordAccess(ctx, pool, req.FdID); msg != "" {
			api.RespondWithError(w, http.StatusForbidden, msg)
			return
		}

		analysisSQL := `
			SELECT r.fd_id, r.fd_ref_no, r.entity_id, r.entity_name,
			       TO_CHAR(r.receipt_date,'YYYY-MM-DD')            AS receipt_date,
			       COALESCE(r.gross_interest_received,0)           AS received_amount,
			       COALESCE(r.tds_amount_deducted,0)               AS tds_deducted,
			       COALESCE(r.net_amount_received,0)               AS net_received,
			       COALESCE(al.total_accrued,0)                    AS total_accrued,
			       COALESCE(al.total_tds,0)                        AS total_tds_accrued,
			       COALESCE(al.total_net,0)                        AS total_net_accrued,
			       COALESCE(r.gross_interest_received,0) - COALESCE(al.total_accrued,0) AS gross_variance,
			       COALESCE(r.net_amount_received,0) - COALESCE(al.total_net,0)       AS net_variance,
			       COALESCE(r.receipt_status,'')                   AS receipt_status,
			       COALESCE(r.reconcile_status,'')                 AS reconcile_status
			FROM investment.fd_interest_receipt r
			LEFT JOIN (
			    SELECT fd_id,
			           SUM(period_interest_accrued) AS total_accrued,
			           SUM(tds_deducted_in_period)  AS total_tds,
			           SUM(net_interest_in_period)  AS total_net
			    FROM investment.fd_accrual_ledger
			    WHERE ledger_row_status='CALCULATED' AND COALESCE(is_deleted,false)=false
			    GROUP BY fd_id
			) al ON al.fd_id = r.fd_id
			WHERE COALESCE(r.is_deleted,false)=false`
		args := []interface{}{}
		idx := 1
		if msg := appendFDEntityScope(ctx, &analysisSQL, &args, &idx, constants.ErrEntityIDFilterAlt, req.EntityID); msg != "" {
			api.RespondWithError(w, http.StatusForbidden, msg)
			return
		}
		if req.FdID != "" {
			analysisSQL += fmt.Sprintf(" AND r.fd_id=$%d", idx)
			args = append(args, req.FdID)
			idx++
		}
		if req.FromDate != "" {
			analysisSQL += fmt.Sprintf(" AND r.receipt_date>=$%d", idx)
			args = append(args, req.FromDate)
			idx++
		}
		if req.ToDate != "" {
			analysisSQL += fmt.Sprintf(" AND r.receipt_date<=$%d", idx)
			args = append(args, req.ToDate)
			idx++
		}
		analysisSQL += " ORDER BY r.receipt_date DESC"

		rows, err := pool.Query(ctx, analysisSQL, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Analysis query failed: "+err.Error())
			return
		}
		defer rows.Close()
		out, _ := rowsToMapSlice(rows)

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"rows":    out,
			"count":   len(out),
		})
	}
}
