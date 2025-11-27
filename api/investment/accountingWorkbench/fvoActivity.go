package accountingworkbench

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------
// Request/Response Types for Fair Value Override
// ---------------------------

type CreateFVORequest struct {
	UserID              string  `json:"user_id"`
	ActivityType        string  `json:"activity_type"` // "FVO"
	EffectiveDate       string  `json:"effective_date"`
	AccountingPeriod    string  `json:"accounting_period,omitempty"`
	DataSource          string  `json:"data_source"`
	SchemeID            string  `json:"scheme_id"`
	ValuationDate       string  `json:"valuation_date"`
	MarketNAV           float64 `json:"market_nav,omitempty"`
	OverrideNAV         float64 `json:"override_nav"`
	Variance            float64 `json:"variance,omitempty"`
	VariancePct         float64 `json:"variance_pct,omitempty"`
	UnitsAffected       float64 `json:"units_affected,omitempty"`
	ValuationAdjustment float64 `json:"valuation_adjustment,omitempty"`
	Justification       string  `json:"justification"`
	EvidenceFileID      string  `json:"evidence_file_id,omitempty"`
}

type UpdateFVORequest struct {
	UserID string                 `json:"user_id"`
	FVOID  string                 `json:"fvo_id"`
	Fields map[string]interface{} `json:"fields"`
	Reason string                 `json:"reason"`
}

// ---------------------------
// CreateFVOSingle
// ---------------------------

func CreateFVOSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateFVORequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		// Validate required fields
		if strings.TrimSpace(req.SchemeID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "scheme_id is required")
			return
		}
		if strings.TrimSpace(req.ValuationDate) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "valuation_date is required")
			return
		}
		if req.OverrideNAV <= 0 {
			api.RespondWithError(w, http.StatusBadRequest, "override_nav must be greater than 0")
			return
		}
		if strings.TrimSpace(req.Justification) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "justification is required")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Name
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "TX begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Create main activity
		activityType := "FVO"
		if strings.TrimSpace(req.ActivityType) != "" {
			activityType = req.ActivityType
		}

		dataSource := "Manual"
		if strings.TrimSpace(req.DataSource) != "" {
			dataSource = req.DataSource
		}

		var activityID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO investment.accounting_activity (
				activity_type, effective_date, accounting_period, data_source, status
			) VALUES ($1, $2, $3, $4, 'DRAFT')
			RETURNING activity_id
		`, activityType, req.EffectiveDate, nullIfEmptyString(req.AccountingPeriod), dataSource).Scan(&activityID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Activity insert failed: "+err.Error())
			return
		}

		// Insert FVO record
		var fvoID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO investment.accounting_fvo (
				activity_id, scheme_id, valuation_date, market_nav, override_nav,
				variance, variance_pct, units_affected, valuation_adjustment,
				justification, evidence_file_id
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			RETURNING fvo_id
		`, activityID, req.SchemeID, req.ValuationDate, nullIfZeroFloat(req.MarketNAV),
			req.OverrideNAV, nullIfZeroFloat(req.Variance), nullIfZeroFloat(req.VariancePct),
			nullIfZeroFloat(req.UnitsAffected), nullIfZeroFloat(req.ValuationAdjustment),
			req.Justification, nullIfEmptyString(req.EvidenceFileID)).Scan(&fvoID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "FVO insert failed: "+err.Error())
			return
		}

		// Create audit trail
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionaccountingactivity (activity_id, actiontype, processing_status, requested_by, requested_at)
			VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now())
		`, activityID, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"activity_id": activityID,
			"fvo_id":      fvoID,
			"requested":   userEmail,
		})
	}
}

// ---------------------------
// CreateFVOBulk
// ---------------------------

func CreateFVOBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID           string `json:"user_id"`
			ActivityType     string `json:"activity_type"`
			EffectiveDate    string `json:"effective_date"`
			AccountingPeriod string `json:"accounting_period,omitempty"`
			DataSource       string `json:"data_source"`
			Rows             []struct {
				SchemeID            string  `json:"scheme_id"`
				ValuationDate       string  `json:"valuation_date"`
				MarketNAV           float64 `json:"market_nav,omitempty"`
				OverrideNAV         float64 `json:"override_nav"`
				Variance            float64 `json:"variance,omitempty"`
				VariancePct         float64 `json:"variance_pct,omitempty"`
				UnitsAffected       float64 `json:"units_affected,omitempty"`
				ValuationAdjustment float64 `json:"valuation_adjustment,omitempty"`
				Justification       string  `json:"justification"`
				EvidenceFileID      string  `json:"evidence_file_id,omitempty"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON body")
			return
		}

		if len(req.Rows) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No rows provided")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Name
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid user session")
			return
		}

		ctx := r.Context()

		// Single transaction for entire batch
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "TX begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Create main activity
		activityType := "FVO"
		if strings.TrimSpace(req.ActivityType) != "" {
			activityType = req.ActivityType
		}

		dataSource := "Manual"
		if strings.TrimSpace(req.DataSource) != "" {
			dataSource = req.DataSource
		}

		var activityID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO investment.accounting_activity (
				activity_type, effective_date, accounting_period, data_source, status
			) VALUES ($1, $2, $3, $4, 'DRAFT')
			RETURNING activity_id
		`, activityType, req.EffectiveDate, nullIfEmptyString(req.AccountingPeriod), dataSource).Scan(&activityID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Activity insert failed: "+err.Error())
			return
		}

		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			// Validate
			if strings.TrimSpace(row.SchemeID) == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "scheme_id is required"})
				continue
			}
			if strings.TrimSpace(row.ValuationDate) == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "valuation_date is required"})
				continue
			}
			if row.OverrideNAV <= 0 {
				results = append(results, map[string]interface{}{"success": false, "error": "override_nav must be > 0"})
				continue
			}
			if strings.TrimSpace(row.Justification) == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "justification is required"})
				continue
			}

			var fvoID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO investment.accounting_fvo (
					activity_id, scheme_id, valuation_date, market_nav, override_nav,
					variance, variance_pct, units_affected, valuation_adjustment,
					justification, evidence_file_id
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
				RETURNING fvo_id
			`, activityID, row.SchemeID, row.ValuationDate, nullIfZeroFloat(row.MarketNAV),
				row.OverrideNAV, nullIfZeroFloat(row.Variance), nullIfZeroFloat(row.VariancePct),
				nullIfZeroFloat(row.UnitsAffected), nullIfZeroFloat(row.ValuationAdjustment),
				row.Justification, nullIfEmptyString(row.EvidenceFileID)).Scan(&fvoID); err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "FVO insert failed: " + err.Error()})
				continue
			}

			results = append(results, map[string]interface{}{
				"success":   true,
				"fvo_id":    fvoID,
				"scheme_id": row.SchemeID,
			})
		}

		// Create single audit trail for batch
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionaccountingactivity (activity_id, actiontype, processing_status, requested_by, requested_at)
			VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now())
		`, activityID, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", map[string]any{
			"activity_id": activityID,
			"fvo_records": results,
		})
	}
}

// ---------------------------
// UpdateFVO
// ---------------------------

func UpdateFVO(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateFVORequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		if strings.TrimSpace(req.FVOID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "fvo_id required")
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "no fields to update")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Name
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "invalid session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "tx begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Fetch existing values
		sel := `
			SELECT activity_id, scheme_id, valuation_date, market_nav, override_nav,
			       variance, variance_pct, units_affected, valuation_adjustment, justification, evidence_file_id
			FROM investment.accounting_fvo
			WHERE fvo_id=$1
			FOR UPDATE
		`
		var activityID string
		var oldVals [10]interface{}
		if err := tx.QueryRow(ctx, sel, req.FVOID).Scan(
			&activityID, &oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
			&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7], &oldVals[8], &oldVals[9],
		); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "fetch failed: "+err.Error())
			return
		}

		fieldPairs := map[string]int{
			"scheme_id":            0,
			"valuation_date":       1,
			"market_nav":           2,
			"override_nav":         3,
			"variance":             4,
			"variance_pct":         5,
			"units_affected":       6,
			"valuation_adjustment": 7,
			"justification":        8,
			"evidence_file_id":     9,
		}

		var sets []string
		var args []interface{}
		pos := 1

		for k, v := range req.Fields {
			lk := strings.ToLower(k)
			if idx, ok := fieldPairs[lk]; ok {
				oldField := "old_" + lk
				sets = append(sets, fmt.Sprintf("%s=$%d, %s=$%d", lk, pos, oldField, pos+1))
				args = append(args, v, oldVals[idx])
				pos += 2
			}
		}

		if len(sets) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "no valid updatable fields found")
			return
		}

		q := fmt.Sprintf("UPDATE investment.accounting_fvo SET %s, updated_at=now() WHERE fvo_id=$%d", strings.Join(sets, ", "), pos)
		args = append(args, req.FVOID)
		if _, err := tx.Exec(ctx, q, args...); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "update failed: "+err.Error())
			return
		}

		// Audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionaccountingactivity (activity_id, actiontype, processing_status, reason, requested_by, requested_at)
			VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, now())
		`, activityID, req.Reason, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"fvo_id":      req.FVOID,
			"activity_id": activityID,
			"requested":   userEmail,
		})
	}
}

// ---------------------------
// GetFVOsWithAudit
// ---------------------------

func GetFVOsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.activity_id)
					a.activity_id,
					a.actiontype,
					a.processing_status,
					a.action_id,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason
				FROM investment.auditactionaccountingactivity a
				ORDER BY a.activity_id, a.requested_at DESC
			)
			SELECT
				fvo.fvo_id,
				fvo.activity_id,
				act.activity_type,
				TO_CHAR(act.effective_date, 'YYYY-MM-DD') AS effective_date,
				TO_CHAR(act.accounting_period, 'YYYY-MM-DD') AS accounting_period,
				act.data_source,
				act.status,
				fvo.scheme_id,
				COALESCE(fvo.old_scheme_id,'') AS old_scheme_id,
				TO_CHAR(fvo.valuation_date, 'YYYY-MM-DD') AS valuation_date,
				TO_CHAR(fvo.old_valuation_date, 'YYYY-MM-DD') AS old_valuation_date,
				COALESCE(fvo.market_nav,0) AS market_nav,
				COALESCE(fvo.old_market_nav,0) AS old_market_nav,
				fvo.override_nav,
				COALESCE(fvo.old_override_nav,0) AS old_override_nav,
				COALESCE(fvo.variance,0) AS variance,
				COALESCE(fvo.old_variance,0) AS old_variance,
				COALESCE(fvo.variance_pct,0) AS variance_pct,
				COALESCE(fvo.old_variance_pct,0) AS old_variance_pct,
				COALESCE(fvo.units_affected,0) AS units_affected,
				COALESCE(fvo.old_units_affected,0) AS old_units_affected,
				COALESCE(fvo.valuation_adjustment,0) AS valuation_adjustment,
				COALESCE(fvo.old_valuation_adjustment,0) AS old_valuation_adjustment,
				fvo.justification,
				COALESCE(fvo.old_justification,'') AS old_justification,
				COALESCE(fvo.evidence_file_id,'') AS evidence_file_id,
				COALESCE(fvo.old_evidence_file_id,'') AS old_evidence_file_id,
				fvo.is_deleted,
				TO_CHAR(fvo.updated_at, 'YYYY-MM-DD HH24:MI:SS') AS updated_at,
				
				COALESCE(l.actiontype,'') AS action_type,
				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.action_id::text,'') AS action_id,
				COALESCE(l.requested_by,'') AS audit_requested_by,
				TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				COALESCE(l.checker_comment,'') AS checker_comment,
				COALESCE(l.reason,'') AS reason
			FROM investment.accounting_fvo fvo
			JOIN investment.accounting_activity act ON act.activity_id = fvo.activity_id
			LEFT JOIN latest_audit l ON l.activity_id = fvo.activity_id
			WHERE COALESCE(fvo.is_deleted, false) = false
				AND COALESCE(act.is_deleted, false) = false
			ORDER BY fvo.updated_at DESC, fvo.fvo_id;
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 1000)
		for rows.Next() {
			vals, _ := rows.Values()
			rec := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				rec[string(f.Name)] = vals[i]
			}
			out = append(out, rec)
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "rows scan failed: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}
