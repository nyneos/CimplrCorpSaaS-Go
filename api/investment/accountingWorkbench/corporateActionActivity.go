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
// Request/Response Types for Corporate Action
// ---------------------------

type CreateCorporateActionRequest struct {
	UserID           string  `json:"user_id"`
	ActivityType     string  `json:"activity_type"` // "CORPORATE_ACTION"
	EffectiveDate    string  `json:"effective_date"`
	AccountingPeriod string  `json:"accounting_period,omitempty"`
	DataSource       string  `json:"data_source"`
	ActionType       string  `json:"action_type"` // SCHEME_NAME_CHANGE, SCHEME_MERGER, SPLIT, BONUS
	SourceSchemeID   string  `json:"source_scheme_id"`
	TargetSchemeID   string  `json:"target_scheme_id,omitempty"`
	NewSchemeName    string  `json:"new_scheme_name,omitempty"`
	RatioNew         float64 `json:"ratio_new,omitempty"`
	RatioOld         float64 `json:"ratio_old,omitempty"`
	ConversionRatio  float64 `json:"conversion_ratio,omitempty"`
	BonusUnits       float64 `json:"bonus_units,omitempty"`
}

type UpdateCorporateActionRequest struct {
	UserID            string                 `json:"user_id"`
	CorporateActionID string                 `json:"corporate_action_id"`
	Fields            map[string]interface{} `json:"fields"`
	Reason            string                 `json:"reason"`
}

// ---------------------------
// CreateCorporateActionSingle
// ---------------------------

func CreateCorporateActionSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateCorporateActionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		// Validate required fields
		if strings.TrimSpace(req.ActionType) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "action_type is required")
			return
		}
		if strings.TrimSpace(req.SourceSchemeID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "source_scheme_id is required")
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
		activityType := "CORPORATE_ACTION"
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

		// Insert Corporate Action record
		var corpActionID string
		if err := tx.QueryRow(ctx, `
			INSERT INTO investment.accounting_corporate_action (
				activity_id, action_type, source_scheme_id, target_scheme_id, new_scheme_name,
				ratio_new, ratio_old, conversion_ratio, bonus_units
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			RETURNING corporate_action_id
		`, activityID, req.ActionType, req.SourceSchemeID, nullIfEmptyString(req.TargetSchemeID),
			nullIfEmptyString(req.NewSchemeName), nullIfZeroFloat(req.RatioNew),
			nullIfZeroFloat(req.RatioOld), nullIfZeroFloat(req.ConversionRatio),
			nullIfZeroFloat(req.BonusUnits)).Scan(&corpActionID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Corporate action insert failed: "+err.Error())
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
			"activity_id":         activityID,
			"corporate_action_id": corpActionID,
			"requested":           userEmail,
		})
	}
}

// ---------------------------
// CreateCorporateActionBulk
// ---------------------------

func CreateCorporateActionBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID           string `json:"user_id"`
			ActivityType     string `json:"activity_type"`
			EffectiveDate    string `json:"effective_date"`
			AccountingPeriod string `json:"accounting_period,omitempty"`
			DataSource       string `json:"data_source"`
			Rows             []struct {
				ActionType      string  `json:"action_type"`
				SourceSchemeID  string  `json:"source_scheme_id"`
				TargetSchemeID  string  `json:"target_scheme_id,omitempty"`
				NewSchemeName   string  `json:"new_scheme_name,omitempty"`
				RatioNew        float64 `json:"ratio_new,omitempty"`
				RatioOld        float64 `json:"ratio_old,omitempty"`
				ConversionRatio float64 `json:"conversion_ratio,omitempty"`
				BonusUnits      float64 `json:"bonus_units,omitempty"`
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
		activityType := "CORPORATE_ACTION"
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
			if strings.TrimSpace(row.ActionType) == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "action_type is required"})
				continue
			}
			if strings.TrimSpace(row.SourceSchemeID) == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "source_scheme_id is required"})
				continue
			}

			var corpActionID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO investment.accounting_corporate_action (
					activity_id, action_type, source_scheme_id, target_scheme_id, new_scheme_name,
					ratio_new, ratio_old, conversion_ratio, bonus_units
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
				RETURNING corporate_action_id
			`, activityID, row.ActionType, row.SourceSchemeID, nullIfEmptyString(row.TargetSchemeID),
				nullIfEmptyString(row.NewSchemeName), nullIfZeroFloat(row.RatioNew),
				nullIfZeroFloat(row.RatioOld), nullIfZeroFloat(row.ConversionRatio),
				nullIfZeroFloat(row.BonusUnits)).Scan(&corpActionID); err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "Corporate action insert failed: " + err.Error()})
				continue
			}

			results = append(results, map[string]interface{}{
				"success":             true,
				"corporate_action_id": corpActionID,
				"source_scheme_id":    row.SourceSchemeID,
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
			"activity_id":              activityID,
			"corporate_action_records": results,
		})
	}
}

// ---------------------------
// UpdateCorporateAction
// ---------------------------

func UpdateCorporateAction(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateCorporateActionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		if strings.TrimSpace(req.CorporateActionID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "corporate_action_id required")
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
			SELECT activity_id, action_type, source_scheme_id, target_scheme_id, new_scheme_name,
			       ratio_new, ratio_old, conversion_ratio, bonus_units
			FROM investment.accounting_corporate_action
			WHERE corporate_action_id=$1
			FOR UPDATE
		`
		var activityID string
		var oldVals [8]interface{}
		if err := tx.QueryRow(ctx, sel, req.CorporateActionID).Scan(
			&activityID, &oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
			&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7],
		); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "fetch failed: "+err.Error())
			return
		}

		fieldPairs := map[string]int{
			"action_type":      0,
			"source_scheme_id": 1,
			"target_scheme_id": 2,
			"new_scheme_name":  3,
			"ratio_new":        4,
			"ratio_old":        5,
			"conversion_ratio": 6,
			"bonus_units":      7,
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

		q := fmt.Sprintf("UPDATE investment.accounting_corporate_action SET %s, updated_at=now() WHERE corporate_action_id=$%d", strings.Join(sets, ", "), pos)
		args = append(args, req.CorporateActionID)
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
			"corporate_action_id": req.CorporateActionID,
			"activity_id":         activityID,
			"requested":           userEmail,
		})
	}
}

// ---------------------------
// GetCorporateActionsWithAudit
// ---------------------------

func GetCorporateActionsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
				ca.corporate_action_id,
				ca.activity_id,
				act.activity_type,
				TO_CHAR(act.effective_date, 'YYYY-MM-DD') AS effective_date,
				TO_CHAR(act.accounting_period, 'YYYY-MM-DD') AS accounting_period,
				act.data_source,
				act.status,
				ca.action_type,
				COALESCE(ca.old_action_type,'') AS old_action_type,
				ca.source_scheme_id,
				COALESCE(ca.old_source_scheme_id,'') AS old_source_scheme_id,
				COALESCE(ca.target_scheme_id,'') AS target_scheme_id,
				COALESCE(ca.old_target_scheme_id,'') AS old_target_scheme_id,
				COALESCE(ca.new_scheme_name,'') AS new_scheme_name,
				COALESCE(ca.old_new_scheme_name,'') AS old_new_scheme_name,
				COALESCE(ca.ratio_new,0) AS ratio_new,
				COALESCE(ca.old_ratio_new,0) AS old_ratio_new,
				COALESCE(ca.ratio_old,0) AS ratio_old,
				COALESCE(ca.old_ratio_old,0) AS old_ratio_old,
				COALESCE(ca.conversion_ratio,0) AS conversion_ratio,
				COALESCE(ca.old_conversion_ratio,0) AS old_conversion_ratio,
				COALESCE(ca.bonus_units,0) AS bonus_units,
				COALESCE(ca.old_bonus_units,0) AS old_bonus_units,
				ca.is_deleted,
				TO_CHAR(ca.updated_at, 'YYYY-MM-DD HH24:MI:SS') AS updated_at,
				
				COALESCE(l.actiontype,'') AS action_type_audit,
				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.action_id::text,'') AS action_id,
				COALESCE(l.requested_by,'') AS audit_requested_by,
				TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				COALESCE(l.checker_comment,'') AS checker_comment,
				COALESCE(l.reason,'') AS reason
			FROM investment.accounting_corporate_action ca
			JOIN investment.accounting_activity act ON act.activity_id = ca.activity_id
			LEFT JOIN latest_audit l ON l.activity_id = ca.activity_id
			WHERE COALESCE(ca.is_deleted, false) = false
				AND COALESCE(act.is_deleted, false) = false
			ORDER BY ca.updated_at DESC, ca.corporate_action_id;
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
