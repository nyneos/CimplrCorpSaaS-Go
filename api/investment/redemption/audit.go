package redemption

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func collectRedemptionAuditRows(rows pgx.Rows) ([]map[string]interface{}, error) {
	defer rows.Close()
	fields := rows.FieldDescriptions()
	out := make([]map[string]interface{}, 0)
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

// ─── GetRedemptionInitiationAuditHistory ─────────────────────────────────────
// POST /investment/redemption/initiation/audit-history
// Body: { user_id, redemption_id? }

func GetRedemptionInitiationAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			UserID       string `json:"user_id"`
			RedemptionID string `json:"redemption_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "user_id is required")
			return
		}
		if api.GetSessionFromCtx(r.Context()) == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		baseQ := `
			SELECT
				a.action_id,
				a.redemption_id,
				a.actiontype,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				TO_CHAR(a.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				TO_CHAR(a.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,
				COALESCE(ri.entity_name,'') AS entity_name,
				COALESCE(ri.scheme_id::text,'') AS scheme_id,
				COALESCE(ri.folio_id::text,'') AS folio_id,
				COALESCE(ri.demat_id::text,'') AS demat_id,
				TO_CHAR(ri.transaction_date,'YYYY-MM-DD') AS transaction_date,
				TO_CHAR(ri.requested_date,'YYYY-MM-DD') AS requested_date,
				COALESCE(ri.by_amount,0) AS by_amount,
				COALESCE(ri.old_by_amount,0) AS old_by_amount,
				COALESCE(ri.by_units,0) AS by_units,
				COALESCE(ri.old_by_units,0) AS old_by_units,
				COALESCE(ri.estimated_proceeds,0) AS estimated_proceeds,
				COALESCE(ri.gain_loss,0) AS gain_loss,
				COALESCE(ri.is_deleted,false) AS is_deleted
			FROM investment.auditactionredemption a
			LEFT JOIN investment.redemption_initiation ri ON ri.redemption_id = a.redemption_id`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.RedemptionID) != "" {
			q = baseQ + `
			WHERE a.redemption_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, req.RedemptionID)
		} else {
			q = baseQ + `
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1000`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		payload, err := collectRedemptionAuditRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read redemption initiation audit history")
			return
		}
		api.LogInfo("RedemptionInitiation AuditHistory: returned %d records", len(payload))
		api.RespondWithPayload(w, true, "", payload)
	}
}

// ─── GetRedemptionConfirmationAuditHistory ───────────────────────────────────
// POST /investment/redemption/confirmation/audit-history
// Body: { user_id, redemption_confirm_id? }

func GetRedemptionConfirmationAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			UserID              string `json:"user_id"`
			RedemptionConfirmID string `json:"redemption_confirm_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "user_id is required")
			return
		}
		if api.GetSessionFromCtx(r.Context()) == nil {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		baseQ := `
			SELECT
				a.action_id,
				a.redemption_confirm_id,
				a.actiontype,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				TO_CHAR(a.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				TO_CHAR(a.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,
				COALESCE(rc.redemption_id::text,'') AS redemption_id,
				COALESCE(rc.actual_nav,0) AS actual_nav,
				COALESCE(rc.old_actual_nav,0) AS old_actual_nav,
				COALESCE(rc.actual_units,0) AS actual_units,
				COALESCE(rc.old_actual_units,0) AS old_actual_units,
				COALESCE(rc.gross_proceeds,0) AS gross_proceeds,
				COALESCE(rc.old_gross_proceeds,0) AS old_gross_proceeds,
				COALESCE(rc.exit_load,0) AS exit_load,
				COALESCE(rc.tds,0) AS tds,
				COALESCE(rc.net_credited,0) AS net_credited,
				COALESCE(rc.stt_charges,0) AS stt_charges,
				COALESCE(rc.resolution_variance,'') AS resolution_variance,
				COALESCE(rc.resolution_comment,'') AS resolution_comment,
				COALESCE(rc.final_realised_capital_gain_loss,0) AS final_realised_capital_gain_loss,
				COALESCE(rc.status,'') AS status,
				COALESCE(rc.is_deleted,false) AS is_deleted
			FROM investment.auditactionredemptionconfirmation a
			LEFT JOIN investment.redemption_confirmation rc ON rc.redemption_confirm_id = a.redemption_confirm_id`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.RedemptionConfirmID) != "" {
			q = baseQ + `
			WHERE a.redemption_confirm_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, req.RedemptionConfirmID)
		} else {
			q = baseQ + `
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1000`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		payload, err := collectRedemptionAuditRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read redemption confirmation audit history")
			return
		}
		api.LogInfo("RedemptionConfirmation AuditHistory: returned %d records", len(payload))
		api.RespondWithPayload(w, true, "", payload)
	}
}
