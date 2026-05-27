package accountingworkbench

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func collectAccountingAuditRows(rows pgx.Rows) ([]map[string]interface{}, error) {
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

// ─── GetAccountingActivityAuditHistory ───────────────────────────────────────
// POST /investment/accounting/activity/audit-history
// Body: { user_id, activity_id? }
// Covers all activity types: MTM, DIVIDEND, FVO, CORPORATE_ACTION, ACCOUNTING, etc.

func GetAccountingActivityAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			UserID     string `json:"user_id"`
			ActivityID string `json:"activity_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
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
				a.activity_id,
				a.actiontype,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				TO_CHAR(a.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				TO_CHAR(a.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,
				COALESCE(ac.activity_type,'') AS activity_type,
				COALESCE(ac.activity_subtype,'') AS activity_subtype,
				TO_CHAR(ac.effective_date,'YYYY-MM-DD') AS effective_date,
				COALESCE(ac.accounting_period,'') AS accounting_period,
				COALESCE(ac.data_source,'') AS data_source,
				COALESCE(ac.status,'') AS status,
				COALESCE(ac.is_deleted,false) AS is_deleted
			FROM investment.auditactionaccountingactivity a
			LEFT JOIN investment.accounting_activity ac ON ac.activity_id = a.activity_id`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.ActivityID) != "" {
			q = baseQ + `
			WHERE a.activity_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, req.ActivityID)
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
		payload, err := collectAccountingAuditRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read accounting activity audit history")
			return
		}
		api.LogInfo("AccountingActivity AuditHistory: returned %d records", len(payload))
		api.RespondWithPayload(w, true, "", payload)
	}
}
