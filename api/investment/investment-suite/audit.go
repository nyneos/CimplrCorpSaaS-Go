package investmentsuite

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── Shared helpers ───────────────────────────────────────────────────────────

func collectInvAuditRows(rows pgx.Rows) ([]map[string]interface{}, error) {
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

// ─── GetProposalAuditHistory ─────────────────────────────────────────────────
// POST /investment/proposals/audit-history
// Body: { user_id, proposal_id? }

func GetProposalAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			UserID     string `json:"user_id"`
			ProposalID string `json:"proposal_id"`
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
				a.proposal_id,
				a.actiontype,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				TO_CHAR(a.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				TO_CHAR(a.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,
				COALESCE(p.proposal_name,'') AS proposal_name,
				COALESCE(p.old_proposal_name,'') AS old_proposal_name,
				COALESCE(p.entity_name,'') AS entity_name,
				COALESCE(p.old_entity_name,'') AS old_entity_name,
				COALESCE(p.total_amount,0) AS total_amount,
				COALESCE(p.old_total_amount,0) AS old_total_amount,
				COALESCE(p.horizon_days,0) AS horizon_days,
				COALESCE(p.old_horizon_days,0) AS old_horizon_days,
				COALESCE(p.source,'') AS source,
				COALESCE(p.old_source,'') AS old_source,
				COALESCE(p.is_deleted,false) AS is_deleted
			FROM investment.auditactionproposal a
			LEFT JOIN investment.investment_proposal p ON p.proposal_id = a.proposal_id`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.ProposalID) != "" {
			q = baseQ + `
			WHERE a.proposal_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, req.ProposalID)
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
		payload, err := collectInvAuditRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read proposal audit history")
			return
		}
		api.LogInfo("Proposal AuditHistory: returned %d records", len(payload))
		api.RespondWithPayload(w, true, "", payload)
	}
}

// ─── GetInitiationAuditHistory ───────────────────────────────────────────────
// POST /investment/initiation/audit-history
// Body: { user_id, initiation_id? }

func GetInitiationAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			UserID       string `json:"user_id"`
			InitiationID string `json:"initiation_id"`
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
				a.initiation_id,
				a.actiontype,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				TO_CHAR(a.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				TO_CHAR(a.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,
				COALESCE(i.proposal_id::text,'') AS proposal_id,
				TO_CHAR(i.transaction_date,'YYYY-MM-DD') AS transaction_date,
				COALESCE(i.entity_name,'') AS entity_name,
				COALESCE(i.old_entity_name,'') AS old_entity_name,
				COALESCE(i.scheme_id::text,'') AS scheme_id,
				COALESCE(i.folio_id::text,'') AS folio_id,
				COALESCE(i.demat_id::text,'') AS demat_id,
				COALESCE(i.amount,0) AS amount,
				COALESCE(i.old_amount,0) AS old_amount,
				COALESCE(i.source,'') AS source,
				COALESCE(i.old_source,'') AS old_source,
				COALESCE(i.is_deleted,false) AS is_deleted
			FROM investment.auditactioninitiation a
			LEFT JOIN investment.investment_initiation i ON i.initiation_id = a.initiation_id`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.InitiationID) != "" {
			q = baseQ + `
			WHERE a.initiation_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, req.InitiationID)
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
		payload, err := collectInvAuditRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read initiation audit history")
			return
		}
		api.LogInfo("Initiation AuditHistory: returned %d records", len(payload))
		api.RespondWithPayload(w, true, "", payload)
	}
}

// ─── GetConfirmationAuditHistory ─────────────────────────────────────────────
// POST /investment/confirmation/audit-history
// Body: { user_id, confirmation_id? }

func GetConfirmationAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			UserID         string `json:"user_id"`
			ConfirmationID string `json:"confirmation_id"`
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
				a.confirmation_id,
				a.actiontype,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				TO_CHAR(a.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				TO_CHAR(a.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,
				COALESCE(c.initiation_id::text,'') AS initiation_id,
				TO_CHAR(c.nav_date,'YYYY-MM-DD') AS nav_date,
				COALESCE(c.nav,0) AS nav,
				COALESCE(c.old_nav,0) AS old_nav,
				COALESCE(c.allotted_units,0) AS allotted_units,
				COALESCE(c.old_allotted_units,0) AS old_allotted_units,
				COALESCE(c.net_amount,0) AS net_amount,
				COALESCE(c.old_net_amount,0) AS old_net_amount,
				COALESCE(c.actual_nav,0) AS actual_nav,
				COALESCE(c.actual_allotted_units,0) AS actual_allotted_units,
				COALESCE(c.variance_nav,0) AS variance_nav,
				COALESCE(c.variance_units,0) AS variance_units,
				COALESCE(c.resolution_comment,'') AS resolution_comment,
				COALESCE(c.status,'') AS status,
				COALESCE(c.is_deleted,false) AS is_deleted
			FROM investment.auditactioninvestmentconfirmation a
			LEFT JOIN investment.investment_confirmation c ON c.confirmation_id = a.confirmation_id`

		var (
			q    string
			args []interface{}
		)
		if strings.TrimSpace(req.ConfirmationID) != "" {
			q = baseQ + `
			WHERE a.confirmation_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, req.ConfirmationID)
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
		payload, err := collectInvAuditRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read confirmation audit history")
			return
		}
		api.LogInfo("Confirmation AuditHistory: returned %d records", len(payload))
		api.RespondWithPayload(w, true, "", payload)
	}
}
