package fdMaturityAndRollover

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── Request types ────────────────────────────────────────────────────────────

type closureAuditRequest struct {
	UserID           string `json:"user_id"`
	ClosureRequestID string `json:"closure_request_id"`
}

// ─── GetClosureAuditHandler ───────────────────────────────────────────────────
// POST /investment/fd/closure/audit
// Body: { user_id, closure_request_id }
// Response: { success: true, data: [...] }

func GetClosureAuditHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req closureAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil ||
			strings.TrimSpace(req.ClosureRequestID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "closure_request_id is required")
			return
		}

		ctx := r.Context()
		if code, msg := validateClosureAuditAccess(ctx, pgxPool, req.ClosureRequestID); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		rows, err := pgxPool.Query(ctx, `
			SELECT
				audit_id,
				closure_request_id,
				action_type,
				processing_status,
				COALESCE(performed_by, '')                                              AS performed_by,
				COALESCE(performed_by_email, '')                                        AS performed_by_email,
				COALESCE(action_reason, '')                                             AS action_reason,
				COALESCE(TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS'), '')             AS created_at,
				COALESCE(old_closure_status, '')                                        AS old_closure_status,
				COALESCE(old_bank_id, '')                                               AS old_bank_id,
				COALESCE(old_bank_name, '')                                             AS old_bank_name,
				COALESCE(old_bank_fd_ref_no, '')                                        AS old_bank_fd_ref_no,
				COALESCE(old_entity_id, '')                                             AS old_entity_id,
				COALESCE(old_entity_name, '')                                           AS old_entity_name,
				COALESCE(old_principal_amount, 0)                                       AS old_principal_amount,
				COALESCE(old_interest_rate, 0)                                          AS old_interest_rate,
				COALESCE(old_interest_type_code, '')                                    AS old_interest_type_code,
				COALESCE(old_tenure_days, 0)                                            AS old_tenure_days,
				COALESCE(TO_CHAR(old_start_date, 'YYYY-MM-DD'), '')                     AS old_start_date,
				COALESCE(TO_CHAR(old_maturity_date, 'YYYY-MM-DD'), '')                  AS old_maturity_date,
				COALESCE(old_accrued_interest, 0)                                       AS old_accrued_interest,
				COALESCE(old_tds_deducted, 0)                                           AS old_tds_deducted,
				COALESCE(old_penalty_rate, 0)                                           AS old_penalty_rate,
				COALESCE(old_penalty_amount, 0)                                         AS old_penalty_amount,
				COALESCE(old_no_interest_flag, false)                                   AS old_no_interest_flag,
				COALESCE(old_days_held, 0)                                              AS old_days_held,
				COALESCE(old_contracted_rate, 0)                                        AS old_contracted_rate,
				COALESCE(old_applicable_rate, 0)                                        AS old_applicable_rate,
				COALESCE(old_net_payout_amount, 0)                                      AS old_net_payout_amount,
				COALESCE(old_settlement_account_id, '')                                 AS old_settlement_account_id,
				COALESCE(old_maturity_instructions, '')                                 AS old_maturity_instructions,
				COALESCE(old_principal_returned, 0)                                     AS old_principal_returned,
				COALESCE(old_gross_interest, 0)                                         AS old_gross_interest,
				COALESCE(old_rollover_type, '')                                         AS old_rollover_type,
				COALESCE(old_rollover_amount, 0)                                        AS old_rollover_amount,
				COALESCE(old_rollover_principal, 0)                                     AS old_rollover_principal,
				COALESCE(old_interest_credited, 0)                                      AS old_interest_credited,
				COALESCE(old_rollover_tenor_days, 0)                                    AS old_rollover_tenor_days,
				COALESCE(old_rollover_interest_rate, 0)                                 AS old_rollover_interest_rate,
				COALESCE(old_partial_withdrawal, false)                                 AS old_partial_withdrawal,
				COALESCE(TO_CHAR(old_new_maturity_date, 'YYYY-MM-DD'), '')              AS old_new_maturity_date,
				COALESCE(old_closure_reason, '')                                        AS old_closure_reason,
				COALESCE(old_closure_notes, '')                                         AS old_closure_notes,
				COALESCE(old_variance_remark, '')                                       AS old_variance_remark
			FROM investment.fd_audit_closure_request
			WHERE closure_request_id = $1
			ORDER BY created_at ASC
		`, req.ClosureRequestID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}

		payload, err := collectClosurePgxRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read closure audit history")
			return
		}

		respondClosureAuditPayload(w, payload)
	}
}

// ─── Access validator ─────────────────────────────────────────────────────────

func validateClosureAuditAccess(ctx context.Context, pool *pgxpool.Pool, closureRequestID string) (int, string) {
	var exists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM investment.fd_closure_request WHERE closure_request_id = $1)`,
		closureRequestID,
	).Scan(&exists); err != nil || !exists {
		return http.StatusNotFound, "closure request not found"
	}
	return 0, ""
}

// ─── Shared helpers ───────────────────────────────────────────────────────────

func collectClosurePgxRows(rows pgx.Rows) ([]map[string]interface{}, error) {
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
				row[f.Name] = ""
			} else {
				row[f.Name] = vals[i]
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func respondClosureAuditPayload(w http.ResponseWriter, payload interface{}) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
		"success": true,
		"data":    payload,
	})
}
