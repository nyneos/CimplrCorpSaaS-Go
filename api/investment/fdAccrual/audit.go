package fdAccrual

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── Request types ────────────────────────────────────────────────────────────

type accrualRunAuditRequest struct {
	UserID string `json:"user_id"`
	RunID  string `json:"run_id"`
}

type accrualLedgerAuditRequest struct {
	UserID   string `json:"user_id"`
	RunID    string `json:"run_id"`
	FDID     string `json:"fd_id"`
	LedgerID string `json:"ledger_id"`
}

// ─── GetAccrualRunAuditHandler ────────────────────────────────────────────────
// POST /investment/fd/accrual/run/audit
// Body: { user_id, run_id }
// Response: { success: true, data: [...] }

func GetAccrualRunAuditHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req accrualRunAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil ||
			strings.TrimSpace(req.RunID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "run_id is required")
			return
		}

		ctx := r.Context()
		if code, msg := requireAccrualRunAccess(ctx, pgxPool, req.RunID); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		rows, err := pgxPool.Query(ctx, `
			SELECT
				a.audit_id,
				a.run_id,
				a.action_type,
				a.processing_status,
				COALESCE(a.reason, '')                                              AS reason,
				COALESCE(a.requested_by, '')                                        AS requested_by,
				COALESCE(TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '')      AS requested_at,
				COALESCE(a.requested_ip, '')                                        AS requested_ip,
				COALESCE(checker_by, '')                                          AS checker_by,
				COALESCE(TO_CHAR((checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '')        AS checker_at,
				COALESCE(a.checker_ip, '')                                          AS checker_ip,
				COALESCE(a.checker_comment, '')                                     AS checker_comment,
				COALESCE(a.old_run_status, '')                                      AS old_run_status,
				COALESCE(a.old_run_mode, '')                                        AS old_run_mode,
				COALESCE(a.old_day_count_convention, '')                            AS old_day_count_convention,
				COALESCE(a.old_rounding_rule, '')                                   AS old_rounding_rule,
				COALESCE(a.old_precision_decimals, 0)                               AS old_precision_decimals,
				a.old_accrual_period_start,
				a.old_accrual_period_end,
				COALESCE(a.old_financial_period, '')                                AS old_financial_period,
				COALESCE(a.old_fd_status_filter, '')                                AS old_fd_status_filter,
				COALESCE(a.old_fd_inclusion_method, '')                             AS old_fd_inclusion_method,
				COALESCE(ai.instance_id,'')                                         AS approval_instance_id,
				COALESCE(ai.status,'')                                              AS approval_engine_status,
				COALESCE(aie.instance_eye_id,'')                                    AS current_eye_id,
				COALESCE(aie.position::text,'')                                     AS current_eye_position,
				COALESCE(aie.approvals_required,0)                                  AS approvals_required,
				COALESCE(aie.approvals_received,0)                                  AS approvals_received,
				aie.sla_deadline                                                    AS sla_deadline,
				COALESCE(aie.is_escalated,false)                                    AS is_escalated
			FROM investment.fd_accrual_run_audit a
			LEFT JOIN uam.approval_instance ai
				ON ai.record_id = a.run_id
				AND ai.module_code = 'FIXED_DEPOSIT'
				AND ai.status = 'PENDING'
				AND ai.is_deleted = false
			LEFT JOIN uam.approval_instance_eye aie
				ON aie.instance_id = ai.instance_id
				AND aie.status = 'ACTIVE'
			WHERE a.run_id = $1
			ORDER BY a.requested_at DESC
		`, req.RunID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}

		payload, err := collectAccrualPgxRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read accrual run audit history")
			return
		}
		uploadRows, err := pgxPool.Query(ctx, `
			SELECT
				('file-' || a.audit_id::text) AS audit_id,
				a.parent_record_id AS run_id,
				a.file_id,
				'UPLOAD_FILE' AS action_type,
				COALESCE(a.processing_status, '') AS processing_status,
				COALESCE(a.reason, '') AS reason,
				COALESCE(a.requested_by, '') AS requested_by,
				COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS requested_at,
				COALESCE(a.requested_ip, '') AS requested_ip,
				COALESCE(a.checker_by, '') AS checker_by,
				COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS checker_at,
				COALESCE(a.checker_ip, '') AS checker_ip,
				COALESCE(a.checker_comment, '') AS checker_comment
			FROM investment.additional_file_audit a
			JOIN investment.fd_accrual_run_files f ON f.file_id = a.file_id AND f.run_id::text = a.parent_record_id
			WHERE a.module_key = 'fd-accrual-run-additional'
			  AND a.parent_record_id = $1
			  AND a.action_type = 'CREATE'
			ORDER BY a.requested_at DESC`, req.RunID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		uploadPayload, err := collectAccrualPgxRows(uploadRows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read accrual run upload audit history")
			return
		}
		payload = append(payload, uploadPayload...)

		respondFDAccrualAuditPayload(w, payload)
	}
}

// ─── GetAccrualLedgerAuditHandler ─────────────────────────────────────────────
// POST /investment/fd/accrual/ledger/audit
// Body: { user_id, run_id?, fd_id?, ledger_id? }  (at least one required)
// Response: { success: true, data: [...] }

func GetAccrualLedgerAuditHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req accrualLedgerAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if strings.TrimSpace(req.RunID) == "" &&
			strings.TrimSpace(req.FDID) == "" &&
			strings.TrimSpace(req.LedgerID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "at least one of run_id, fd_id, or ledger_id is required")
			return
		}

		ctx := r.Context()
		if req.RunID != "" {
			if code, msg := requireAccrualRunAccess(ctx, pgxPool, req.RunID); code != 0 {
				api.RespondWithError(w, code, msg)
				return
			}
		}

		baseQuery := `
			SELECT
				a.audit_id,
				a.ledger_id,
				a.run_id,
				a.fd_id,
				COALESCE(a.fd_ref_no, '')                                         AS fd_ref_no,
				COALESCE(a.bank_name, '')                                         AS bank_name,
				COALESCE(a.entity_id, '')                                         AS entity_id,
				a.action_type,
				a.processing_status,
				COALESCE(a.requested_by, '')                                      AS requested_by,
				COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '')    AS requested_at,
				COALESCE(a.requested_ip, '')                                      AS requested_ip,
				COALESCE(a.checker_by, '')                                        AS checker_by,
				COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '')      AS checker_at,
				COALESCE(a.checker_ip, '')                                        AS checker_ip,
				COALESCE(a.checker_comment, '')                                   AS checker_comment,
				COALESCE(a.principal_amount, 0)                                   AS principal_amount,
				COALESCE(a.interest_rate, 0)                                      AS interest_rate,
				COALESCE(a.accrual_days, 0)                                       AS accrual_days,
				COALESCE(a.old_period_interest_accrued, 0)                        AS old_period_interest_accrued,
				COALESCE(a.old_closing_accrued_balance, 0)                        AS old_closing_accrued_balance,
				COALESCE(a.old_tds_deducted_in_period, 0)                         AS old_tds_deducted_in_period,
				COALESCE(a.old_net_interest_in_period, 0)                         AS old_net_interest_in_period,
				COALESCE(a.old_ledger_row_status, '')                             AS old_ledger_row_status,
				COALESCE(a.old_formula_used, '')                                  AS old_formula_used,
				COALESCE(a.old_is_overridden, false)                              AS old_is_overridden,
				COALESCE(a.old_override_amount, 0)                                AS old_override_amount,
				COALESCE(a.old_override_status, '')                               AS old_override_status
			FROM investment.fd_accrual_ledger_audit a
			WHERE 1=1`

		args := make([]interface{}, 0, 3)
		argIdx := 1

		if req.RunID != "" {
			baseQuery += fmt.Sprintf(" AND a.run_id = $%d", argIdx)
			args = append(args, req.RunID)
			argIdx++
		}
		if req.FDID != "" {
			baseQuery += fmt.Sprintf(" AND a.fd_id = $%d", argIdx)
			args = append(args, req.FDID)
			argIdx++
		}
		if req.LedgerID != "" {
			baseQuery += fmt.Sprintf(" AND a.ledger_id = $%d", argIdx)
			args = append(args, req.LedgerID)
			argIdx++
		}
		baseQuery += accrualEntityScopePredicate(ctx, "a", &argIdx, &args)
		baseQuery += ` ORDER BY a.requested_at DESC`

		rows, err := pgxPool.Query(ctx, baseQuery, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}

		payload, err := collectAccrualPgxRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read accrual ledger audit history")
			return
		}
		ledgerIDs := make([]string, 0, len(payload))
		if req.LedgerID != "" {
			ledgerIDs = append(ledgerIDs, req.LedgerID)
		} else {
			for _, row := range payload {
				if ledgerID := strings.TrimSpace(fmt.Sprint(row["ledger_id"])); ledgerID != "" {
					ledgerIDs = append(ledgerIDs, ledgerID)
				}
			}
		}
		if len(ledgerIDs) > 0 {
			uploadRows, err := pgxPool.Query(ctx, `
				SELECT
					('file-' || a.audit_id::text) AS audit_id,
					a.parent_record_id AS ledger_id,
					a.file_id,
					'UPLOAD_FILE' AS action_type,
					COALESCE(a.processing_status, '') AS processing_status,
					COALESCE(a.reason, '') AS reason,
					COALESCE(a.requested_by, '') AS requested_by,
					COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS requested_at,
					COALESCE(a.requested_ip, '') AS requested_ip,
					COALESCE(a.checker_by, '') AS checker_by,
					COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS checker_at,
					COALESCE(a.checker_ip, '') AS checker_ip,
					COALESCE(a.checker_comment, '') AS checker_comment
				FROM investment.additional_file_audit a
				JOIN investment.fd_accrual_ledger_files f ON f.file_id = a.file_id AND f.ledger_id::text = a.parent_record_id
				WHERE a.module_key = 'fd-accrual-ledger-additional'
				  AND a.parent_record_id = ANY($1::text[])
				  AND a.action_type = 'CREATE'
				ORDER BY a.requested_at DESC`, ledgerIDs)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
				return
			}
			uploadPayload, err := collectAccrualPgxRows(uploadRows)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read accrual ledger upload audit history")
				return
			}
			payload = append(payload, uploadPayload...)
		}

		respondFDAccrualAuditPayload(w, payload)
	}
}

// ─── Shared helpers ───────────────────────────────────────────────────────────

// ─── GetScheduleConfigAuditHandler ───────────────────────────────────────────
// POST /investment/fd/accrual/schedule/audit
// Body: { user_id, config_id }
// Response: { success: true, data: [...] }

func GetScheduleConfigAuditHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			UserID   string `json:"user_id"`
			ConfigID string `json:"config_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil ||
			strings.TrimSpace(req.ConfigID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "config_id is required")
			return
		}

		ctx := r.Context()
		if _, ok, err := requireScheduleConfigScope(ctx, pgxPool, req.ConfigID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrScheduleConfigLookupFailed+err.Error())
			return
		} else if !ok {
			api.RespondWithError(w, http.StatusForbidden, "Schedule config is not within your authorized entity scope.")
			return
		}

		rows, err := pgxPool.Query(ctx, `
			SELECT
				audit_id,
				config_id,
				action_type,
				processing_status,
				COALESCE(reason, '')                                              AS reason,
				COALESCE(requested_by, '')                                        AS requested_by,
				COALESCE(TO_CHAR((requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '')      AS requested_at,
				COALESCE(requested_ip, '')                                        AS requested_ip,
				COALESCE(checker_by, '')                                          AS checker_by,
				COALESCE(TO_CHAR((checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '')        AS checker_at,
				COALESCE(checker_ip, '')                                          AS checker_ip,
				COALESCE(checker_comment, '')                                     AS checker_comment,
				COALESCE(old_schedule_frequency, '')                              AS old_schedule_frequency,
				COALESCE(old_run_day_of_month, 0)                                 AS old_run_day_of_month,
				COALESCE(TO_CHAR(old_run_time, 'HH24:MI:SS'), '')                 AS old_run_time,
				COALESCE(old_default_run_mode, '')                                AS old_default_run_mode,
				COALESCE(old_default_bank_id_filter, '')                          AS old_default_bank_id_filter,
				COALESCE(old_default_fd_status_filter, '')                        AS old_default_fd_status_filter,
				COALESCE(old_auto_submit_for_approval, false)                     AS old_auto_submit_for_approval,
				COALESCE(old_is_active, false)                                    AS old_is_active
			FROM investment.fd_accrual_schedule_config_audit
			WHERE config_id = $1
			ORDER BY requested_at DESC
		`, req.ConfigID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}

		payload, err := collectAccrualPgxRows(rows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read schedule config audit history")
			return
		}
		uploadRows, err := pgxPool.Query(ctx, `
			SELECT
				('file-' || a.audit_id::text) AS audit_id,
				a.parent_record_id AS config_id,
				a.file_id,
				'UPLOAD_FILE' AS action_type,
				COALESCE(a.processing_status, '') AS processing_status,
				COALESCE(a.reason, '') AS reason,
				COALESCE(a.requested_by, '') AS requested_by,
				COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS requested_at,
				COALESCE(a.requested_ip, '') AS requested_ip,
				COALESCE(a.checker_by, '') AS checker_by,
				COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '') AS checker_at,
				COALESCE(a.checker_ip, '') AS checker_ip,
				COALESCE(a.checker_comment, '') AS checker_comment
			FROM investment.additional_file_audit a
			JOIN investment.fd_accrual_schedule_config_files f ON f.file_id = a.file_id AND f.config_id::text = a.parent_record_id
			WHERE a.module_key = 'fd-accrual-schedule-config-additional'
			  AND a.parent_record_id = $1
			  AND a.action_type = 'CREATE'
			ORDER BY a.requested_at DESC`, req.ConfigID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		uploadPayload, err := collectAccrualPgxRows(uploadRows)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read schedule config upload audit history")
			return
		}
		payload = append(payload, uploadPayload...)

		respondFDAccrualAuditPayload(w, payload)
	}
}

func collectAccrualPgxRows(rows pgx.Rows) ([]map[string]interface{}, error) {
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

func respondFDAccrualAuditPayload(w http.ResponseWriter, payload interface{}) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
		"success": true,
		"data":    payload,
	})
}
