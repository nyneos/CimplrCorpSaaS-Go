package fdInterestAndTdsWorkbench

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── Request types ────────────────────────────────────────────────────────────

type tdsReceiptAuditRequest struct {
	UserID string `json:"user_id"`
	TDSID  string `json:"tds_id"`
	FDID   string `json:"fd_id"`
}

// ─── GetTDSReceiptAuditHandler ────────────────────────────────────────────────
// POST /investment/fd/tds-register/audit
// Body: { user_id, tds_id } or { user_id, fd_id }  (at least one required)
// Response: { success: true, data: [...] }

func GetTDSReceiptAuditHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req tdsReceiptAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if strings.TrimSpace(req.TDSID) == "" && strings.TrimSpace(req.FDID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "tds_id or fd_id is required")
			return
		}

		ctx := r.Context()

		if req.TDSID != "" {
			if code, msg := validateTDSAuditAccess(ctx, pgxPool, req.TDSID); code != 0 {
				api.RespondWithError(w, code, msg)
				return
			}
			if msg := validateTDSRecordAccess(ctx, pgxPool, req.TDSID); msg != "" {
				api.RespondWithError(w, http.StatusForbidden, msg)
				return
			}
		}
		if req.FDID != "" {
			if msg := validateFDRecordAccess(ctx, pgxPool, req.FDID); msg != "" {
				api.RespondWithError(w, http.StatusForbidden, msg)
				return
			}
		}

		baseQuery := `
			SELECT
				a.audit_id,
				a.tds_id,
				a.action_type,
				a.processing_status,
				COALESCE(a.requested_by, '')                                          AS requested_by,
				COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '')        AS requested_at,
				COALESCE(a.requested_ip, '')                                          AS requested_ip,
				COALESCE(a.checker_by, '')                                            AS checker_by,
				COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD HH24:MI:SS'), '')          AS checker_at,
				COALESCE(a.checker_ip, '')                                            AS checker_ip,
				COALESCE(a.checker_comment, '')                                       AS checker_comment,
				COALESCE(t.fd_id, '')                                                 AS fd_id,
				COALESCE(t.fd_ref_no, '')                                             AS fd_ref_no,
				COALESCE(t.entity_id, '')                                             AS entity_id,
				COALESCE(t.tds_status, '')                                            AS tds_status,
				COALESCE(t.gross_interest, 0)                                         AS gross_interest,
				COALESCE(t.tds_expected, 0)                                           AS tds_expected,
				COALESCE(t.tds_deducted_actual, 0)                                    AS tds_deducted_actual,
				COALESCE(t.tds_variance, 0)                                           AS tds_variance,
				COALESCE(t.tds_rate_applied, 0)                                       AS tds_rate_applied,
				COALESCE(t.tds_section, '')                                           AS tds_section,
				COALESCE(TO_CHAR(t.period_start, 'YYYY-MM-DD'), '')                   AS period_start,
				COALESCE(TO_CHAR(t.period_end, 'YYYY-MM-DD'), '')                     AS period_end,
				COALESCE(TO_CHAR(t.deduction_date, 'YYYY-MM-DD'), '')                 AS deduction_date,
				COALESCE(ai.instance_id,'')                                           AS approval_instance_id,
				COALESCE(ai.status,'')                                                AS approval_engine_status,
				COALESCE(aie.instance_eye_id,'')                                      AS current_eye_id,
				COALESCE(aie.position::text,'')                                       AS current_eye_position,
				COALESCE(aie.approvals_required,0)                                    AS approvals_required,
				COALESCE(aie.approvals_received,0)                                    AS approvals_received,
				aie.sla_deadline                                                      AS sla_deadline,
				COALESCE(aie.is_escalated,false)                                      AS is_escalated
			FROM investment.fd_tds_receipt_audit a
			LEFT JOIN investment.fd_tds_receipt t ON t.tds_id = a.tds_id
			LEFT JOIN LATERAL (
				SELECT ai.* FROM uam.approval_instance ai
				WHERE ai.record_id = a.tds_id
				  AND ai.module_code = 'FIXED_DEPOSIT'
				  AND ai.status = 'PENDING'
				  AND ai.is_deleted = false
				ORDER BY ai.submitted_at DESC, ai.instance_id DESC
				LIMIT 1
			) ai ON true
			LEFT JOIN LATERAL (
				SELECT aie.* FROM uam.approval_instance_eye aie
				WHERE aie.instance_id = ai.instance_id
				  AND aie.status = 'ACTIVE'
				ORDER BY aie.position ASC, aie.instance_eye_id ASC
				LIMIT 1
			) aie ON true
			WHERE 1=1`

		args := make([]interface{}, 0, 2)
		argIdx := 1

		if req.TDSID != "" {
			baseQuery += fmt.Sprintf(" AND a.tds_id = $%d", argIdx)
			args = append(args, req.TDSID)
			argIdx++
		}
		if req.FDID != "" {
			baseQuery += fmt.Sprintf(" AND t.fd_id = $%d", argIdx)
			args = append(args, req.FDID)
			argIdx++
		}
		baseQuery += ` ORDER BY a.requested_at DESC`

		rows, err := pgxPool.Query(ctx, baseQuery, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}

		payload, err := rowsToMapSlice(rows)
		rows.Close()
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read TDS receipt audit history")
			return
		}
		if payload == nil {
			payload = []map[string]interface{}{}
		}

		respondTDSAuditPayload(w, payload)
	}
}

// ─── Access validator ─────────────────────────────────────────────────────────

func validateTDSAuditAccess(ctx context.Context, pool *pgxpool.Pool, tdsID string) (int, string) {
	var exists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM investment.fd_tds_receipt WHERE tds_id = $1 AND is_deleted = false)`,
		tdsID,
	).Scan(&exists); err != nil || !exists {
		return http.StatusNotFound, "TDS receipt not found"
	}
	return 0, ""
}

// ─── Response helper ──────────────────────────────────────────────────────────

func respondTDSAuditPayload(w http.ResponseWriter, payload interface{}) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
		"success": true,
		"data":    payload,
	})
}
