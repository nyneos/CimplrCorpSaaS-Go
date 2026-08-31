package reopen

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// listQuery is the shared SELECT for ListReopenRequests/DetailReopenRequest.
// There is no separate fd_closing_reopen_request_audit table (the request
// row IS its own audit trail), so this joins straight to the request table
// itself plus the same approval-engine LATERAL join shape as
// cycle.listWithAuditQuery / lock.listQuery.
const listQuery = `
	SELECT
		rr.request_id, rr.cycle_id, rr.reason, COALESCE(rr.impact_summary,'') AS impact_summary,
		rr.approver_id, rr.approver_name, COALESCE(rr.approver_role,'') AS approver_role,
		COALESCE(rr.accrual_valid,false) AS accrual_valid,
		COALESCE(rr.reconciliation_valid,false) AS reconciliation_valid,
		COALESCE(rr.accounting_valid,false) AS accounting_valid,
		rr.validation_status, COALESCE(rr.validation_errors,'') AS validation_errors,
		rr.processing_status,
		COALESCE(rr.requested_by,'') AS requested_by,
		COALESCE(TO_CHAR((rr.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
		COALESCE(rr.checker_by,'') AS checker_by,
		COALESCE(TO_CHAR((rr.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
		COALESCE(rr.checker_comment,'') AS checker_comment,
		COALESCE(TO_CHAR((rr.reopened_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),'') AS reopened_at,
		COALESCE(rr.reopened_by,'') AS reopened_by,
		COALESCE(TO_CHAR((rr.relocked_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),'') AS relocked_at,
		COALESCE(rr.relocked_by,'') AS relocked_by,
		c.entity_id, c.entity_name, c.financial_period,

		COALESCE(ai.instance_id,'') AS approval_instance_id,
		COALESCE(ai.status,'') AS approval_engine_status,
		COALESCE(aie.instance_eye_id,'') AS current_eye_id,
		COALESCE(aie.position::text,'') AS current_eye_position,
		COALESCE(aie.approvals_required,0) AS approvals_required,
		COALESCE(aie.approvals_received,0) AS approvals_received,
		aie.sla_deadline AS sla_deadline,
		COALESCE(aie.is_escalated,false) AS is_escalated
	FROM investment.fd_closing_reopen_request rr
	JOIN investment.fd_closing_cycle c ON c.cycle_id = rr.cycle_id
	LEFT JOIN LATERAL (
		SELECT ai.* FROM uam.approval_instance ai
		WHERE ai.record_id = rr.request_id AND ai.module_code = $1
		  AND ai.status = 'PENDING' AND ai.is_deleted = false
		ORDER BY ai.submitted_at DESC, ai.instance_id DESC
		LIMIT 1
	) ai ON true
	LEFT JOIN LATERAL (
		SELECT aie.* FROM uam.approval_instance_eye aie
		WHERE aie.instance_id = ai.instance_id AND aie.status = 'ACTIVE'
		ORDER BY aie.position ASC, aie.instance_eye_id ASC
		LIMIT 1
	) aie ON true
	WHERE rr.is_deleted = false`

// ListReopenRequests handles POST /investment/fd-closing/reopen/list.
func ListReopenRequests(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CycleID string `json:"cycle_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req) // body is optional for list

		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)

		q := listQuery
		args := []interface{}{moduleCode}
		argIdx := 2

		if !scope.IsAdminOverride && len(scope.EntityIDs) > 0 {
			q += " AND c.entity_id = ANY($" + strconv.Itoa(argIdx) + "::text[])"
			args = append(args, scope.EntityIDs)
			argIdx++
		}
		if strings.TrimSpace(req.CycleID) != "" {
			q += " AND rr.cycle_id = $" + strconv.Itoa(argIdx)
			args = append(args, strings.TrimSpace(req.CycleID))
			argIdx++
		}
		q += ` ORDER BY GREATEST(
			COALESCE(rr.requested_at,'1970-01-01'::timestamp),
			COALESCE(rr.checker_at,'1970-01-01'::timestamp)
		) DESC`

		rows, err := pool.Query(ctx, q, args...)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingReopen] ListReopenRequests query: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		defer rows.Close()

		out, err := scanRowsToMaps(rows)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingReopen] ListReopenRequests row error: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{"rows": out})
		api.LogInfo("[FDClosingReopen] ListReopenRequests: %d rows", len(out))
	}
}

// scanRowsToMaps converts a pgx.Rows result into []map[string]interface{},
// same pattern as cycle.scanRowsToMaps / lock.scanRowsToMaps.
func scanRowsToMaps(rows pgx.Rows) ([]map[string]interface{}, error) {
	fields := rows.FieldDescriptions()
	out := make([]map[string]interface{}, 0, 50)
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
