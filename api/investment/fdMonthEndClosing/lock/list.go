package lock

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

// listQuery is the shared SELECT for ListLockRequests/DetailLockRequest. There
// is no separate fd_closing_lock_request_audit table (the request row IS its
// own audit trail), so — unlike cycle's listWithAuditQuery — this joins
// straight to the request table itself rather than a latest_audit CTE, plus
// the same approval-engine LATERAL join shape as GetBookingsWithAudit.
const listQuery = `
	SELECT
		lr.request_id, lr.cycle_id, lr.lock_type,
		TO_CHAR(lr.lock_effective_date,'YYYY-MM-DD') AS lock_effective_date,
		COALESCE(lr.remarks,'') AS remarks,
		lr.approver_id, lr.approver_name, COALESCE(lr.approver_role,'') AS approver_role,
		lr.processing_status,
		COALESCE(lr.requested_by,'') AS requested_by,
		COALESCE(TO_CHAR((lr.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
		COALESCE(lr.checker_by,'') AS checker_by,
		COALESCE(TO_CHAR((lr.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
		COALESCE(lr.checker_comment,'') AS checker_comment,
		COALESCE(TO_CHAR((lr.applied_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),'') AS applied_at,
		COALESCE(lr.applied_by,'') AS applied_by,
		c.entity_id, c.entity_name, c.financial_period,

		COALESCE(ai.instance_id,'') AS approval_instance_id,
		COALESCE(ai.status,'') AS approval_engine_status,
		COALESCE(aie.instance_eye_id,'') AS current_eye_id,
		COALESCE(aie.position::text,'') AS current_eye_position,
		COALESCE(aie.approvals_required,0) AS approvals_required,
		COALESCE(aie.approvals_received,0) AS approvals_received,
		aie.sla_deadline AS sla_deadline,
		COALESCE(aie.is_escalated,false) AS is_escalated
	FROM investment.fd_closing_lock_request lr
	JOIN investment.fd_closing_cycle c ON c.cycle_id = lr.cycle_id
	LEFT JOIN LATERAL (
		SELECT ai.* FROM uam.approval_instance ai
		WHERE ai.record_id = lr.request_id AND ai.module_code = $1
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
	WHERE lr.is_deleted = false`

// ListLockRequests handles POST /investment/fd-closing/lock/list.
func ListLockRequests(pool *pgxpool.Pool) http.HandlerFunc {
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
			q += " AND lr.cycle_id = $" + strconv.Itoa(argIdx)
			args = append(args, strings.TrimSpace(req.CycleID))
			argIdx++
		}
		q += ` ORDER BY GREATEST(
			COALESCE(lr.requested_at,'1970-01-01'::timestamp),
			COALESCE(lr.checker_at,'1970-01-01'::timestamp)
		) DESC`

		rows, err := pool.Query(ctx, q, args...)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingLock] ListLockRequests query: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		defer rows.Close()

		out, err := scanRowsToMaps(rows)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingLock] ListLockRequests row error: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{"rows": out})
		api.LogInfo("[FDClosingLock] ListLockRequests: %d rows", len(out))
	}
}

// scanRowsToMaps converts a pgx.Rows result into []map[string]interface{},
// same pattern as cycle.scanRowsToMaps.
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
