package scope

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgxpool"
)

// listWithAuditQuery is the shared latest_audit + approval-engine LATERAL
// join shape for ListScope — same conventions as cycle/list.go's
// listWithAuditQuery, per the handler spec's Section 9 query conventions,
// joined to investment.fd_master for entity_name/principal_amount display.
const listWithAuditQuery = `
	WITH latest_audit AS (
		SELECT DISTINCT ON (a.scope_id)
			a.scope_id, a.audit_id, a.action_type, a.processing_status,
			a.requested_by, a.requested_at, a.checker_by, a.checker_at, a.checker_comment, a.reason,
			a.old_selection_status
		FROM investment.fd_closing_cycle_fd_scope_audit a
		ORDER BY a.scope_id,
		         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),
		                  COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
	)
	SELECT
		m.scope_id, m.cycle_id, m.fd_id,
		COALESCE(fm.entity_id,'') AS entity_id,
		COALESCE(fm.entity_name,'') AS entity_name,
		COALESCE(fm.principal_amount,0) AS principal_amount,
		m.selection_status,
		m.added_by, TO_CHAR(m.added_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS added_at,
		COALESCE(m.approved_by,'') AS approved_by,
		COALESCE(TO_CHAR(m.approved_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'') AS approved_at,
		COALESCE(m.removed_by,'') AS removed_by,
		COALESCE(TO_CHAR(m.removed_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),'') AS removed_at,
		m.is_deleted,

		COALESCE(l.audit_id::text,'') AS audit_id,
		COALESCE(l.action_type,'') AS action_type,
		COALESCE(l.processing_status,'') AS processing_status,
		COALESCE(l.requested_by,'') AS requested_by,
		COALESCE(TO_CHAR((l.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
		COALESCE(l.checker_by,'') AS checker_by,
		COALESCE(TO_CHAR((l.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
		COALESCE(l.checker_comment,'') AS checker_comment,
		COALESCE(l.reason,'') AS reason,
		COALESCE(l.old_selection_status,'') AS old_selection_status,

		COALESCE(ai.instance_id,'') AS approval_instance_id,
		COALESCE(ai.status,'') AS approval_engine_status,
		COALESCE(aie.instance_eye_id,'') AS current_eye_id,
		COALESCE(aie.position::text,'') AS current_eye_position,
		COALESCE(aie.approvals_required,0) AS approvals_required,
		COALESCE(aie.approvals_received,0) AS approvals_received,
		aie.sla_deadline AS sla_deadline,
		COALESCE(aie.is_escalated,false) AS is_escalated
	FROM investment.fd_closing_cycle_fd_scope m
	LEFT JOIN investment.fd_master fm ON fm.fd_id = m.fd_id
	LEFT JOIN latest_audit l ON l.scope_id = m.scope_id
	LEFT JOIN LATERAL (
		SELECT ai.* FROM uam.approval_instance ai
		WHERE ai.record_id = m.scope_id AND ai.module_code = $moduleCode$
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
	WHERE m.is_deleted = false`

// ListScope handles POST /investment/fd-closing/scope/list. cycle_id scopes
// to one cycle's FD-scope grid when supplied; omitted, it lists every scope
// row across every cycle the caller's entity scope can see (the "All Scope
// Selections" tab), entity-filtered the same way cycle/list.go does.
func ListScope(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CycleID string `json:"cycle_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		req.CycleID = strings.TrimSpace(req.CycleID)

		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)

		q := strings.Replace(listWithAuditQuery, "$moduleCode$", "$1", 1)
		args := []interface{}{moduleCode}
		argIdx := 2

		if req.CycleID != "" {
			var entityID string
			if err := pool.QueryRow(ctx, `
				SELECT entity_id FROM investment.fd_closing_cycle
				WHERE cycle_id = $1 AND is_deleted = false`,
				req.CycleID,
			).Scan(&entityID); err != nil {
				fdclosingcommon.RespondError(w, http.StatusNotFound, "Cycle not found")
				return
			}
			if !scope.HasEntityAccess(entityID) {
				fdclosingcommon.RespondError(w, http.StatusForbidden,
					"Entity ID '"+entityID+"' is not within your authorized access scope.")
				return
			}
			q += " AND m.cycle_id = $" + strconv.Itoa(argIdx)
			args = append(args, req.CycleID)
			argIdx++
		} else if !scope.IsAdminOverride && len(scope.EntityIDs) > 0 {
			q += ` AND EXISTS (
				SELECT 1 FROM investment.fd_closing_cycle c
				WHERE c.cycle_id = m.cycle_id AND c.entity_id = ANY($` + strconv.Itoa(argIdx) + `::text[])
			)`
			args = append(args, scope.EntityIDs)
			argIdx++
		}

		q += ` ORDER BY GREATEST(
			COALESCE(l.requested_at,'1970-01-01'::timestamp),
			COALESCE(l.checker_at,'1970-01-01'::timestamp)
		) DESC`

		rows, err := pool.Query(ctx, q, args...)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingScope] ListScope query: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		defer rows.Close()

		out, err := scanRowsToMaps(rows)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingScope] ListScope row error: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{"rows": out})
		api.LogInfo("[FDClosingScope] ListScope: cycle=%s %d rows", req.CycleID, len(out))
	}
}
