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

// auditHistorySelect is the shared SELECT (no WHERE/ORDER BY) used by
// AuditScope so the column shape stays aligned with ListScope's latest_audit.
const auditHistorySelect = `
	SELECT
		a.audit_id::text, a.scope_id, m.cycle_id, m.fd_id,
		a.action_type, a.processing_status,
		COALESCE(a.requested_by,'') AS requested_by,
		COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
		COALESCE(a.checker_by,'') AS checker_by,
		COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
		COALESCE(a.checker_comment,'') AS checker_comment,
		COALESCE(a.reason,'') AS reason,
		COALESCE(a.old_selection_status,'') AS old_selection_status
	FROM investment.fd_closing_cycle_fd_scope_audit a
	JOIN investment.fd_closing_cycle_fd_scope m ON m.scope_id = a.scope_id`

// AuditScope handles POST /investment/fd-closing/scope/audit.
// scope_id is optional: omitted, it returns the latest rows across every
// scope the caller can see (entity-scoped), same shape as cycle/audit.go.
func AuditScope(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ScopeID string `json:"scope_id"`
			CycleID string `json:"cycle_id"`
			Limit   int    `json:"limit"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		limit := req.Limit
		if limit <= 0 || limit > 1000 {
			limit = 200
		}

		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)

		q := auditHistorySelect + `
			JOIN investment.fd_closing_cycle c ON c.cycle_id = m.cycle_id
			WHERE 1=1`
		var args []interface{}
		argIdx := 1
		if id := strings.TrimSpace(req.ScopeID); id != "" {
			q += " AND a.scope_id = $" + strconv.Itoa(argIdx)
			args = append(args, id)
			argIdx++
		}
		if id := strings.TrimSpace(req.CycleID); id != "" {
			q += " AND m.cycle_id = $" + strconv.Itoa(argIdx)
			args = append(args, id)
			argIdx++
		}
		if !scope.IsAdminOverride && len(scope.EntityIDs) > 0 {
			q += " AND c.entity_id = ANY($" + strconv.Itoa(argIdx) + "::text[])"
			args = append(args, scope.EntityIDs)
			argIdx++
		}
		q += " ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC"
		if strings.TrimSpace(req.ScopeID) == "" {
			q += " LIMIT $" + strconv.Itoa(argIdx)
			args = append(args, limit)
		}

		rows, err := pool.Query(ctx, q, args...)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingScope] AuditScope query: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		defer rows.Close()

		out, err := scanRowsToMaps(rows)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingScope] AuditScope row error: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{"audit_logs": out})
		api.LogInfo("[FDClosingScope] AuditScope: %d records", len(out))
	}
}
