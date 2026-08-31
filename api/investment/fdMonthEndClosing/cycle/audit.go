package cycle

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

// auditHistorySelect is the shared SELECT (no WHERE/ORDER BY) used by both
// DetailCycle's embedded audit_history and AuditCycle's standalone endpoint,
// so the two never drift out of sync on column shape.
const auditHistorySelect = `
	SELECT
		a.audit_id::text, a.cycle_id, a.action_type, a.processing_status,
		COALESCE(a.requested_by,'') AS requested_by,
		COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
		COALESCE(a.checker_by,'') AS checker_by,
		COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
		COALESCE(a.checker_comment,'') AS checker_comment,
		COALESCE(a.reason,'') AS reason,
		COALESCE(a.old_status,'') AS old_status,
		COALESCE(a.old_bank_id,'') AS old_bank_id,
		COALESCE(a.old_currency_code,'') AS old_currency_code,
		COALESCE(a.old_include_matured,false) AS old_include_matured,
		COALESCE(a.new_bank_id,'') AS new_bank_id,
		COALESCE(a.new_currency_code,'') AS new_currency_code,
		COALESCE(a.new_include_matured,false) AS new_include_matured
	FROM investment.fd_closing_cycle_audit a`

// AuditCycle handles POST /investment/fd-closing/cycle/audit — a standalone
// paginated audit-trail endpoint pairing with a frontend AuditTrailSection +
// mapFDClosingCycleAuditToUI mapper (per the handler spec). cycle_id is
// optional: omitted, it returns the latest rows across every cycle the
// caller can see (entity-scoped), same shape as
// fdBookingWorkbench's GetBookingAuditHistory.
func AuditCycle(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
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

		var q string
		var args []interface{}
		if strings.TrimSpace(req.CycleID) != "" {
			q = auditHistorySelect + `
				JOIN investment.fd_closing_cycle m ON m.cycle_id = a.cycle_id
				WHERE a.cycle_id = $1`
			args = append(args, strings.TrimSpace(req.CycleID))
			if !scope.IsAdminOverride && len(scope.EntityIDs) > 0 {
				q += " AND m.entity_id = ANY($2::text[])"
				args = append(args, scope.EntityIDs)
			}
			q += " ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC"
		} else {
			q = auditHistorySelect + `
				JOIN investment.fd_closing_cycle m ON m.cycle_id = a.cycle_id
				WHERE 1=1`
			argIdx := 1
			if !scope.IsAdminOverride && len(scope.EntityIDs) > 0 {
				q += " AND m.entity_id = ANY($" + strconv.Itoa(argIdx) + "::text[])"
				args = append(args, scope.EntityIDs)
				argIdx++
			}
			q += " ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC"
			q += " LIMIT $" + strconv.Itoa(argIdx)
			args = append(args, limit)
		}

		rows, err := pool.Query(ctx, q, args...)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] AuditCycle query: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		defer rows.Close()

		out, err := scanRowsToMaps(rows)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] AuditCycle row error: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{"audit_logs": out})
		api.LogInfo("[FDClosingCycle] AuditCycle: %d records", len(out))
	}
}
