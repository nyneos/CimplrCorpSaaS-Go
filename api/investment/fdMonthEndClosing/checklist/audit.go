package checklist

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

// checklistAuditSelect is the shared SELECT (no WHERE/ORDER BY) used by both
// DetailChecklistItem's embedded audit_history and AuditChecklistItem's
// standalone endpoint, so the two never drift out of sync on column shape —
// same pairing convention as cycle/audit.go's auditHistorySelect.
const checklistAuditSelect = `
	SELECT
		a.audit_id::text, a.item_id, a.action_type, a.processing_status,
		COALESCE(a.requested_by,'') AS requested_by,
		COALESCE(TO_CHAR((a.requested_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
		COALESCE(a.checker_by,'') AS checker_by,
		COALESCE(TO_CHAR((a.checker_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata'),'YYYY-MM-DD HH24:MI:SS'),'') AS checker_at,
		COALESCE(a.checker_comment,'') AS checker_comment,
		COALESCE(a.reason,'') AS reason,
		COALESCE(a.old_status,'') AS old_status,
		COALESCE(a.old_blocked_comment,'') AS old_blocked_comment,
		COALESCE(a.old_exception_count,0) AS old_exception_count,
		COALESCE(a.new_status,'') AS new_status,
		COALESCE(a.new_blocked_comment,'') AS new_blocked_comment,
		COALESCE(a.new_exception_count,0) AS new_exception_count,
		COALESCE(a.new_evidence_ref,'') AS new_evidence_ref,
		COALESCE(a.new_evidence_type,'') AS new_evidence_type,
		COALESCE(i.fd_id,'') AS fd_id,
		COALESCE(i.step_code,'') AS step_code,
		COALESCE(i.step_name,'') AS step_name,
		COALESCE(i.cycle_id,'') AS cycle_id,
		COALESCE(i.evidence_ref,'') AS evidence_ref,
		COALESCE(i.evidence_type,'') AS evidence_type,
		COALESCE(i.status,'') AS item_status
	FROM investment.fd_closing_checklist_item_audit a
	JOIN investment.fd_closing_checklist_item i ON i.item_id = a.item_id`

const (
	checklistAuditEntityFilter    = " AND c.entity_id = ANY($"
	checklistAuditTextArraySuffix = "::text[])"
	checklistAuditOrderBy         = " ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC"
	checklistAuditLimit           = " LIMIT $"
)

// appendChecklistAuditScopeAndPaging appends the entity-scope filter (if the
// caller isn't admin-override and has a restricted entity list), the shared
// ORDER BY, and the LIMIT clause — the tail shared by all three AuditChecklistItem
// branches (item_id / cycle_id / unfiltered) so they can't drift out of sync.
func appendChecklistAuditScopeAndPaging(q string, args []interface{}, scope ctxutil.RequestScope, limit int) (string, []interface{}) {
	if !scope.IsAdminOverride && len(scope.EntityIDs) > 0 {
		args = append(args, scope.EntityIDs)
		q += checklistAuditEntityFilter + strconv.Itoa(len(args)) + checklistAuditTextArraySuffix
	}
	q += checklistAuditOrderBy
	args = append(args, limit)
	q += checklistAuditLimit + strconv.Itoa(len(args))
	return q, args
}

// AuditChecklistItem handles POST /investment/fd-closing/checklist/audit — a
// standalone paginated audit-trail endpoint pairing with the shared frontend
// AuditTrailSection component's `endpoint` prop. item_id is optional:
// omitted, it returns the latest rows across every checklist item the caller
// can see (entity-scoped), same shape as cycle/audit.go's AuditCycle.
func AuditChecklistItem(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ItemID  string `json:"item_id"`
			CycleID string `json:"cycle_id"`
			Limit   int    `json:"limit"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req) // body is optional for audit
		limit := req.Limit
		if limit <= 0 || limit > 1000 {
			limit = 200
		}

		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)

		var q string
		var args []interface{}
		if itemID := strings.TrimSpace(req.ItemID); itemID != "" {
			q = checklistAuditSelect + `
				JOIN investment.fd_closing_cycle c ON c.cycle_id = i.cycle_id
				WHERE a.item_id = $1`
			args = append(args, itemID)
			q, args = appendChecklistAuditScopeAndPaging(q, args, scope, limit)
		} else if cycleID := strings.TrimSpace(req.CycleID); cycleID != "" {
			q = checklistAuditSelect + `
				JOIN investment.fd_closing_cycle c ON c.cycle_id = i.cycle_id
				WHERE i.cycle_id = $1`
			args = append(args, cycleID)
			q, args = appendChecklistAuditScopeAndPaging(q, args, scope, limit)
		} else {
			q = checklistAuditSelect + `
				JOIN investment.fd_closing_cycle c ON c.cycle_id = i.cycle_id
				WHERE 1=1`
			q, args = appendChecklistAuditScopeAndPaging(q, args, scope, limit)
		}

		rows, err := pool.Query(ctx, q, args...)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingChecklist] AuditChecklistItem query: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		defer rows.Close()

		out, err := scanRowsToMaps(rows)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingChecklist] AuditChecklistItem row error: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{"audit_logs": out})
		api.LogInfo("[FDClosingChecklist] AuditChecklistItem: %d records", len(out))
	}
}
