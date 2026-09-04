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

// approvedActiveStatuses is the "eligible for downstream work" status set for
// cycle pickers (Checklist Work, Accrual Completion, Lock, Evidence Pack, etc.).
// DRAFT is intentionally excluded. Callers must ALSO pass the approved-CREATE
// gate below — status alone is not enough (scope add can flip DRAFT→IN_PROGRESS
// before the cycle CREATE request is checker-approved).
var approvedActiveStatuses = []string{"IN_PROGRESS", "AWAITING_APPROVAL", "REOPENED"}

// ListApprovedActiveCycles handles POST /investment/fd-closing/cycle/list-approved-active.
func ListApprovedActiveCycles(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			EntityID string `json:"entity_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)

		q := strings.Replace(listWithAuditQuery, "$moduleCode$", "$1", 1)
		args := []interface{}{moduleCode}
		argIdx := 2

		q += " AND m.status = ANY($" + strconv.Itoa(argIdx) + "::text[])"
		args = append(args, approvedActiveStatuses)
		argIdx++

		// Gate: cycle CREATE must already be checker-approved. Pending CREATE
		// cycles must not appear in Checklist / Accrual / Receipt / Lock pickers.
		q += ` AND EXISTS (
			SELECT 1 FROM investment.fd_closing_cycle_audit ca
			WHERE ca.cycle_id = m.cycle_id
			  AND ca.action_type = 'CREATE'
			  AND ca.processing_status = 'APPROVED'
		)`

		if !scope.IsAdminOverride && len(scope.EntityIDs) > 0 {
			q += " AND m.entity_id = ANY($" + strconv.Itoa(argIdx) + "::text[])"
			args = append(args, scope.EntityIDs)
			argIdx++
		}
		if strings.TrimSpace(req.EntityID) != "" {
			q += " AND m.entity_id = $" + strconv.Itoa(argIdx)
			args = append(args, strings.TrimSpace(req.EntityID))
			argIdx++
		}
		q += " ORDER BY m.financial_period DESC, m.entity_name ASC"

		rows, err := pool.Query(ctx, q, args...)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] ListApprovedActiveCycles query: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		defer rows.Close()

		out, err := scanRowsToMaps(rows)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] ListApprovedActiveCycles row error: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{"rows": out})
		api.LogInfo("[FDClosingCycle] ListApprovedActiveCycles: %d rows", len(out))
	}
}
