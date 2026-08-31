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

// approvedActiveStatuses is the "eligible to lock/reopen" status set for the
// generic cycle picker used by downstream screens (Lock Request, Reopen
// Request, Evidence Pack forms, etc.). Kept as its own named constant, easy
// to find/adjust, per the handler spec's explicit warning: different handoff
// points may need different status sets — do not silently reuse this one for
// a different picker without checking against the relevant mock screen first.
var approvedActiveStatuses = []string{"IN_PROGRESS", "AWAITING_APPROVAL"}

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
