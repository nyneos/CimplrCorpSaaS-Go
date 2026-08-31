package cycle

import (
	"encoding/json"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DetailCycle handles POST /investment/fd-closing/cycle/detail. Params go in
// the JSON body (this repo's all-POST convention), not a query string —
// unlike fdBookingWorkbench's GetBookingDetail, which predates that
// convention being applied consistently to new modules.
func DetailCycle(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CycleID string `json:"cycle_id"`
			// UserID is the optional viewer id, used to compute
			// viewer_can_act/viewer_active_eye_id on the approval_workflow payload.
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		req.CycleID = strings.TrimSpace(req.CycleID)
		if req.CycleID == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "cycle_id is required")
			return
		}

		ctx := r.Context()

		var cycle map[string]interface{}
		{
			rows, err := pool.Query(ctx, `
				SELECT
					cycle_id, close_type, entity_id, entity_name,
					COALESCE(bank_id,'') AS bank_id, COALESCE(bank_name,'') AS bank_name,
					COALESCE(currency_code,'') AS currency_code,
					financial_period,
					TO_CHAR(period_start,'YYYY-MM-DD') AS period_start,
					TO_CHAR(period_end,'YYYY-MM-DD') AS period_end,
					include_matured, source, status,
					fd_count, readiness_score, blocker_count, eligibility,
					initiated_by, TO_CHAR(initiated_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS initiated_at,
					is_deleted, created_by, TO_CHAR(created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created_at
				FROM investment.fd_closing_cycle
				WHERE cycle_id = $1`,
				req.CycleID,
			)
			if err != nil {
				api.LogErrorForResponse(w, "[FDClosingCycle] DetailCycle query: %v", err)
				fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
				return
			}
			rowsOut, scanErr := scanRowsToMaps(rows)
			rows.Close()
			if scanErr != nil {
				api.LogErrorForResponse(w, "[FDClosingCycle] DetailCycle row error: %v", scanErr)
				fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
				return
			}
			if len(rowsOut) == 0 {
				fdclosingcommon.RespondError(w, http.StatusNotFound, "Cycle not found")
				return
			}
			cycle = rowsOut[0]
		}

		entityID, _ := cycle["entity_id"].(string)
		scope := ctxutil.FromContext(ctx)
		if !scope.HasEntityAccess(entityID) {
			fdclosingcommon.RespondError(w, http.StatusForbidden,
				"Entity ID '"+entityID+"' is not within your authorized access scope.")
			return
		}

		auditRows, err := pool.Query(ctx, auditHistorySelect+`
			WHERE a.cycle_id = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp),COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`,
			req.CycleID,
		)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] DetailCycle audit query: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		auditHistory, err := scanRowsToMaps(auditRows)
		auditRows.Close()
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] DetailCycle audit row error: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
			return
		}

		var approvalWorkflow interface{}
		var instanceID string
		_ = pool.QueryRow(ctx, `
			SELECT instance_id FROM uam.approval_instance
			WHERE record_id = $1 AND module_code = $2 AND is_deleted = false
			ORDER BY submitted_at DESC LIMIT 1`,
			req.CycleID, moduleCode,
		).Scan(&instanceID)
		if instanceID != "" {
			richDetail, richErr := approvalengine.GetRichInstanceDetail(ctx, pool, instanceID, req.UserID)
			if richErr != nil {
				api.LogError("[FDClosingCycle] GetRichInstanceDetail failed for instance=%s cycle=%s: %v", instanceID, req.CycleID, richErr)
			} else {
				approvalWorkflow = richDetail
			}
		}

		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{
			"cycle":             cycle,
			"audit_history":     auditHistory,
			"approval_workflow": approvalWorkflow,
		})
		api.LogInfo("[FDClosingCycle] DetailCycle: cycle_id=%s", req.CycleID)
	}
}
