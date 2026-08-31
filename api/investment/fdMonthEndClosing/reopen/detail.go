package reopen

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

// DetailReopenRequest handles POST /investment/fd-closing/reopen/detail.
// Params travel in the JSON body per this repo's all-POST convention.
func DetailReopenRequest(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RequestID string `json:"request_id"`
			// UserID is the optional viewer id, used to compute
			// viewer_can_act/viewer_active_eye_id on the approval_workflow payload.
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		req.RequestID = strings.TrimSpace(req.RequestID)
		if req.RequestID == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "request_id is required")
			return
		}

		ctx := r.Context()

		rows, err := pool.Query(ctx, listQuery+" AND rr.request_id = $2", moduleCode, req.RequestID)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingReopen] DetailReopenRequest query: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
			return
		}
		out, scanErr := scanRowsToMaps(rows)
		rows.Close()
		if scanErr != nil {
			api.LogErrorForResponse(w, "[FDClosingReopen] DetailReopenRequest row error: %v", scanErr)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
			return
		}
		if len(out) == 0 {
			fdclosingcommon.RespondError(w, http.StatusNotFound, "Reopen request not found")
			return
		}
		reopenRequest := out[0]

		entityID, _ := reopenRequest["entity_id"].(string)
		scope := ctxutil.FromContext(ctx)
		if !scope.HasEntityAccess(entityID) {
			fdclosingcommon.RespondError(w, http.StatusForbidden,
				"Entity ID '"+entityID+"' is not within your authorized access scope.")
			return
		}

		var approvalWorkflow interface{}
		var instanceID string
		_ = pool.QueryRow(ctx, `
			SELECT instance_id FROM uam.approval_instance
			WHERE record_id = $1 AND module_code = $2 AND is_deleted = false
			ORDER BY submitted_at DESC LIMIT 1`,
			req.RequestID, moduleCode,
		).Scan(&instanceID)
		if instanceID != "" {
			richDetail, richErr := approvalengine.GetRichInstanceDetail(ctx, pool, instanceID, req.UserID)
			if richErr != nil {
				api.LogError("[FDClosingReopen] GetRichInstanceDetail failed for instance=%s request=%s: %v", instanceID, req.RequestID, richErr)
			} else {
				approvalWorkflow = richDetail
			}
		}

		fdclosingcommon.RespondSuccess(w, "Success", map[string]interface{}{
			"reopen_request":    reopenRequest,
			"approval_workflow": approvalWorkflow,
		})
		api.LogInfo("[FDClosingReopen] DetailReopenRequest: request_id=%s", req.RequestID)
	}
}
