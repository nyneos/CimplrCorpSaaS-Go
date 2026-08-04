package rules

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"
	dmsjobs "CimplrCorpSaas/internal/jobs/dms"

	"github.com/jackc/pgx/v5/pgxpool"
)

type runReq struct {
	RuleID  string `json:"rule_id"`
	ActorID string `json:"actor_id"`
}

// HandleRun manually triggers a rule synchronously — runs it inline and
// returns the generation_run id + outcome. Not a new entry in the standard
// CLAUDE.md route-action vocabulary (list/create/update/.../approve/reject) —
// it executes the rule rather than mutating it, which reads as a genuinely
// distinct action; flagged for confirmation before treating it as precedent
// for other modules.
func HandleRun(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req runReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.RuleID = strings.TrimSpace(req.RuleID)
		if req.RuleID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "rule_id is required", "VALIDATION_ERROR")
			return
		}
		actor := common.RequestActor(r, req.ActorID)

		runID, err := dmsjobs.RunGeneration(r.Context(), pool, req.RuleID, "MANUAL", actor)
		if err != nil {
			api.RespondEnvelopeFailureWithData(w, http.StatusUnprocessableEntity, err.Error(), "DMS_RULE_RUN_FAILED",
				map[string]interface{}{"run_id": runID})
			return
		}
		api.RespondEnvelopeSuccess(w, "Rule run completed", map[string]interface{}{"run_id": runID})
	}
}
