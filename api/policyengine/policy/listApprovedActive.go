package policy

import (
	"net/http"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type approvedActiveItem struct {
	PolicyID      string   `json:"policy_id"`
	Code          string   `json:"code"`
	Name          string   `json:"name"`
	Category      string   `json:"category"`
	RuleType      string   `json:"rule_type"`
	TriggerEvents []string `json:"trigger_events"`
	Modules       []string `json:"modules"`
}

// HandleListApprovedActive returns checker-approved, Active, non-deleted policies —
// this is what /policies/check's implicit policy loader also filters on.
func HandleListApprovedActive(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		rows, err := pool.Query(r.Context(), `
			SELECT p.policy_id::text, p.code, p.name, p.category, p.rule_type,
			       COALESCE(t.trigger_events, ARRAY[]::varchar[]) AS trigger_events,
			       COALESCE(m.modules, ARRAY[]::varchar[]) AS modules
			FROM policyengine_svc.policy_master p
			LEFT JOIN LATERAL (
				SELECT array_agg(pt.event_code ORDER BY pt.event_code) AS trigger_events
				FROM policyengine_svc.policy_trigger pt
				WHERE pt.policy_id = p.policy_id AND pt.is_deleted = false
			) t ON true
			LEFT JOIN LATERAL (
				SELECT array_agg(pmod.module_code ORDER BY pmod.module_code) AS modules
				FROM policyengine_svc.policy_module pmod
				WHERE pmod.policy_id = p.policy_id AND pmod.is_deleted = false
			) m ON true
			WHERE p.is_deleted = false AND p.status = 'Active' AND p.processing_status = 'APPROVED'
			` + common.PolicyListOrderBy)
		if err != nil {
			api.LogErrorForResponse(w, "policy list-approved-active: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list policies", "POLICY_LIST_FAILED")
			return
		}
		defer rows.Close()

		out := make([]approvedActiveItem, 0)
		for rows.Next() {
			var it approvedActiveItem
			if err := rows.Scan(&it.PolicyID, &it.Code, &it.Name, &it.Category, &it.RuleType,
				&it.TriggerEvents, &it.Modules); err != nil {
				api.LogErrorForResponse(w, "policy list-approved-active scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list policies", "POLICY_LIST_FAILED")
				return
			}
			out = append(out, it)
		}
		api.RespondEnvelopeSuccess(w, "Approved active policies fetched", out)
	}
}
