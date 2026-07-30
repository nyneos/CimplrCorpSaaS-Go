package policy

import (
	"net/http"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type dynamicListRefItem struct {
	RefCode     string `json:"ref_code"`
	Label       string `json:"label"`
	Description string `json:"description"`
}

// HandleDynamicListRegistryList returns active, non-deleted Dynamic list refs
// for the authoring dropdown (BRD §5.5.2). Discovery metadata only — no SQL.
func HandleDynamicListRegistryList(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		rows, err := pool.Query(r.Context(), `
			SELECT ref_code, label, COALESCE(description, '')
			  FROM policyengine_svc.dynamic_list_registry
			 WHERE is_deleted = false AND is_active = true
			 ORDER BY label, ref_code`)
		if err != nil {
			api.LogErrorForResponse(w, "dynamic-list-refs list: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list dynamic list refs", "DYNAMIC_LIST_REFS_LIST_FAILED")
			return
		}
		defer rows.Close()

		out := make([]dynamicListRefItem, 0)
		for rows.Next() {
			var it dynamicListRefItem
			if err := rows.Scan(&it.RefCode, &it.Label, &it.Description); err != nil {
				api.LogErrorForResponse(w, "dynamic-list-refs list scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list dynamic list refs", "DYNAMIC_LIST_REFS_LIST_FAILED")
				return
			}
			out = append(out, it)
		}
		api.RespondEnvelopeSuccess(w, "Dynamic list refs fetched", out)
	}
}
