package cdm

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

// HandleResolve maps a user_alias, extra alias, or stable name → canonical evaluation name.
// Additive helper so engines can accept convenient names without renaming deployed vars.
func HandleResolve(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req struct {
			NameOrAlias string `json:"name_or_alias"`
		}
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		key := strings.TrimSpace(req.NameOrAlias)
		if key == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "name_or_alias is required", "VALIDATION_ERROR")
			return
		}

		var name, label, canonicalRef, userAlias string
		err := pool.QueryRow(r.Context(), `
			SELECT name, label, COALESCE(canonical_ref,''), COALESCE(user_alias,'')
			FROM policyengine_svc.cdm_variable
			WHERE is_deleted = false AND status = 'Active'
			  AND (name = $1 OR user_alias = $1 OR label = $1)
			ORDER BY
				CASE
					WHEN label = $1 THEN 0
					WHEN user_alias = $1 THEN 1
					ELSE 2
				END
			LIMIT 1`, key).Scan(&name, &label, &canonicalRef, &userAlias)
		if err != nil {
			err = pool.QueryRow(r.Context(), `
				SELECT c.name, c.label, COALESCE(c.canonical_ref,''), COALESCE(c.user_alias,'')
				FROM policyengine_svc.cdm_variable_alias a
				JOIN policyengine_svc.cdm_variable c ON c.variable_id = a.variable_id
				WHERE a.is_deleted = false AND c.is_deleted = false AND c.status = 'Active'
				  AND a.alias_name = $1
				LIMIT 1`, key).Scan(&name, &label, &canonicalRef, &userAlias)
		}
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusNotFound, "CDM variable not found for name_or_alias", "CDM_RESOLVE_NOT_FOUND")
			return
		}
		api.RespondEnvelopeSuccess(w, "CDM variable resolved", map[string]string{
			"name":          name,
			"label":         label,
			"canonical_ref": canonicalRef,
			"user_alias":    userAlias,
			"resolved_from": key,
		})
	}
}
