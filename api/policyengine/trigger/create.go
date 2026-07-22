package trigger

import (
	"net/http"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

// HandleCreate rejects custom trigger creation. Event codes are a fixed system
// vocabulary: PRE_/POST_ × create|upload|edit|delete|approve|reject (plus a few
// orthogonal seeds). See database/2026-07-19/fixed_system_triggers.sql.
func HandleCreate(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		_ = pool
		api.RespondEnvelopeError(w, http.StatusForbidden,
			"trigger events are system-fixed (PRE_/POST_ create|upload|edit|delete|approve|reject). Creating custom triggers is disabled.",
			"TRIGGER_CREATE_DISABLED")
	}
}
