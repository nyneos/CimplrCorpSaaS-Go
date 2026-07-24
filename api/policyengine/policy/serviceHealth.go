package policy

import (
	"net/http"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/internal/services/policysvc"

	"github.com/jackc/pgx/v5/pgxpool"
)

// HandleServiceHealth probes the standalone policy check relay.
func HandleServiceHealth(_ *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		client := policysvc.NewFromEnv()
		if err := client.Health(r.Context()); err != nil {
			api.LogErrorForResponse(w, "policy relay health: %v", err)
			api.RespondEnvelopeSuccess(w, "Policy check service unreachable", map[string]interface{}{
				"healthy": false,
				"message": "Policy check service is unreachable. Evaluate / test will fail until it is back.",
			})
			return
		}
		api.RespondEnvelopeSuccess(w, "Policy check service healthy", map[string]interface{}{
			"healthy": true,
			"message": "",
		})
	}
}
