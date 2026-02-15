package bankstatement

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/jobs"
	"encoding/json"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ManualCategorizationTriggerHandler allows admins to manually trigger the auto-categorization job
func ManualCategorizationTriggerHandler(pgxPool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var body struct {
			UserID    string `json:"user_id"`
			BatchSize int    `json:"batch_size,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		if body.UserID == "" {
			http.Error(w, constants.ErrMissingUserID, http.StatusBadRequest)
			return
		}

		// Validate user session (basic check)
		ctx := r.Context()
		if ctxUID := api.GetUserIDFromCtx(ctx); ctxUID != "" && ctxUID != body.UserID {
			http.Error(w, constants.ErrInvalidSessionCapitalized, http.StatusForbidden)
			return
		}

		// Set default batch size
		batchSize := body.BatchSize
		if batchSize <= 0 {
			batchSize = 500
		}
		if batchSize > 5000 {
			batchSize = 5000 // Cap at 5000 for safety
		}

		// Trigger the categorization job
		err := jobs.ProcessUncategorizedTransactions(pgxPool, batchSize)
		if err != nil {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "Categorization job failed: " + err.Error(),
			})
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "Categorization job completed successfully",
		})
	})
}
