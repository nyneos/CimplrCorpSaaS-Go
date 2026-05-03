package bankstatement

import (
	apictx "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	cashjobs "CimplrCorpSaas/internal/jobs/cash"
	"encoding/json"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ManualCategorizationTriggerHandler allows admins to manually trigger the auto-categorization job
func ManualCategorizationTriggerHandler(pgxPool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			apictx.Error(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var body struct {
			UserID    string `json:"user_id"`
			BatchSize int    `json:"batch_size,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			apictx.Error(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if body.UserID == "" {
			apictx.Error(w, http.StatusBadRequest, constants.ErrMissingUserID)
			return
		}

		// Validate user session (basic check)
		ctx := r.Context()
		if ctxUID := apictx.GetUserIDFromCtx(ctx); ctxUID != "" && ctxUID != body.UserID {
			apictx.Error(w, http.StatusForbidden, constants.ErrInvalidSessionCapitalized)
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
		err := cashjobs.ProcessUncategorizedTransactions(pgxPool, batchSize)
		if err != nil {
			apictx.Error(w, http.StatusOK, "Categorization job failed: "+err.Error())
			return
		}

		apictx.Success(w, http.StatusOK, map[string]any{}, "Categorization job completed successfully")
	})
}

