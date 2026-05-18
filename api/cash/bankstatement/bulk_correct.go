package bankstatement

// BulkCorrectHandler — POST /cash/smart-cat/bulk-correct
//
// Applies a user-chosen category to multiple transactions at once by
// calling the same applyCorrection logic used by the single-item
// ReviewActionHandler.  This is the "Apply" path for AI bulk suggestions.
//
// Request:
//
//	{
//	  "user_id": "...",
//	  "corrections": [
//	    { "transaction_ids": [447737, 447847], "category_id": "CAT-001" },
//	    ...
//	  ]
//	}
//
// Response:
//
//	{ "success": true, "applied": 42, "failed": 0 }

import (
	"encoding/json"
	"net/http"

	"CimplrCorpSaas/api/constants"
	middlewares "CimplrCorpSaas/api/middlewares"

	"github.com/jackc/pgx/v5/pgxpool"
)

type bulkCorrectionGroup struct {
	TransactionIDs []int64 `json:"transaction_ids"`
	CategoryID     string  `json:"category_id"`
}

func BulkCorrectHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			UserID      string                `json:"user_id"`
			Corrections []bulkCorrectionGroup `json:"corrections"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			writeErrJSON(w, constants.ErrMissingUserID, http.StatusBadRequest)
			return
		}
		if len(req.Corrections) == 0 {
			writeErrJSON(w, "corrections is empty", http.StatusBadRequest)
			return
		}

		correctedBy := req.UserID
		if s := middlewares.GetSessionFromContext(r.Context()); s != nil && s.Name != "" {
			correctedBy = s.Name
		}

		ctx := r.Context()
		applied, failed := 0, 0

		for _, group := range req.Corrections {
			if group.CategoryID == "" || group.CategoryID == "UNALLOCATED" {
				continue
			}
			for _, txnID := range group.TransactionIDs {
				if err := applyCorrection(ctx, pool, txnID, group.CategoryID, correctedBy); err != nil {
					failed++
				} else {
					applied++
				}
			}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"applied": applied,
			"failed":  failed,
		})
	})
}
