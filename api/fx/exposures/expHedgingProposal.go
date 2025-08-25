package exposures

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"encoding/json"
	"net/http"

	"github.com/lib/pq"
)

// Helper: send JSON error response
// func respondWithError(w http.ResponseWriter, status int, errMsg string) {
// 	w.Header().Set("Content-Type", "application/json")
// 	w.WriteHeader(status)
// 	json.NewEncoder(w).Encode(map[string]interface{}{
// 		"success": false,
// 		"error":   errMsg,
// 	})
// }

// Handler: Aggregate hedging proposals for accessible business units
func GetHedgingProposalsAggregated(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "Please login to continue.")
			return
		}

		// Get business units from context (set by middleware)
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}

		// Ensure all exposure_header_id are present in hedging_proposal
		_, _ = db.Exec(`INSERT INTO hedging_proposal (exposure_header_id)
			SELECT exposure_header_id
			FROM exposure_headers
			WHERE entity = ANY($1)
			  AND exposure_header_id NOT IN (
				SELECT exposure_header_id FROM hedging_proposal
			  )`, pq.Array(buNames))

		// Aggregate hedging proposals
		query := `
			SELECT 
				h.entity AS business_unit,
				h.currency,
				h.exposure_type,
				ARRAY_AGG(h.exposure_header_id) AS contributing_header_ids,
				SUM(COALESCE(b.month_1, 0)) AS hedge_month1,
				SUM(COALESCE(b.month_2, 0)) AS hedge_month2,
				SUM(COALESCE(b.month_3, 0)) AS hedge_month3,
				SUM(COALESCE(b.month_4, 0)) AS hedge_month4,
				SUM(COALESCE(b.month_4_6, 0)) AS hedge_month4to6,
				SUM(COALESCE(b.month_6plus, 0)) AS hedge_month6plus,
				SUM(COALESCE(b.old_month1, 0)) AS old_hedge_month1,
				SUM(COALESCE(b.old_month2, 0)) AS old_hedge_month2,
				SUM(COALESCE(b.old_month3, 0)) AS old_hedge_month3,
				SUM(COALESCE(b.old_month4, 0)) AS old_hedge_month4,
				SUM(COALESCE(b.old_month4to6, 0)) AS old_hedge_month4to6,
				SUM(COALESCE(b.old_month6plus, 0)) AS old_hedge_month6plus,
				MAX(hp.comments) AS comments,
				MAX(hp.status_hedging) AS status
			FROM exposure_headers h
			JOIN exposure_bucketing b ON h.exposure_header_id = b.exposure_header_id AND (b.status_bucketing = 'approved' OR b.status_bucketing = 'Approved')
			JOIN exposure_line_items l ON h.exposure_header_id = l.exposure_header_id
			LEFT JOIN hedging_proposal hp ON h.exposure_header_id = hp.exposure_header_id
			WHERE h.entity = ANY($1)
			GROUP BY h.entity, h.currency, h.exposure_type
		`
		rows, err := db.Query(query, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to aggregate proposals")
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		proposals := []map[string]interface{}{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := rows.Scan(valPtrs...); err != nil {
				continue
			}
			rowMap := map[string]interface{}{}
			for i, col := range cols {
				rowMap[col] = vals[i]
			}
			proposals = append(proposals, rowMap)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"proposals": proposals,
		})
	}
}