package exposures

import (
	"encoding/json"
	"net/http"

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Helper: send JSON error response
// func respondWithError(w http.ResponseWriter, status int, errMsg string) {
// 	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
// 	w.WriteHeader(status)
// 	json.NewEncoder(w).Encode(map[string]interface{}{
// 		constants.ValueSuccess: false,
// 		constants.ValueError:   errMsg,
// 	})
// }

// Handler: Aggregate hedging proposals for accessible business units
func GetHedgingProposalsAggregated(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrPleaseLogin)
			return
		}

		// Get business units from context (set by middleware)
		scope := ctxutil.FromContext(ctx)
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		// Ensure hedging_proposal rows exist for approved bucketing exposures
		_, _ = pool.Exec(ctx, `INSERT INTO hedging_proposal (exposure_header_id)
			SELECT h.exposure_header_id
			FROM exposure_headers h
			JOIN exposure_bucketing b ON h.exposure_header_id = b.exposure_header_id
			WHERE h.entity = ANY($1)
			  AND COALESCE(h.is_deleted, false) = false
			  AND lower(COALESCE(b.status_bucketing, '')) = 'approved'
			  AND h.exposure_header_id NOT IN (
				SELECT exposure_header_id FROM hedging_proposal
			  )`, buNames)

		// Aggregate hedging proposals from approved bucketing only
		query := `
			SELECT 
				h.entity AS business_unit,
				h.currency,
				h.exposure_type,
				ARRAY_AGG(DISTINCT h.exposure_header_id::text) AS contributing_header_ids,
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
				COALESCE(NULLIF(MAX(hp.status_hedging), ''), 'pending') AS status
			FROM exposure_headers h
			JOIN exposure_bucketing b
			  ON h.exposure_header_id = b.exposure_header_id
			 AND lower(COALESCE(b.status_bucketing, '')) = 'approved'
			LEFT JOIN hedging_proposal hp ON h.exposure_header_id = hp.exposure_header_id
			WHERE h.entity = ANY($1)
			  AND COALESCE(h.is_deleted, false) = false
			  AND EXISTS (
				SELECT 1 FROM exposure_line_items l WHERE l.exposure_header_id = h.exposure_header_id
			  )
			GROUP BY h.entity, h.currency, h.exposure_type
		`
		rows, err := pool.Query(ctx, query, buNames)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to aggregate proposals")
			return
		}
		defer rows.Close()
		cols := pgxColumnNames(rows)
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
				rowMap[col] = parseDBValue(col, vals[i])
			}
			proposals = append(proposals, rowMap)
		}
		respondWithSuccess(w, http.StatusOK, "Success", map[string]interface{}{
			"proposals": proposals,
		})
	}
}
