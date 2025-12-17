package buCurrExpDash

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
)

func respondWithError(w http.ResponseWriter, status int, errMsg string) {
	log.Println("[ERROR]", errMsg)
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		constants.ValueSuccess: false,
		constants.ValueError:   errMsg,
	})
}

// Handler: GetDashboard
func GetDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := `
		WITH exposure_summary AS (
		  SELECT 
		      e.entity AS bu,
		      e.currency,
		      SUM(CASE WHEN lower(e.exposure_type) = 'debitors'  THEN ABS(e.total_original_amount) ELSE 0 END) AS debitors,
		      SUM(CASE WHEN lower(e.exposure_type) = 'creditors' THEN ABS(e.total_original_amount) ELSE 0 END) AS creditors,
		      SUM(CASE WHEN lower(e.exposure_type) = 'LC'       THEN ABS(e.total_original_amount) ELSE 0 END) AS lc,
		      SUM(CASE WHEN lower(e.exposure_type) = 'grn'      THEN ABS(e.total_original_amount) ELSE 0 END) AS grn,
		      SUM(
		  CASE WHEN lower(e.exposure_type) IN ('debitors','creditors','LC','grn')
		       THEN ABS(e.total_original_amount) ELSE 0 END
		) AS total_payable_exposure
		  FROM exposure_headers e
		  GROUP BY e.entity, e.currency
		),
		cover_summary AS (
		  SELECT 
		      e.entity AS bu,
		      e.currency,
		      COALESCE(SUM(CASE WHEN fb.order_type = 'Sell' THEN ABS(l.hedged_amount) ELSE 0 END),0) AS cover_taken_export,
		      COALESCE(SUM(CASE WHEN fb.order_type = 'Buy'  THEN ABS(l.hedged_amount) ELSE 0 END),0) AS cover_taken_import
		  FROM exposure_headers e
		  LEFT JOIN exposure_hedge_links l 
		      ON e.exposure_header_id = l.exposure_header_id
		  LEFT JOIN forward_bookings fb
		      ON l.booking_id = fb.system_transaction_id
		  GROUP BY e.entity, e.currency
		)
		SELECT 
		    es.bu,
		    es.currency,
		    es.debitors,
		    es.creditors,
		    es.lc,
		    es.grn,
		    es.total_payable_exposure,
		    cs.cover_taken_export,
		    cs.cover_taken_import,
		    (es.total_payable_exposure - cs.cover_taken_export) AS outstanding_cover_export,
		    (es.total_payable_exposure - cs.cover_taken_import) AS outstanding_cover_import
		FROM exposure_summary es
		LEFT JOIN cover_summary cs 
		    ON es.bu = cs.bu AND es.currency = cs.currency;
		`
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: constants.ErrUserIDRequired})
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusInternalServerError, "Business units not found in context")
			return
		}
		rows, err := db.Query(query)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: "Server error"})
			return
		}
		defer rows.Close()
		var dashboards []map[string]interface{}
		idx := 1
		for rows.Next() {
			var bu, currency string
			var debitors, creditors, lc, grn, totalPayableExposure, coverTakenExport, coverTakenImport, outstandingCoverExport, outstandingCoverImport sql.NullFloat64
			if err := rows.Scan(&bu, &currency, &debitors, &creditors, &lc, &grn, &totalPayableExposure, &coverTakenExport, &coverTakenImport, &outstandingCoverExport, &outstandingCoverImport); err != nil {
				continue
			}
			rowMap := map[string]interface{}{
				"id":                       idx,
				"bu":                       bu,
				"currency":                 currency,
				"debitors":                 debitors.Float64,
				"creditors":                creditors.Float64,
				"lc":                       lc.Float64,
				"grn":                      grn.Float64,
				"total_payable_exposure":   totalPayableExposure.Float64,
				"cover_taken_export":       coverTakenExport.Float64,
				"cover_taken_import":       coverTakenImport.Float64,
				"outstanding_cover_export": outstandingCoverExport.Float64,
				"outstanding_cover_import": outstandingCoverImport.Float64,
			}
			// Only include if bu (entity) is in allowed buNames
			if bu, ok := rowMap["bu"].(string); ok && containsString(buNames, bu) {
				dashboards = append(dashboards, rowMap)
				idx++
			}
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]any{"dashboards": dashboards})
	}
}

// Helper: containsString
func containsString(arr []string, s string) bool {
	for _, v := range arr {
		if v == s {
			return true
		}
	}
	return false
}
