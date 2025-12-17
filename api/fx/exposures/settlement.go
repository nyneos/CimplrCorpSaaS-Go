package exposures

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"encoding/json"
	"net/http"

	"CimplrCorpSaas/api/constants"

	"github.com/lib/pq"
)

// Handler for settlement

// FilterForwardBookingsForSettlement handles the filtering of forward bookings for settlement
func FilterForwardBookingsForSettlement(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID            string   `json:"user_id"`
			ExposureHeaderIDs []string `json:"exposure_header_ids"`
			Entity            string   `json:"entity"`
			Currency          string   `json:"currency"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.ExposureHeaderIDs) == 0 || req.Entity == "" || req.Currency == "" || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "user_id, exposure_header_ids (array), entity, and currency are required")
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		query := `
			SELECT 
				fb.internal_reference_id AS "Forward Ref",
				COALESCE((SELECT running_open_amount FROM forward_booking_ledger fbl WHERE fbl.booking_id = fb.system_transaction_id ORDER BY ledger_sequence DESC LIMIT 1), fb.booking_amount) AS "Outstanding Amount",
				fb.spot_rate AS "Spot",
				fb.total_rate AS "Fwd",
				fb.bank_margin AS "Margin",
				fb.counterparty_dealer AS "Bank Name",
				fb.maturity_date AS "Maturity"
			FROM exposure_hedge_links ehl
			JOIN forward_bookings fb ON ehl.booking_id = fb.system_transaction_id
			WHERE ehl.exposure_header_id = ANY($1)
				AND fb.quote_currency = $2
				AND (
					fb.entity_level_0 = $3
					OR fb.entity_level_1 = $3
					OR fb.entity_level_2 = $3
					OR fb.entity_level_3 = $3
				)
				AND fb.status = 'Confirmed'
				AND (
					fb.entity_level_0 = ANY($4)
					OR fb.entity_level_1 = ANY($4)
					OR fb.entity_level_2 = ANY($4)
					OR fb.entity_level_3 = ANY($4)
				)
		`
		rows, err := db.Query(query, pq.Array(req.ExposureHeaderIDs), req.Currency, req.Entity, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch forward bookings for settlement")
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		data := []map[string]interface{}{}
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
			data = append(data, rowMap)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "data": data})
	}
}

// Handler: GetForwardBookingsByEntityAndCurrency
func GetForwardBookingsByEntityAndCurrency(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			Entity   string `json:"entity"`
			Currency string `json:"currency"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Entity == "" || req.Currency == "" || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "user_id, entity, and currency are required")
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		query := `
			SELECT 
				fb.internal_reference_id AS "Forward Ref",
				COALESCE((SELECT running_open_amount FROM forward_booking_ledger fbl WHERE fbl.booking_id = fb.system_transaction_id ORDER BY ledger_sequence DESC LIMIT 1), fb.booking_amount) AS "Outstanding Amount",
				fb.spot_rate AS "Spot",
				fb.total_rate AS "Fwd",
				fb.bank_margin AS "Margin",
				fb.counterparty_dealer AS "Bank Name",
				fb.maturity_date AS "Maturity"
			FROM forward_bookings fb
			WHERE fb.quote_currency = $1
				AND (
					fb.entity_level_0 = $2
					OR fb.entity_level_1 = $2
					OR fb.entity_level_2 = $2
					OR fb.entity_level_3 = $2
				)
				AND fb.status = 'Confirmed'
				AND (
					fb.entity_level_0 = ANY($3)
					OR fb.entity_level_1 = ANY($3)
					OR fb.entity_level_2 = ANY($3)
					OR fb.entity_level_3 = ANY($3)
				)
		`
		rows, err := db.Query(query, req.Currency, req.Entity, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch forward bookings")
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		data := []map[string]interface{}{}
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
			data = append(data, rowMap)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "data": data})
	}
}
