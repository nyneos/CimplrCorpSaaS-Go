package forwards

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"encoding/json"
	"net/http"

	"github.com/lib/pq"
)

// Handler: UpdateForwardBookingProcessingStatus
// func UpdateForwardBookingProcessingStatus(db *sql.DB) http.HandlerFunc {
// 	return func(w http.ResponseWriter, r *http.Request) {
// 		var req struct {
// 			UserID string `json:"user_id"`
// 			SystemTransactionID string `json:"system_transaction_id"`
// 			ProcessingStatus string `json:"processing_status"`
// 		}
// 		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
// 			w.WriteHeader(http.StatusBadRequest)
// 			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id required"})
// 			return
// 		}
// 		if req.SystemTransactionID == "" || (req.ProcessingStatus != "Approved" && req.ProcessingStatus != "Rejected") {
// 			w.WriteHeader(http.StatusBadRequest)
// 			json.NewEncoder(w).Encode(map[string]interface{}{"error": "system_transaction_id and valid processing_status (Approved/Rejected) required"})
// 			return
// 		}
// 		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
// 		if !ok || len(buNames) == 0 {
// 			w.WriteHeader(http.StatusForbidden)
// 			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No accessible business units found"})
// 			return
// 		}
// 		// Check if booking belongs to accessible business units
// 		var entityLevel0 string
// 		err := db.QueryRow(`SELECT entity_level_0 FROM forward_bookings WHERE system_transaction_id = $1`, req.SystemTransactionID).Scan(&entityLevel0)
// 		if err != nil {
// 			w.WriteHeader(http.StatusNotFound)
// 			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "No matching forward booking found"})
// 			return
// 		}
// 		found := false
// 		for _, bu := range buNames {
// 			if bu == entityLevel0 {
// 				found = true
// 				break
// 			}
// 		}
// 		if !found {
// 			w.WriteHeader(http.StatusForbidden)
// 			json.NewEncoder(w).Encode(map[string]interface{}{"error": "You do not have access to this business unit"})
// 			return
// 		}
// 		query := `UPDATE forward_bookings SET processing_status = $1 WHERE system_transaction_id = $2 RETURNING *`
// 		row := db.QueryRow(query, req.ProcessingStatus, req.SystemTransactionID)
// 		cols := []string{"internal_reference_id","entity_level_0","entity_level_1","entity_level_2","entity_level_3","local_currency","order_type","transaction_type","counterparty","mode_of_delivery","delivery_period","add_date","settlement_date","maturity_date","delivery_date","currency_pair","base_currency","quote_currency","booking_amount","value_type","actual_value_base_currency","spot_rate","forward_points","bank_margin","total_rate","value_quote_currency","intervening_rate_quote_to_local","value_local_currency","internal_dealer","counterparty_dealer","remarks","narration","transaction_timestamp","processing_status","system_transaction_id"}
// 		vals := make([]interface{}, len(cols))
// 		valPtrs := make([]interface{}, len(cols))
// 		for i := range vals {
// 			valPtrs[i] = &vals[i]
// 		}
// 		if err := row.Scan(valPtrs...); err != nil {
// 			w.WriteHeader(http.StatusNotFound)
// 			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "No matching forward booking found"})
// 			return
// 		}
// 		result := make(map[string]interface{})
// 		for i, col := range cols {
// 			result[col] = vals[i]
// 		}
// 		w.Header().Set("Content-Type", "application/json")
// 		w.WriteHeader(http.StatusOK)
// 		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": result})
// 	}
// }

// Handler: BulkUpdateForwardBookingProcessingStatus
func BulkUpdateForwardBookingProcessingStatus(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			SystemTransactionIDs []string `json:"system_transaction_ids"`
			ProcessingStatus string `json:"processing_status"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id required"})
			return
		}
		if len(req.SystemTransactionIDs) == 0 || (req.ProcessingStatus != "Approved" && req.ProcessingStatus != "Rejected") {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "system_transaction_ids (array) and valid processing_status (Approved/Rejected) required"})
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No accessible business units found"})
			return
		}
		// Find which records are delete-approval and accessible
		delRows, err := db.Query(`SELECT system_transaction_id, entity_level_0 FROM forward_bookings WHERE system_transaction_id = ANY($1) AND processing_status = 'Delete-approval'`, pq.Array(req.SystemTransactionIDs))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		var deletedIds []string
		for delRows.Next() {
			var id, entityLevel0 string
			if err := delRows.Scan(&id, &entityLevel0); err == nil {
				for _, bu := range buNames {
					if bu == entityLevel0 {
						deletedIds = append(deletedIds, id)
						break
					}
				}
			}
		}
		delRows.Close()
		// Delete them
		if len(deletedIds) > 0 {
			_, err := db.Exec(`DELETE FROM forward_bookings WHERE system_transaction_id = ANY($1) AND processing_status = 'Delete-approval'`, pq.Array(deletedIds))
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
				return
			}
		}
		// The rest are eligible for update
		updateIds := []string{}
		for _, id := range req.SystemTransactionIDs {
			found := false
			for _, delId := range deletedIds {
				if id == delId {
					found = true
					break
				}
			}
			if !found {
				updateIds = append(updateIds, id)
			}
		}
		var updatedRows []map[string]interface{}
		if len(updateIds) > 0 {
			// Only update those belonging to accessible business units
			rows, err := db.Query(`SELECT system_transaction_id, entity_level_0 FROM forward_bookings WHERE system_transaction_id = ANY($1)`, pq.Array(updateIds))
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
				return
			}
			var eligibleIds []string
			var eligibleMap = make(map[string]bool)
			for rows.Next() {
				var id, entityLevel0 string
				if err := rows.Scan(&id, &entityLevel0); err == nil {
					for _, bu := range buNames {
						if bu == entityLevel0 {
							eligibleIds = append(eligibleIds, id)
							eligibleMap[id] = true
							break
						}
					}
				}
			}
			rows.Close()
			if len(eligibleIds) > 0 {
				var resultRows *sql.Rows
				if req.ProcessingStatus == "Approved" {
					resultRows, err = db.Query(`UPDATE forward_bookings SET processing_status = $1, status = 'Confirmed' WHERE system_transaction_id = ANY($2) RETURNING *`, req.ProcessingStatus, pq.Array(eligibleIds))
				} else {
					resultRows, err = db.Query(`UPDATE forward_bookings SET processing_status = $1 WHERE system_transaction_id = ANY($2) RETURNING *`, req.ProcessingStatus, pq.Array(eligibleIds))
				}
				if err == nil {
					cols, _ := resultRows.Columns()
					for resultRows.Next() {
						vals := make([]interface{}, len(cols))
						valPtrs := make([]interface{}, len(cols))
						for i := range vals {
							valPtrs[i] = &vals[i]
						}
						if err := resultRows.Scan(valPtrs...); err == nil {
							rowMap := make(map[string]interface{})
							for i, col := range cols {
								rowMap[col] = vals[i]
							}
							updatedRows = append(updatedRows, rowMap)
						}
					}
					resultRows.Close()
				}
			}
		}
		if len(updatedRows) > 0 || len(deletedIds) > 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": updatedRows, "deleted": deletedIds})
		} else {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "No matching forward bookings found"})
		}
	}
}

// Handler: BulkDeleteForwardBookings
func BulkDeleteForwardBookings(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			SystemTransactionIDs []string `json:"system_transaction_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id required"})
			return
		}
		if len(req.SystemTransactionIDs) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "system_transaction_ids (array) required"})
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No accessible business units found"})
			return
		}
		// Only update those belonging to accessible business units
		rows, err := db.Query(`SELECT system_transaction_id, entity_level_0 FROM forward_bookings WHERE system_transaction_id = ANY($1)`, pq.Array(req.SystemTransactionIDs))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		var eligibleIds []string
		for rows.Next() {
			var id, entityLevel0 string
			if err := rows.Scan(&id, &entityLevel0); err == nil {
				for _, bu := range buNames {
					if bu == entityLevel0 {
						eligibleIds = append(eligibleIds, id)
						break
					}
				}
			}
		}
		rows.Close()
		if len(eligibleIds) == 0 {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "No matching forward bookings found"})
			return
		}
		updateQuery := `UPDATE forward_bookings SET processing_status = 'Delete-approval' WHERE system_transaction_id = ANY($1) RETURNING *`
		resultRows, err := db.Query(updateQuery, pq.Array(eligibleIds))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		cols, _ := resultRows.Columns()
		var updated []map[string]interface{}
		for resultRows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := resultRows.Scan(valPtrs...); err == nil {
				rowMap := make(map[string]interface{})
				for i, col := range cols {
					rowMap[col] = vals[i]
				}
				updated = append(updated, rowMap)
			}
		}
		resultRows.Close()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": updated})
	}
}


// Handler: AddForwardConfirmationManualEntry
func AddForwardConfirmationManualEntry(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID               string `json:"user_id"`
			InternalReferenceID  string `json:"internal_reference_id"`
			EntityLevel0         string `json:"entity_level_0"`
			BankTransactionID    string `json:"bank_transaction_id"`
			SwiftUniqueID        string `json:"swift_unique_id"`
			BankConfirmationDate string `json:"bank_confirmation_date"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id required"})
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No accessible business units found"})
			return
		}
		found := false
		for _, bu := range buNames {
			if bu == req.EntityLevel0 {
				found = true
				break
			}
		}
		if !found {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "You do not have access to this business unit"})
			return
		}
		// Convert empty string date fields to nil
		bankConfirmationDate := req.BankConfirmationDate
		if bankConfirmationDate == "" {
			bankConfirmationDate = "1970-01-01" // or set to nil if you want NULL
		}
		updateQuery := `UPDATE forward_bookings SET
		       status = 'Confirmed',
		       bank_transaction_id = $1,
		       swift_unique_id = $2,
		       bank_confirmation_date = $3,
		       processing_status = 'pending'
	       WHERE internal_reference_id = $4 AND status = 'Pending Confirmation' AND entity_level_0 = $5
	       RETURNING internal_reference_id, entity_level_0, bank_transaction_id, swift_unique_id, bank_confirmation_date, status, processing_status`
		var bankConfirmationDateVal interface{}
		if bankConfirmationDate == "" {
			bankConfirmationDateVal = nil
		} else {
			bankConfirmationDateVal = bankConfirmationDate
		}
		updateValues := []interface{}{
			req.BankTransactionID,
			req.SwiftUniqueID,
			bankConfirmationDateVal,
			req.InternalReferenceID,
			req.EntityLevel0,
		}
		row := db.QueryRow(updateQuery, updateValues...)
		cols := []string{"internal_reference_id", "entity_level_0", "bank_transaction_id", "swift_unique_id", "bank_confirmation_date", "status", "processing_status"}
		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}
		if err := row.Scan(valPtrs...); err != nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "No matching record found or already confirmed"})
			return
		}
		result := make(map[string]interface{})
		for i, col := range cols {
			result[col] = vals[i]
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": result})
	}
}

