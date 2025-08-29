package forwards

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/lib/pq"
)

func UpdateForwardBookingFields(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Accept system_transaction_id and fields in the JSON body
		var req struct {
			SystemTransactionID string                 `json:"system_transaction_id"`
			Fields              map[string]interface{} `json:"fields"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.SystemTransactionID == "" || len(req.Fields) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "system_transaction_id and at least one field to update must be provided in body"})
			return
		}
		// Check if booking exists
		var exists bool
		err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM forward_bookings WHERE system_transaction_id = $1)", req.SystemTransactionID).Scan(&exists)
		if err != nil || !exists {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No matching forward booking found"})
			return
		}
		// Get valid columns for forward_bookings
		colRows, err := db.Query(`SELECT column_name FROM information_schema.columns WHERE table_name = 'forward_bookings'`)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "Failed to fetch columns"})
			return
		}
		validCols := map[string]bool{}
		for colRows.Next() {
			var col string
			if err := colRows.Scan(&col); err == nil {
				validCols[col] = true
			}
		}
		colRows.Close()
		// Filter fields to only valid columns (ignore system_transaction_id)
		updateFields := map[string]interface{}{}
		for k, v := range req.Fields {
			if validCols[k] && k != "system_transaction_id" {
				updateFields[k] = v
			}
		}
		// Always set processing_status to 'pending'
		updateFields["processing_status"] = "pending"
		if len(updateFields) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No valid fields to update"})
			return
		}
		// Build dynamic SET clause
		keys := make([]string, 0, len(updateFields))
		values := make([]interface{}, 0, len(updateFields)+1)
		for k := range updateFields {
			keys = append(keys, k)
		}
		setClause := make([]string, len(keys))
		for i, k := range keys {
			setClause[i] = fmt.Sprintf("%s = $%d", k, i+1)
			values = append(values, updateFields[k])
		}
		values = append(values, req.SystemTransactionID)
		updateQuery := fmt.Sprintf("UPDATE forward_bookings SET %s WHERE system_transaction_id = $%d RETURNING *", strings.Join(setClause, ", "), len(values))
		row := db.QueryRow(updateQuery, values...)
		// Return all columns
		colRows2, err := db.Query(`SELECT column_name FROM information_schema.columns WHERE table_name = 'forward_bookings'`)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "Failed to fetch columns"})
			return
		}
		var allCols []string
		for colRows2.Next() {
			var col string
			if err := colRows2.Scan(&col); err == nil {
				allCols = append(allCols, col)
			}
		}
		colRows2.Close()
		vals := make([]interface{}, len(allCols))
		valPtrs := make([]interface{}, len(allCols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}
		if err := row.Scan(valPtrs...); err != nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No matching forward booking found after update"})
			return
		}
		result := map[string]interface{}{}
		for i, col := range allCols {
			result[col] = vals[i]
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": result})
	}
}

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

