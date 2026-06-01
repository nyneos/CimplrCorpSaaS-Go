package forwards

import (
	"CimplrCorpSaas/api/fx/auditutil"
	"CimplrCorpSaas/internal/ctxutil"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"CimplrCorpSaas/api/constants"

	"github.com/lib/pq"
)

func isForwardPendingDeleteStatus(status string) bool {
	normalized := strings.ToUpper(strings.TrimSpace(status))
	return normalized == constants.FwdProcessingStatusPendingDeleteApproval || normalized == strings.ToUpper(constants.FwdProcessingStatusDeleteApproval)
}

func normalizeForwardSystemTransactionID(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return ""
	case []byte:
		return strings.TrimSpace(string(v))
	default:
		text := strings.TrimSpace(fmt.Sprint(v))
		if text == "<nil>" {
			return ""
		}
		return text
	}
}

func UpdateForwardBookingFields(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Accept system_transaction_id and fields in the JSON body
		var req struct {
			SystemTransactionID string                 `json:"system_transaction_id"`
			Fields              map[string]interface{} `json:"fields"`
			UserID              string                 `json:"user_id"`
			Reason              string                 `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.SystemTransactionID == "" || len(req.Fields) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: "system_transaction_id and at least one field to update must be provided in body"})
			return
		}
		// Check if booking exists
		var exists bool
		err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM forward_bookings WHERE system_transaction_id = $1)", req.SystemTransactionID).Scan(&exists)
		if err != nil || !exists {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: "No matching forward booking found"})
			return
		}
		// Get valid columns for forward_bookings
		colRows, err := db.Query(`SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'forward_bookings'`)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: "Failed to fetch columns"})
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
		// Edits require checker approval before becoming approved again.
		updateFields["processing_status"] = constants.FwdProcessingStatusPendingEditApproval
		if len(updateFields) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: "No valid fields to update"})
			return
		}
		oldValues := auditutil.FetchRowSnapshot(r.Context(), db, "public.forward_bookings", "system_transaction_id", req.SystemTransactionID)
		// Build dynamic SET clause
		keys := make([]string, 0, len(updateFields))
		values := make([]interface{}, 0, len(updateFields)+1)
		for k := range updateFields {
			keys = append(keys, k)
		}
		setClause := make([]string, len(keys))
		for i, k := range keys {
			setClause[i] = fmt.Sprintf(constants.FormatSQLColumnArg, k, i+1)
			values = append(values, updateFields[k])
		}
		values = append(values, req.SystemTransactionID)
		updateQuery := fmt.Sprintf("UPDATE forward_bookings SET %s WHERE system_transaction_id = $%d RETURNING row_to_json(forward_bookings)", strings.Join(setClause, ", "), len(values))
		var updatedRaw []byte
		if err := db.QueryRow(updateQuery, values...).Scan(&updatedRaw); err != nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: "No matching forward booking found after update"})
			return
		}
		result := map[string]interface{}{}
		if err := json.Unmarshal(updatedRaw, &result); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: "Failed to parse updated forward booking"})
			return
		}
		requestedBy := auditutil.Actor(req.UserID)
		if strings.TrimSpace(requestedBy) == "" {
			requestedBy = auditutil.ActorFromContext(r.Context())
		}
		auditutil.RecordAction(r.Context(), db, auditutil.ActionParams{TableName: auditutil.TableForwardBooking, ParentColumn: "system_transaction_id", ParentID: req.SystemTransactionID, ActionType: constants.FwdActionTypeEdit, Status: constants.FwdProcessingStatusPendingEditApproval, Reason: strings.TrimSpace(req.Reason), RequestedBy: requestedBy, OldValues: oldValues, NewValues: result})
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "updated": result})
	}
}

// Handler: BulkUpdateForwardBookingProcessingStatus
func BulkUpdateForwardBookingProcessingStatus(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID               string   `json:"user_id"`
			SystemTransactionIDs []string `json:"system_transaction_ids"`
			ProcessingStatus     string   `json:"processing_status"`
			ApprovalComment      string   `json:"approval_comment"`
			RejectionComment     string   `json:"rejection_comment"`
			Comment              string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: constants.ErrUserIDRequired})
			return
		}
		if len(req.SystemTransactionIDs) == 0 || (req.ProcessingStatus != constants.FwdProcessingStatusApproved && req.ProcessingStatus != constants.FwdProcessingStatusRejected) {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: "system_transaction_ids (array) and valid processing_status (Approved/Rejected) required"})
			return
		}
		scope := ctxutil.FromContext(r.Context())
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: constants.ErrNoAccessibleBusinessUnit})
			return
		}
		actor := auditutil.Actor(req.UserID)
		// Find which records are delete-approval and accessible
		delRows, err := db.Query(`
			SELECT system_transaction_id, entity_level_0
			FROM forward_bookings
			WHERE system_transaction_id = ANY($1)
			  AND processing_status IN ($2, $3)
		`, pq.Array(req.SystemTransactionIDs),
			constants.FwdProcessingStatusDeleteApproval,
			constants.FwdProcessingStatusPendingDeleteApproval,
		)

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: err.Error()})
			return
		}
		var deletedIds []string
		var deletedRows []map[string]interface{}
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
		// Only approved delete requests should soft-delete the booking.
		if len(deletedIds) > 0 && req.ProcessingStatus == constants.FwdProcessingStatusApproved {
			rows, err := db.Query(`
				UPDATE forward_bookings
				SET processing_status = $3,
				    is_deleted = TRUE,
				    deleted_at = now(),
				    deleted_by = $2
				WHERE system_transaction_id = ANY($1)
				  AND UPPER(COALESCE(processing_status, '')) IN ('DELETE-APPROVAL', 'PENDING_DELETE_APPROVAL')
				RETURNING *
			`, pq.Array(deletedIds), req.UserID, constants.FwdProcessingStatusApproved)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: err.Error()})
				return
			}
			cols, _ := rows.Columns()
			for rows.Next() {
				vals := make([]interface{}, len(cols))
				valPtrs := make([]interface{}, len(cols))
				for i := range vals {
					valPtrs[i] = &vals[i]
				}
				if err := rows.Scan(valPtrs...); err == nil {
					rowMap := make(map[string]interface{})
					for i, col := range cols {
						rowMap[col] = vals[i]
					}
					deletedRows = append(deletedRows, rowMap)
				}
			}
			rows.Close()
		}
		if len(deletedIds) > 0 && req.ProcessingStatus == constants.FwdProcessingStatusRejected {
			rows, err := db.Query(`
				UPDATE forward_bookings
				SET processing_status = $2
				WHERE system_transaction_id = ANY($1)
				  AND UPPER(COALESCE(processing_status, '')) IN ('DELETE-APPROVAL', 'PENDING_DELETE_APPROVAL')
				RETURNING *
			`, pq.Array(deletedIds), constants.FwdProcessingStatusApproved)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: err.Error()})
				return
			}
			cols, _ := rows.Columns()
			for rows.Next() {
				vals := make([]interface{}, len(cols))
				valPtrs := make([]interface{}, len(cols))
				for i := range vals {
					valPtrs[i] = &vals[i]
				}
				if err := rows.Scan(valPtrs...); err == nil {
					rowMap := make(map[string]interface{})
					for i, col := range cols {
						rowMap[col] = vals[i]
					}
					deletedRows = append(deletedRows, rowMap)
				}
			}
			rows.Close()
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
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: err.Error()})
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
				if req.ProcessingStatus == constants.FwdProcessingStatusApproved {
					resultRows, err = db.Query(`UPDATE forward_bookings SET processing_status = $1, status = $2 WHERE system_transaction_id = ANY($3) RETURNING *`, req.ProcessingStatus, constants.FwdStatusConfirmed, pq.Array(eligibleIds))
				} else {
					resultRows, err = db.Query(`UPDATE forward_bookings SET processing_status = $1 WHERE system_transaction_id = ANY($2) RETURNING *`, constants.FwdProcessingStatusRejected, pq.Array(eligibleIds))
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
			decisionStatus := strings.ToUpper(req.ProcessingStatus)
			if req.ProcessingStatus == constants.FwdProcessingStatusRejected {
				decisionStatus = constants.StatusRejected
			}
			decisionComment := strings.TrimSpace(req.Comment)
			if decisionComment == "" && req.ProcessingStatus == constants.FwdProcessingStatusApproved {
				decisionComment = strings.TrimSpace(req.ApprovalComment)
			}
			if decisionComment == "" && req.ProcessingStatus == constants.FwdProcessingStatusRejected {
				decisionComment = strings.TrimSpace(req.RejectionComment)
			}
			for _, row := range deletedRows {
				id := normalizeForwardSystemTransactionID(row["system_transaction_id"])
				if id == "" {
					continue
				}
				auditutil.RecordDecision(r.Context(), db, auditutil.DecisionParams{TableName: auditutil.TableForwardBooking, ParentColumn: "system_transaction_id", ParentID: id, Status: decisionStatus, CheckerBy: actor, Comment: decisionComment})
			}
			for _, row := range updatedRows {
				if id, ok := row["system_transaction_id"]; ok {
					if normalizedID := normalizeForwardSystemTransactionID(id); normalizedID != "" {
						auditutil.RecordDecision(r.Context(), db, auditutil.DecisionParams{TableName: auditutil.TableForwardBooking, ParentColumn: "system_transaction_id", ParentID: normalizedID, Status: decisionStatus, CheckerBy: actor, Comment: decisionComment})
					}
				}
			}
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "updated": updatedRows, "deleted": deletedIds})
		} else {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "No matching forward bookings found"})
		}
	}
}

// Handler: BulkDeleteForwardBookings
func BulkDeleteForwardBookings(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID               string   `json:"user_id"`
			SystemTransactionIDs []string `json:"system_transaction_ids"`
			Reason               string   `json:"reason"`
			Comment              string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: constants.ErrUserIDRequired})
			return
		}
		if len(req.SystemTransactionIDs) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: "system_transaction_ids (array) required"})
			return
		}
		scope := ctxutil.FromContext(r.Context())
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: constants.ErrNoAccessibleBusinessUnit})
			return
		}
		// Only update those belonging to accessible business units
		rows, err := db.Query(`SELECT system_transaction_id, entity_level_0 FROM forward_bookings WHERE system_transaction_id = ANY($1)`, pq.Array(req.SystemTransactionIDs))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: err.Error()})
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
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "No matching forward bookings found"})
			return
		}
		oldValuesByID := make(map[string]map[string]interface{}, len(eligibleIds))
		for _, id := range eligibleIds {
			oldValuesByID[id] = auditutil.FetchRowSnapshot(r.Context(), db, "public.forward_bookings", "system_transaction_id", id)
		}
		updateQuery := `
			UPDATE forward_bookings
			SET processing_status = $2
			WHERE system_transaction_id = ANY($1)
			  AND COALESCE(is_deleted, false) = false
			RETURNING *
		`
		resultRows, err := db.Query(updateQuery, pq.Array(eligibleIds), constants.FwdProcessingStatusPendingDeleteApproval)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: err.Error()})
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
				if id, ok := rowMap["system_transaction_id"]; ok {
					if normalizedID := normalizeForwardSystemTransactionID(id); normalizedID != "" {
						reason := strings.TrimSpace(req.Reason)
						if reason == "" {
							reason = strings.TrimSpace(req.Comment)
						}
						auditutil.RecordAction(r.Context(), db, auditutil.ActionParams{TableName: auditutil.TableForwardBooking, ParentColumn: "system_transaction_id", ParentID: normalizedID, ActionType: constants.FwdActionTypeDelete, Status: constants.FwdProcessingStatusPendingDeleteApproval, Reason: reason, RequestedBy: auditutil.Actor(req.UserID), OldValues: oldValuesByID[normalizedID], NewValues: rowMap})
					}
				}
			}
		}
		resultRows.Close()
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "updated": updated})
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
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: constants.ErrUserIDRequired})
			return
		}
		scope := ctxutil.FromContext(r.Context())
		buNames := scope.EntityNames
		if len(buNames) == 0 {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: constants.ErrNoAccessibleBusinessUnit})
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
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueError: "You do not have access to this business unit"})
			return
		}
		// Convert empty string date fields to nil
		bankConfirmationDate := req.BankConfirmationDate
		if bankConfirmationDate == "" {
			bankConfirmationDate = "1970-01-01" // or set to nil if you want NULL
		}
		updateQuery := `UPDATE forward_bookings SET
		       status = $1,
		       bank_transaction_id = $2,
		       swift_unique_id = $3,
		       bank_confirmation_date = $4,
		       processing_status = $5
	       WHERE internal_reference_id = $6 AND status = $7 AND entity_level_0 = $8
	       RETURNING internal_reference_id, entity_level_0, bank_transaction_id, swift_unique_id, bank_confirmation_date, status, processing_status`
		var bankConfirmationDateVal interface{}
		if bankConfirmationDate == "" {
			bankConfirmationDateVal = nil
		} else {
			bankConfirmationDateVal = bankConfirmationDate
		}
		updateValues := []interface{}{
			constants.FwdStatusConfirmed,
			req.BankTransactionID,
			req.SwiftUniqueID,
			bankConfirmationDateVal,
			constants.FwdProcessingStatusPending,
			req.InternalReferenceID,
			constants.FwdStatusPendingConfirmation,
			req.EntityLevel0,
		}
		row := db.QueryRow(updateQuery, updateValues...)
		cols := []string{"internal_reference_id", "entity_level_0", "bank_transaction_id", "swift_unique_id", "bank_confirmation_date", constants.KeyStatus, "processing_status"}
		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}
		if err := row.Scan(valPtrs...); err != nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "No matching record found or already confirmed"})
			return
		}
		result := make(map[string]interface{})
		for i, col := range cols {
			result[col] = vals[i]
		}
		var systemTransactionID string
		_ = db.QueryRowContext(r.Context(), `SELECT system_transaction_id FROM forward_bookings WHERE internal_reference_id = $1 AND entity_level_0 = $2 LIMIT 1`, req.InternalReferenceID, req.EntityLevel0).Scan(&systemTransactionID)
		if strings.TrimSpace(systemTransactionID) != "" {
			auditutil.RecordAction(r.Context(), db, auditutil.ActionParams{TableName: auditutil.TableForwardBooking, ParentColumn: "system_transaction_id", ParentID: systemTransactionID, ActionType: constants.FwdActionTypeConfirm, Status: constants.FwdProcessingStatusPendingEditApproval, Reason: "", RequestedBy: auditutil.Actor(req.UserID), OldValues: nil, NewValues: result})
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true, "updated": result})
	}
}
