package exposures

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/xuri/excelize/v2"

	// "mime/multipart"
	// "math/rand"
	// "time"

	"github.com/lib/pq"
)

func UploadExposure(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID string `json:"user_id"`
		// ...other exposure fields...
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	activeSessions := auth.GetActiveSessions()
	found := false
	for _, session := range activeSessions {
		if session.UserID == req.UserID {
			found = true
			break
		}
	}

	if !found {
		http.Error(w, "Unauthorized: invalid session", http.StatusUnauthorized)
		return
	}

	buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
	if !ok {
		http.Error(w, "Business units not found in context", http.StatusInternalServerError)
		return
	}
	responseNames, _ := json.Marshal(buNames)
	// Handle the exposure upload logic here
	w.Write(responseNames)
}


// Helper: send JSON error response and log
func respondWithError(w http.ResponseWriter, status int, errMsg string) {
	log.Println("[ERROR]", errMsg)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"error":   errMsg,
	})
}

// Helper: check if string in slice
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// Helper: parse DB value to correct Go type
func parseDBValue(col string, val interface{}) interface{} {
	if b, ok := val.([]byte); ok {
		// JSON fields
		if col == "additional_header_details" || col == "additional_line_details" {
			var obj interface{}
			if err := json.Unmarshal(b, &obj); err == nil {
				return obj
			}
		}
		// Numeric fields
		numericFields := map[string]bool{
			"amount_in_local_currency": true,
			"line_item_amount": true,
			"quantity": true,
			"total_open_amount": true,
			"total_original_amount": true,
			"unit_price": true,
		}
		if numericFields[col] {
			s := string(b)
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f
			}
		}
		return string(b)
	}
	return val
}

// Handler
func EditExposureHeadersLineItemsJoined(db *sql.DB) http.HandlerFunc {
	// ...existing code...
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     string                 `json:"id"` // exposure_header_id
			Fields map[string]interface{} `json:"fields"`
			UserID string                 `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid request: "+err.Error())
			return
		}

		// Get buNames from context
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusInternalServerError, "Business units not found in context")
			return
		}

		// Get joined row
		joinRow := db.QueryRow(`
			SELECT h.exposure_header_id, h.entity
			FROM exposure_headers h
			JOIN exposure_line_items l ON h.exposure_header_id = l.exposure_header_id
			WHERE h.exposure_header_id = $1
		`, req.ID)

		var exposureHeaderID, entity string
		if err := joinRow.Scan(&exposureHeaderID, &entity); err != nil {
			respondWithError(w, http.StatusNotFound, "Row not found: "+err.Error())
			return
		}

		// Validate entity is in buNames
		if !contains(buNames, entity) {
			respondWithError(w, http.StatusForbidden, "Forbidden: entity not accessible")
			return
		}

		// Get columns for each table
		headerColsRes, _ := db.Query(`SELECT column_name FROM information_schema.columns WHERE table_name = 'exposure_headers'`)
		lineColsRes, _ := db.Query(`SELECT column_name FROM information_schema.columns WHERE table_name = 'exposure_line_items'`)
		defer headerColsRes.Close()
		defer lineColsRes.Close()

		var headerCols, lineCols []string
		for headerColsRes.Next() {
			var col string
			headerColsRes.Scan(&col)
			headerCols = append(headerCols, col)
		}
		for lineColsRes.Next() {
			var col string
			lineColsRes.Scan(&col)
			lineCols = append(lineCols, col)
		}

		headerFields := make(map[string]interface{})
		lineFields := make(map[string]interface{})
		for k, v := range req.Fields {
			if contains(headerCols, k) {
				headerFields[k] = v
			}
			if contains(lineCols, k) {
				lineFields[k] = v
			}
		}

		// Update header if needed
		if len(headerFields) > 0 {
			setParts := []string{}
			values := []interface{}{}
			i := 1
			for k, v := range headerFields {
				setParts = append(setParts, fmt.Sprintf("%s = $%d", k, i))
				values = append(values, v)
				i++
			}
			setParts = append(setParts, "approval_status = 'Pending'")
			values = append(values, exposureHeaderID)

			query := fmt.Sprintf(
				"UPDATE exposure_headers SET %s WHERE exposure_header_id = $%d",
				strings.Join(setParts, ", "),
				len(values),
			)
			if _, err := db.Exec(query, values...); err != nil {
				respondWithError(w, http.StatusInternalServerError, "Header update failed: "+err.Error())
				return
			}
		}

		// Update line item if needed
		if len(lineFields) > 0 {
			setParts := []string{}
			values := []interface{}{}
			i := 1
			for k, v := range lineFields {
				setParts = append(setParts, fmt.Sprintf("%s = $%d", k, i))
				values = append(values, v)
				i++
			}
			values = append(values, exposureHeaderID)

			query := fmt.Sprintf(
				"UPDATE exposure_line_items SET %s WHERE exposure_header_id = $%d",
				strings.Join(setParts, ", "),
				len(values),
			)
			if _, err := db.Exec(query, values...); err != nil {
				respondWithError(w, http.StatusInternalServerError, "Line item update failed: "+err.Error())
				return
			}
		}

		// Only return the updated fields
		// Fetch full joined row and parse fields
		rows, err := db.Query(`
			SELECT h.*, l.*
			FROM exposure_headers h
			JOIN exposure_line_items l ON h.exposure_header_id = l.exposure_header_id
			WHERE h.exposure_header_id = $1
		`, exposureHeaderID)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Fetch failed: "+err.Error())
			return
		}
		defer rows.Close()

		cols, _ := rows.Columns()
		results := []map[string]interface{}{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := rows.Scan(valPtrs...); err != nil {
				respondWithError(w, http.StatusInternalServerError, "Row scan failed: "+err.Error())
				return
			}
			rowMap := map[string]interface{}{}
			for i, col := range cols {
				rowMap[col] = parseDBValue(col, vals[i])
			}
			results = append(results, rowMap)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    results,
		})
	}
}



// Handler: GetExposureHeadersLineItems - returns joined exposure_headers and exposure_line_items filtered by entity
func GetExposureHeadersLineItems(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "Please login to continue.")
			return
		}

		// Get user's business unit name
		var userBu string
		err := db.QueryRow("SELECT business_unit_name FROM users WHERE id = $1", req.UserID).Scan(&userBu)
		if err != nil || userBu == "" {
			respondWithError(w, http.StatusNotFound, "User not found or has no business unit assigned")
			return
		}

		// Get root entity id
		var rootEntityId string
		err = db.QueryRow(
			"SELECT entity_id FROM masterEntity WHERE entity_name = $1 AND (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)",
			userBu,
		).Scan(&rootEntityId)
		if err != nil {
			respondWithError(w, http.StatusNotFound, "Business unit entity not found")
			return
		}

		// Recursive CTE to get all descendant entity_names
		rows, err := db.Query(`
			WITH RECURSIVE descendants AS (
				SELECT entity_id, entity_name FROM masterEntity WHERE entity_id = $1
				UNION ALL
				SELECT me.entity_id, me.entity_name
				FROM masterEntity me
				INNER JOIN entityRelationships er ON me.entity_id = er.child_entity_id
				INNER JOIN descendants d ON er.parent_entity_id = d.entity_id
				WHERE (me.approval_status = 'Approved' OR me.approval_status = 'approved') AND (me.is_deleted = false OR me.is_deleted IS NULL)
			)
			SELECT entity_name FROM descendants
		`, rootEntityId)
		if err != nil {
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}
		defer rows.Close()
		var buNames []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err == nil {
				buNames = append(buNames, name)
			}
		}
		if len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}

		// Join exposure_headers and exposure_line_items filtered by entity
		joinRows, err := db.Query(`
			SELECT h.*, l.*
			FROM exposure_headers h
			JOIN exposure_line_items l ON h.exposure_header_id = l.exposure_header_id
			WHERE h.entity = ANY($1)
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch exposure headers/line items")
			return
		}
		defer joinRows.Close()
		joinCols, _ := joinRows.Columns()
		joinData := []map[string]interface{}{}
		for joinRows.Next() {
			vals := make([]interface{}, len(joinCols))
			valPtrs := make([]interface{}, len(joinCols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := joinRows.Scan(valPtrs...); err != nil {
				continue
			}
			rowMap := map[string]interface{}{}
			for i, col := range joinCols {
				rowMap[col] = parseDBValue(col, vals[i])
			}
			joinData = append(joinData, rowMap)
		}

		// Fetch permissions for 'exposure-upload' page for this role
		exposureUploadPerms := map[string]interface{}{}
		var roleId int
		err = db.QueryRow("SELECT role_id FROM user_roles WHERE user_id = $1 LIMIT 1", req.UserID).Scan(&roleId)
		if err == nil {
			permRows, err := db.Query(`
				SELECT p.page_name, p.tab_name, p.action, rp.allowed
				FROM role_permissions rp
				JOIN permissions p ON rp.permission_id = p.id
				WHERE rp.role_id = $1 AND (rp.status = 'Approved' OR rp.status = 'approved')
			`, roleId)
			if err == nil {
				defer permRows.Close()
				for permRows.Next() {
					var pageName, tabName, action string
					var allowed bool
					if err := permRows.Scan(&pageName, &tabName, &action, &allowed); err == nil {
						if pageName != "exposure-upload" {
							continue
						}
						if exposureUploadPerms["exposure-upload"] == nil {
							exposureUploadPerms["exposure-upload"] = map[string]interface{}{}
						}
						perms := exposureUploadPerms["exposure-upload"].(map[string]interface{})
						if tabName == "" {
							if perms["pagePermissions"] == nil {
								perms["pagePermissions"] = map[string]interface{}{}
							}
							perms["pagePermissions"].(map[string]interface{})[action] = allowed
						} else {
							if perms["tabs"] == nil {
								perms["tabs"] = map[string]interface{}{}
							}
							if perms["tabs"].(map[string]interface{})[tabName] == nil {
								perms["tabs"].(map[string]interface{})[tabName] = map[string]interface{}{}
							}
							perms["tabs"].(map[string]interface{})[tabName].(map[string]interface{})[action] = allowed
						}
					}
				}
			}
		}

		resp := map[string]interface{}{
			"buAccessible": buNames,
			"pageData": joinData,
		}
		if perms, ok := exposureUploadPerms["exposure-upload"]; ok {
			resp["exposure-upload"] = perms
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

// Handler: GetPendingApprovalHeadersLineItems - returns joined exposure_headers and exposure_line_items filtered by entity and approval_status pending
func GetPendingApprovalHeadersLineItems(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "Please login to continue.")
			return
		}

		// Validate session
		activeSessions := auth.GetActiveSessions()
		var session *auth.UserSession
		for _, s := range activeSessions {
			if s.UserID == req.UserID {
				session = s
				break
			}
		}
		if session == nil {
			respondWithError(w, http.StatusNotFound, "Session expired or not found. Please login again.")
			return
		}

		// Get user's business unit name
		var userBu string
		err := db.QueryRow("SELECT business_unit_name FROM users WHERE id = $1", req.UserID).Scan(&userBu)
		if err != nil || userBu == "" {
			respondWithError(w, http.StatusNotFound, "User not found or has no business unit assigned")
			return
		}

		// Get root entity id
		var rootEntityId string
		err = db.QueryRow(
			"SELECT entity_id FROM masterEntity WHERE entity_name = $1 AND (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)",
			userBu,
		).Scan(&rootEntityId)
		if err != nil {
			respondWithError(w, http.StatusNotFound, "Business unit entity not found")
			return
		}

		// Recursive CTE to get all descendant entity_names
		rows, err := db.Query(`
			WITH RECURSIVE descendants AS (
				SELECT entity_id, entity_name FROM masterEntity WHERE entity_id = $1
				UNION ALL
				SELECT me.entity_id, me.entity_name
				FROM masterEntity me
				INNER JOIN entityRelationships er ON me.entity_id = er.child_entity_id
				INNER JOIN descendants d ON er.parent_entity_id = d.entity_id
				WHERE (me.approval_status = 'Approved' OR me.approval_status = 'approved') AND (me.is_deleted = false OR me.is_deleted IS NULL)
			)
			SELECT entity_name FROM descendants
		`, rootEntityId)
		if err != nil {
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}
		defer rows.Close()
		var buNames []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err == nil {
				buNames = append(buNames, name)
			}
		}
		if len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}

		// Join exposure_headers and exposure_line_items filtered by entity and approval_status pending
		joinRows, err := db.Query(`
			SELECT h.*, l.*
			FROM exposure_headers h
			JOIN exposure_line_items l ON h.exposure_header_id = l.exposure_header_id
			WHERE h.entity = ANY($1) AND h.approval_status NOT IN ('Approved', 'approved')
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch pending approval headers/line items")
			return
		}
		defer joinRows.Close()
		joinCols, _ := joinRows.Columns()
		joinData := []map[string]interface{}{}
		for joinRows.Next() {
			vals := make([]interface{}, len(joinCols))
			valPtrs := make([]interface{}, len(joinCols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := joinRows.Scan(valPtrs...); err != nil {
				continue
			}
			rowMap := map[string]interface{}{}
			for i, col := range joinCols {
				rowMap[col] = parseDBValue(col, vals[i])
			}
			joinData = append(joinData, rowMap)
		}

		// Fetch permissions for 'exposure-upload' page for this role
		exposureUploadPerms := map[string]interface{}{}
		var roleId int
		err = db.QueryRow("SELECT role_id FROM user_roles WHERE user_id = $1 LIMIT 1", req.UserID).Scan(&roleId)
		if err == nil {
			permRows, err := db.Query(`
				SELECT p.page_name, p.tab_name, p.action, rp.allowed
				FROM role_permissions rp
				JOIN permissions p ON rp.permission_id = p.id
				WHERE rp.role_id = $1 AND (rp.status = 'Approved' OR rp.status = 'approved')
			`, roleId)
			if err == nil {
				defer permRows.Close()
				for permRows.Next() {
					var pageName, tabName, action string
					var allowed bool
					if err := permRows.Scan(&pageName, &tabName, &action, &allowed); err == nil {
						if pageName != "exposure-upload" {
							continue
						}
						if exposureUploadPerms["exposure-upload"] == nil {
							exposureUploadPerms["exposure-upload"] = map[string]interface{}{}
						}
						perms := exposureUploadPerms["exposure-upload"].(map[string]interface{})
						if tabName == "" {
							if perms["pagePermissions"] == nil {
								perms["pagePermissions"] = map[string]interface{}{}
							}
							perms["pagePermissions"].(map[string]interface{})[action] = allowed
						} else {
							if perms["tabs"] == nil {
								perms["tabs"] = map[string]interface{}{}
							}
							if perms["tabs"].(map[string]interface{})[tabName] == nil {
								perms["tabs"].(map[string]interface{})[tabName] = map[string]interface{}{}
							}
							perms["tabs"].(map[string]interface{})[tabName].(map[string]interface{})[action] = allowed
						}
					}
				}
			}
		}

		resp := map[string]interface{}{
			"buAccessible": buNames,
			"pageData": joinData,
		}
		if perms, ok := exposureUploadPerms["exposure-upload"]; ok {
			resp["exposure-upload"] = perms
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

// Handler: deleteExposureHeaders - marks headers for delete approval
func DeleteExposureHeaders(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			ExposureHeaderIds []string `json:"exposureHeaderIds"`
			RequestedBy       string   `json:"requested_by"`
			DeleteComment     string   `json:"delete_comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.ExposureHeaderIds) == 0 || req.RequestedBy == "" {
			respondWithError(w, http.StatusBadRequest, "exposureHeaderIds and requested_by are required")
			return
		}
		// Mark for delete approval first
		res, err := db.Exec(
			`UPDATE exposure_headers SET approval_status = 'Delete-Approval', delete_comment = $1 WHERE exposure_header_id = ANY($2::uuid[])`,
			req.DeleteComment,
			pq.Array(req.ExposureHeaderIds),
		)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		count, _ := res.RowsAffected()
		if count == 0 {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "No matching exposure_headers found",
			})
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": fmt.Sprintf("%d exposure_header(s) marked for delete approval", count),
		})
	}
}
// Handler: rejectMultipleExposureHeaders - rejects multiple headers
func RejectMultipleExposureHeaders(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			ExposureHeaderIds []string `json:"exposureHeaderIds"`
			RejectedBy        string   `json:"rejected_by"`
			RejectionComment  string   `json:"rejection_comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.ExposureHeaderIds) == 0 || req.RejectedBy == "" {
			respondWithError(w, http.StatusBadRequest, "exposureHeaderIds and rejected_by are required")
			return
		}
		rows, err := db.Query(
			`UPDATE exposure_headers SET approval_status = 'Rejected', rejected_by = $1, rejection_comment = $2, rejected_at = NOW() WHERE exposure_header_id = ANY($3::uuid[]) RETURNING *`,
			req.RejectedBy,
			req.RejectionComment,
			pq.Array(req.ExposureHeaderIds),
		)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		rejected := []map[string]interface{}{}
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
			rejected = append(rejected, rowMap)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"rejected": rejected,
		})
	}
}
// Handler: approveMultipleExposureHeaders - approves multiple headers and handles business logic
func ApproveMultipleExposureHeaders(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			ExposureHeaderIds []string `json:"exposureHeaderIds"`
			ApprovedBy        string   `json:"approved_by"`
			ApprovalComment   string   `json:"approval_comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.ExposureHeaderIds) == 0 || req.ApprovedBy == "" {
			respondWithError(w, http.StatusBadRequest, "exposureHeaderIds and approved_by are required")
			return
		}
		tx, err := db.Begin()
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer tx.Rollback()
		// Fetch current statuses and details
		rows, err := tx.Query(`
			SELECT exposure_header_id, approval_status, exposure_type, status, document_id, total_original_amount, total_open_amount, additional_header_details
			FROM exposure_headers WHERE exposure_header_id = ANY($1::uuid[])`, pq.Array(req.ExposureHeaderIds))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		type headerRow struct {
			ExposureHeaderId     string
			ApprovalStatus      string
			ExposureType        string
			Status              string
			DocumentId          string
			TotalOriginalAmount float64
			TotalOpenAmount     float64
			AdditionalDetails   []byte
		}
		headers := []headerRow{}
		for rows.Next() {
			var h headerRow
			if err := rows.Scan(
				&h.ExposureHeaderId,
				&h.ApprovalStatus,
				&h.ExposureType,
				&h.Status,
				&h.DocumentId,
				&h.TotalOriginalAmount,
				&h.TotalOpenAmount,
				&h.AdditionalDetails,
			); err == nil {
				headers = append(headers, h)
			}
		}
		toDelete := []string{}
		toApprove := []string{}
		skipped := []string{}
		for _, h := range headers {
			status := strings.ToLower(h.ApprovalStatus)
			if strings.Contains(status, "delete") {
				toDelete = append(toDelete, h.ExposureHeaderId)
			} else if status == "pending" || status == "rejected" {
				toApprove = append(toApprove, h.ExposureHeaderId)
			} else if status == "approved" {
				skipped = append(skipped, h.ExposureHeaderId)
			}
		}
		results := map[string]interface{}{
			"deleted": []map[string]interface{}{},
			"approved": []map[string]interface{}{},
			"rolled": []map[string]interface{}{},
			"skipped": skipped,
		}
		// Handle delete-approval: delete header and line items
		if len(toDelete) > 0 {
			errors := []string{}
			// Delete from exposure_rollover_log
			if _, err := tx.Exec(`DELETE FROM exposure_rollover_log WHERE child_header_id = ANY($1::uuid[]) OR parent_header_id = ANY($1::uuid[])`, pq.Array(toDelete)); err != nil {
				errors = append(errors, "exposure_rollover_log: "+err.Error())
			}
			// Delete from exposure_bucketing
			if _, err := tx.Exec(`DELETE FROM exposure_bucketing WHERE exposure_header_id = ANY($1::uuid[])`, pq.Array(toDelete)); err != nil {
				errors = append(errors, "exposure_bucketing: "+err.Error())
			}
			// Delete from hedging_proposal
			if _, err := tx.Exec(`DELETE FROM hedging_proposal WHERE exposure_header_id = ANY($1::uuid[])`, pq.Array(toDelete)); err != nil {
				errors = append(errors, "hedging_proposal: "+err.Error())
			}
			// Log deleted headers before deletion
			delRows, errDel := tx.Query(`DELETE FROM exposure_headers WHERE exposure_header_id = ANY($1::uuid[]) RETURNING *`, pq.Array(toDelete))
			deletedHeaders := []map[string]interface{}{}
			if errDel != nil {
				errors = append(errors, "exposure_headers: "+errDel.Error())
			} else {
				delCols, _ := delRows.Columns()
				for delRows.Next() {
					vals := make([]interface{}, len(delCols))
					valPtrs := make([]interface{}, len(delCols))
					for i := range vals {
						valPtrs[i] = &vals[i]
					}
					if err := delRows.Scan(valPtrs...); err != nil {
						continue
					}
					rowMap := map[string]interface{}{}
					for i, col := range delCols {
						rowMap[col] = parseDBValue(col, vals[i])
					}
					deletedHeaders = append(deletedHeaders, rowMap)
				}
				delRows.Close()
			}
			// Delete from exposure_line_items
			if _, err := tx.Exec(`DELETE FROM exposure_line_items WHERE exposure_header_id = ANY($1::uuid[])`, pq.Array(toDelete)); err != nil {
				errors = append(errors, "exposure_line_items: "+err.Error())
			}
			results["deleted"] = deletedHeaders
			// Reverse rollover if needed
			for _, h := range deletedHeaders {
				var parentDocNo string
				var typeStr string
				var addDetails map[string]interface{}
				if v, ok := h["exposure_type"].(string); ok {
					typeStr = strings.ToLower(v)
				}
				if v, ok := h["additional_header_details"].(map[string]interface{}); ok {
					addDetails = v
				}
				if typeStr == "lc" && addDetails != nil {
					if lc, ok := addDetails["input_letters_of_credit"].(map[string]interface{}); ok {
						if linked, ok := lc["linked_po_so_number"].(string); ok {
							parentDocNo = linked
						}
					}
				} else if typeStr == "grn" && addDetails != nil {
					if grn, ok := addDetails["input_grn"].(map[string]interface{}); ok {
						if linked, ok := grn["linked_id"].(string); ok {
							parentDocNo = linked
						}
					}
				} else if typeStr == "creditors" && addDetails != nil {
					if cred, ok := addDetails["input_creditors"].(map[string]interface{}); ok {
						if linked, ok := cred["linked_id"].(string); ok {
							parentDocNo = linked
						}
					}
				} else if typeStr == "debitors" && addDetails != nil {
					if deb, ok := addDetails["input_debitors"].(map[string]interface{}); ok {
						if linked, ok := deb["linked_id"].(string); ok {
							parentDocNo = linked
						}
					}
				}
				if parentDocNo != "" {
					var parentId string
					err := tx.QueryRow(`SELECT exposure_header_id FROM exposure_headers WHERE document_id = $1 LIMIT 1`, parentDocNo).Scan(&parentId)
					if err == nil {
						amt := 0.0
						if v, ok := h["total_open_amount"].(float64); ok {
							amt = math.Abs(v)
						} else if v, ok := h["total_original_amount"].(float64); ok {
							amt = math.Abs(v)
						}
						_, _ = tx.Exec(`UPDATE exposure_headers SET total_open_amount = total_open_amount + $1, status = 'Open' WHERE exposure_header_id = $2`, amt, parentId)
					}
				}
			}
		}
		
		// Approve remaining headers
		if len(toApprove) > 0 {
			appRows, _ := tx.Query(`UPDATE exposure_headers SET approval_status = 'Approved', approved_by = $1, approval_comment = $2, approved_at = NOW() WHERE exposure_header_id = ANY($3::uuid[]) RETURNING *`, req.ApprovedBy, req.ApprovalComment, pq.Array(toApprove))
			approvedHeaders := []map[string]interface{}{}
			appCols, _ := appRows.Columns()
			for appRows.Next() {
				vals := make([]interface{}, len(appCols))
				valPtrs := make([]interface{}, len(appCols))
				for i := range vals {
					valPtrs[i] = &vals[i]
				}
				if err := appRows.Scan(valPtrs...); err != nil {
					continue
				}
				rowMap := map[string]interface{}{}
				for i, col := range appCols {
					rowMap[col] = parseDBValue(col, vals[i])
				}
				approvedHeaders = append(approvedHeaders, rowMap)
			}
			appRows.Close()
			results["approved"] = approvedHeaders
			// Rollover logic
			for _, h := range approvedHeaders {
				var parentDocNo string
				var typeStr string
				var addDetails map[string]interface{}
				if v, ok := h["exposure_type"].(string); ok {
					typeStr = strings.ToLower(v)
				}
				if v, ok := h["additional_header_details"].(map[string]interface{}); ok {
					addDetails = v
				}
				if typeStr == "lc" && addDetails != nil {
					if lc, ok := addDetails["input_letters_of_credit"].(map[string]interface{}); ok {
						if linked, ok := lc["linked_po_so_number"].(string); ok {
							parentDocNo = linked
						}
					}
				} else if typeStr == "grn" && addDetails != nil {
					if grn, ok := addDetails["input_grn"].(map[string]interface{}); ok {
						if linked, ok := grn["linked_id"].(string); ok {
							parentDocNo = linked
						}
					}
				} else if typeStr == "creditors" && addDetails != nil {
					if cred, ok := addDetails["input_creditors"].(map[string]interface{}); ok {
						if linked, ok := cred["linked_id"].(string); ok {
							parentDocNo = linked
						}
					}
				} else if typeStr == "debitors" && addDetails != nil {
					if deb, ok := addDetails["input_debitors"].(map[string]interface{}); ok {
						if linked, ok := deb["linked_id"].(string); ok {
							parentDocNo = linked
						}
					}
				}
				if parentDocNo != "" {
					var parentId string
					err := tx.QueryRow(`SELECT exposure_header_id FROM exposure_headers WHERE document_id = $1 LIMIT 1`, parentDocNo).Scan(&parentId)
					if err == nil {
						amt := 0.0
						if v, ok := h["total_original_amount"].(float64); ok {
							amt = math.Abs(v)
						}
						_, _ = tx.Exec(`UPDATE exposure_headers SET total_open_amount = total_open_amount - $1, status = 'Rolled' WHERE exposure_header_id = $2`, amt, parentId)
						_, _ = tx.Exec(`UPDATE exposure_headers SET status = 'Rolled' WHERE exposure_header_id = $1`, h["exposure_header_id"])
						_, _ = tx.Exec(`INSERT INTO exposure_rollover_log (parent_header_id, child_header_id, rollover_amount, rollover_date, created_at) VALUES ($1, $2, $3, CURRENT_DATE, NOW())`, parentId, h["exposure_header_id"], amt)
						if rolled, ok := results["rolled"].([]map[string]interface{}); ok {
							rolled = append(rolled, h)
							results["rolled"] = rolled
						}
					}
				}
			}
		}
		tx.Commit()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"deleted": results["deleted"],
			"approved": results["approved"],
			"rolled": results["rolled"],
			"skipped": results["skipped"],
		})
	}
}

// Handler: BatchUploadStagingData - handles batch upload for staging tables
func BatchUploadStagingData(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse multipart form
		err := r.ParseMultipartForm(32 << 20) // 32MB max
		if err != nil {
			respondWithError(w, http.StatusBadRequest, "Invalid multipart form: "+err.Error())
			return
		}
		userID := r.FormValue("user_id")
		if userID == "" {
			respondWithError(w, http.StatusBadRequest, "Please login to continue.")
			return
		}
		// Get buNames from context (from middleware)
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, "No accessible business units found")
			return
		}
		// Supported file fields
		fileFields := []struct {
			Field     string
			DataType  string
			TableName string
		}{
			{"input_grn", "grn", "input_grn"},
			{"input_letters_of_credit", "LC", "input_letters_of_credit"},
			{"input_purchase_orders", "PO", "input_purchase_orders"},
			{"input_sales_orders", "SO", "input_sales_orders"},
			{"input_creditors", "creditors", "input_creditors"},
			{"input_debitors", "debitors", "input_debitors"},
		}
		results := []map[string]interface{}{}
		absorptionErrors := []string{}
		for _, field := range fileFields {
			files := r.MultipartForm.File[field.Field]
			for _, fh := range files {
				filename := fh.Filename
				tempFile, err := os.CreateTemp("", "upload-*")
				// if err != nil {
				// 	results = append(results, map[string]interface{}{"filename": filename, "error": "Failed to save file"})
				// 	continue
				// }
				if err != nil {
					results = append(results, map[string]interface{}{"filename": filename, "error": "Failed to open file"})
					tempFile.Close()
					os.Remove(tempFile.Name())
					continue
				}
				for _, fh := range files {
					filename := fh.Filename
					tempFile, err := os.CreateTemp("", "upload-*")
					if err != nil {
						results = append(results, map[string]interface{}{"filename": filename, "error": "Failed to save file"})
						continue
					}
					f, err := fh.Open()
					if err != nil {
						results = append(results, map[string]interface{}{"filename": filename, "error": "Failed to open file"})
						tempFile.Close()
						os.Remove(tempFile.Name())
						continue
					}
					io.Copy(tempFile, f)
					f.Close()
					tempFile.Close()
					var dataArr []map[string]interface{}
					ext := filepath.Ext(filename)
					if ext == ".csv" {
						file, _ := os.Open(tempFile.Name())
						reader := csv.NewReader(file)
						headers, err := reader.Read()
						if err != nil {
							results = append(results, map[string]interface{}{"filename": filename, "error": "Failed to read CSV headers"})
							file.Close()
							os.Remove(tempFile.Name())
							continue
						}
						for {
							row, err := reader.Read()
							if err != nil {
								break
							}
							obj := map[string]interface{}{}
							for i, h := range headers {
								obj[h] = row[i]
							}
							dataArr = append(dataArr, obj)
						}
						file.Close()
					} else if ext == ".xlsx" || ext == ".xls" {
						xl, err := excelize.OpenFile(tempFile.Name())
						if err != nil {
							results = append(results, map[string]interface{}{"filename": filename, "error": "Failed to read Excel file"})
							os.Remove(tempFile.Name())
							continue
						}
						sheet := xl.GetSheetName(0)
						rows, err := xl.GetRows(sheet)
						if err != nil || len(rows) < 1 {
							results = append(results, map[string]interface{}{"filename": filename, "error": "No data in Excel file"})
							os.Remove(tempFile.Name())
							continue
						}
						headers := rows[0]
						for _, row := range rows[1:] {
							obj := map[string]interface{}{}
							for i, h := range headers {
								if i < len(row) {
									obj[h] = row[i]
								} else {
									obj[h] = nil
								}
							}
							dataArr = append(dataArr, obj)
						}
					} else {
						results = append(results, map[string]interface{}{"filename": filename, "error": "Unsupported file type"})
						os.Remove(tempFile.Name())
						continue
					}
					var buCol string
					switch field.DataType {
					case "LC":
						buCol = "applicant_name"
					case "PO", "SO":
						buCol = "entity"
					case "creditors", "debitors", "grn":
						buCol = "company"
					}
					invalidRows := []string{}
					for _, row := range dataArr {
						if buCol != "" {
							buVal, _ := row[buCol].(string)
							if !contains(buNames, buVal) {
								ref := "(no ref)"
								for _, k := range []string{"reference_no", "document_no", "system_lc_number", "bank_reference"} {
									if v, ok := row[k].(string); ok && v != "" {
										ref = v
										break
									}
								}
								invalidRows = append(invalidRows, ref)
							}
						}
					}
					if len(invalidRows) > 0 {
						results = append(results, map[string]interface{}{
							"filename": filename,
							"error": "Some rows have business_unit not allowed for this user.",
							"invalidReferenceNos": invalidRows,
						})
						os.Remove(tempFile.Name())
						continue
					}
					uploadBatchId := uuid.New().String()
					insertedRows := 0
					for i, row := range dataArr {
						row["upload_batch_id"] = uploadBatchId
						row["row_number"] = i + 1
						keys := []string{}
						vals := []interface{}{}
						placeholders := []string{}
						idx := 1
						for k, v := range row {
							keys = append(keys, k)
							vals = append(vals, v)
							placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
							idx++
						}
						query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", field.TableName, strings.Join(keys, ", "), strings.Join(placeholders, ", "))
						_, err := db.Exec(query, vals...)
						if err != nil {
							results = append(results, map[string]interface{}{"filename": filename, "error": "Failed to insert row: " + err.Error()})
							db.Exec(fmt.Sprintf("DELETE FROM %s WHERE upload_batch_id = $1", field.TableName), uploadBatchId)
							os.Remove(tempFile.Name())
							break
						}
						insertedRows++
					}
					mappingRows, err := db.Query(`SELECT source_column_name, target_table_name, target_field_name FROM upload_mappings WHERE exposure_type = $1 ORDER BY target_table_name, target_field_name`, field.DataType)
					insertedFinalRows := 0
					if err == nil {
						defer mappingRows.Close()
						var mappings []struct {
							SourceCol string
							TargetTable string
							TargetField string
						}
						for mappingRows.Next() {
							var src, tgtTable, tgtField string
							mappingRows.Scan(&src, &tgtTable, &tgtField)
							mappings = append(mappings, struct {
								SourceCol string
								TargetTable string
								TargetField string
							}{src, tgtTable, tgtField})
						}
						stagedRows, err := db.Query(fmt.Sprintf(`SELECT * FROM %s WHERE upload_batch_id = $1`, field.TableName), uploadBatchId)
						if err == nil {
							defer stagedRows.Close()
							stagedCols, _ := stagedRows.Columns()
							for stagedRows.Next() {
								stagedVals := make([]interface{}, len(stagedCols))
								stagedPtrs := make([]interface{}, len(stagedCols))
								for i := range stagedVals {
									stagedPtrs[i] = &stagedVals[i]
								}
								stagedRows.Scan(stagedPtrs...)
								staged := map[string]interface{}{}
								for i, col := range stagedCols {
									staged[col] = stagedVals[i]
								}
								header := map[string]interface{}{}
								headerDetails := map[string]interface{}{}
								for _, m := range mappings {
									if m.TargetTable == "exposure_headers" {
										var val interface{}
										switch m.SourceCol {
										case field.DataType:
											val = field.DataType
										case "Open":
											val = "Open"
										case "true":
											val = true
										case field.TableName:
											val = staged
										default:
											val = staged[m.SourceCol]
										}
										if m.TargetField == "additional_header_details" {
											headerDetails[m.SourceCol] = val
										} else {
											if strings.Contains(m.TargetField, "amount") {
												switch v := val.(type) {
												case float64:
													val = math.Abs(v)
												case string:
													if f, err := strconv.ParseFloat(v, 64); err == nil {
														val = math.Abs(f)
													}
												}
											}
											header[m.TargetField] = val
										}
									}
								}
								header["additional_header_details"] = headerDetails
								headerKeys := []string{}
								headerVals := []interface{}{}
								headerPlaceholders := []string{}
								idx := 1
								for k, v := range header {
									headerKeys = append(headerKeys, k)
									// Marshal map[string]interface{} to JSON for DB
									if m, ok := v.(map[string]interface{}); ok {
										jsonVal, err := json.Marshal(m)
										if err != nil {
											absorptionErrors = append(absorptionErrors, fmt.Sprintf("header marshal error for file %s, key %s: %v", filename, k, err))
											headerVals = append(headerVals, nil)
										} else {
											headerVals = append(headerVals, jsonVal)
										}
									} else {
										headerVals = append(headerVals, v)
									}
									headerPlaceholders = append(headerPlaceholders, fmt.Sprintf("$%d", idx))
									idx++
								}
								headerInsert := fmt.Sprintf("INSERT INTO exposure_headers (%s) VALUES (%s) RETURNING exposure_header_id", strings.Join(headerKeys, ", "), strings.Join(headerPlaceholders, ", "))
								var exposureHeaderId string
								err := db.QueryRow(headerInsert, headerVals...).Scan(&exposureHeaderId)
								if err != nil {
									absorptionErrors = append(absorptionErrors, fmt.Sprintf("header insert error for file %s: %v", filename, err))
									continue
								}
								insertedFinalRows++
								line := map[string]interface{}{}
								lineDetails := map[string]interface{}{}
								for _, m := range mappings {
									if m.TargetTable == "exposure_line_items" {
										var val interface{}
										switch m.SourceCol {
										case field.DataType:
											val = field.DataType
										case "1":
											val = 1
										case field.TableName:
											val = staged
										default:
											val = staged[m.SourceCol]
										}
										if m.TargetField == "additional_line_details" {
											lineDetails[m.SourceCol] = val
										} else {
											if strings.Contains(m.TargetField, "amount") {
												switch v := val.(type) {
												case float64:
													val = math.Abs(v)
												case string:
													if f, err := strconv.ParseFloat(v, 64); err == nil {
														val = math.Abs(f)
													}
												}
											}
											line[m.TargetField] = val
										}
									}
								}
								// Marshal lineDetails if needed
								if len(lineDetails) > 0 {
									jsonVal, err := json.Marshal(lineDetails)
									if err == nil {
										line["additional_line_details"] = jsonVal
									} else {
										absorptionErrors = append(absorptionErrors, fmt.Sprintf("line details marshal error for file %s: %v", filename, err))
										line["additional_line_details"] = nil
									}
								}
								line["exposure_header_id"] = exposureHeaderId
								if _, ok := line["linked_exposure_header_id"]; ok {
									delete(line, "linked_exposure_header_id")
								}
								lineKeys := []string{}
								lineVals := []interface{}{}
								linePlaceholders := []string{}
								idx = 1
								for k, v := range line {
									lineKeys = append(lineKeys, k)
									// Marshal map[string]interface{} to JSON for DB
									if m, ok := v.(map[string]interface{}); ok {
										jsonVal, err := json.Marshal(m)
										if err != nil {
											absorptionErrors = append(absorptionErrors, fmt.Sprintf("line marshal error for file %s, key %s: %v", filename, k, err))
											lineVals = append(lineVals, nil)
										} else {
											lineVals = append(lineVals, jsonVal)
										}
									} else {
										lineVals = append(lineVals, v)
									}
									linePlaceholders = append(linePlaceholders, fmt.Sprintf("$%d", idx))
									idx++
								}
								lineInsert := fmt.Sprintf("INSERT INTO exposure_line_items (%s) VALUES (%s)", strings.Join(lineKeys, ", "), strings.Join(linePlaceholders, ", "))
								_, err = db.Exec(lineInsert, lineVals...)
								if err != nil {
									absorptionErrors = append(absorptionErrors, fmt.Sprintf("line item insert error for file %s: %v", filename, err))
								}
							}
						}
					}
					success := insertedFinalRows > 0
					msg := "Batch uploaded to staging table only"
					if success {
						msg = "Batch absorbed into exposures"
					}
					results = append(results, map[string]interface{}{
						"success": success,
						"filename": filename,
						"message": msg,
						"uploadBatchId": uploadBatchId,
						"insertedRows": insertedRows,
						"insertedFinalRows": insertedFinalRows,
					})
					os.Remove(tempFile.Name())
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		var allErrors []string
		// Collect errors from absorptionErrors
		if len(absorptionErrors) > 0 {
			allErrors = append(allErrors, absorptionErrors...)
		}
		// Collect errors from results
		for _, res := range results {
			if errMsg, ok := res["error"].(string); ok && errMsg != "" {
				allErrors = append(allErrors, errMsg)
			}
			if success, ok := res["success"].(bool); ok && !success {
				// If not already in errors, add generic error
				if errMsg, ok := res["error"].(string); !ok || errMsg == "" {
					allErrors = append(allErrors, fmt.Sprintf("File %v failed to process.", res["filename"]))
				}
			}
		}
		if len(results) == 0 {
			allErrors = append(allErrors, "No files found.")
		}
		if len(allErrors) > 0 {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"error": strings.Join(allErrors, "; "),
			})
			return
		}
		// All successful
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data": results,
		})

	}
}